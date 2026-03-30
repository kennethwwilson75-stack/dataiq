"""Impact Analysis Agent — identifies downstream effects of failures."""

import json
import os

from anthropic import Anthropic

from .state import DataIQState, ImpactAnalysisOutput


def run_impact_analysis(state: DataIQState) -> dict:
    """Identify downstream impacts of detected failures."""
    print("\n[Impact Analysis] Mapping downstream effects...")

    output = ImpactAnalysisOutput()

    # Build dependency graph from manifest
    manifest_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "dataiq", "target", "manifest.json"
    )

    # Build reverse dependency graph (who depends on whom)
    reverse_deps = {}
    model_names = {}
    if os.path.exists(manifest_path):
        with open(manifest_path) as f:
            manifest = json.load(f)

        for node_id, node in manifest.get("nodes", {}).items():
            if node.get("resource_type") == "model":
                model_names[node_id] = node.get("name", node_id)
                for dep in node.get("depends_on", {}).get("nodes", []):
                    if dep not in reverse_deps:
                        reverse_deps[dep] = []
                    reverse_deps[dep].append(node_id)

    # Find all downstream models from root cause
    rc = state.root_cause
    pm = state.pipeline_monitor

    root_model = rc.root_cause_model if rc else ""
    affected = set()
    queue = [root_model] if root_model else []

    # Also add failed models from pipeline monitor
    if pm:
        queue.extend(pm.failed_models)

    visited = set()
    while queue:
        current = queue.pop(0)
        if current in visited:
            continue
        visited.add(current)
        affected.add(current)
        for downstream in reverse_deps.get(current, []):
            queue.append(downstream)

    affected_names = [model_names.get(m, m) for m in affected]
    output.affected_models = affected_names
    output.affected_count = len(affected)

    # Map downstream impacts to business contexts
    downstream_impacts = []
    stakeholder_impacts = []

    mart_models = [m for m in affected_names if any(
        mart in m for mart in ["daily_trip_metrics", "hourly_demand", "payment_summary"]
    )]

    if mart_models:
        downstream_impacts.append(f"Mart models affected: {mart_models}")
        stakeholder_impacts.append("Daily revenue dashboards may show stale/incorrect data")
        stakeholder_impacts.append("Demand forecasting models consuming hourly_demand will be impacted")

    if "daily_trip_metrics" in affected_names:
        stakeholder_impacts.append("Executive daily KPI reports are unreliable")
    if "payment_summary" in affected_names:
        stakeholder_impacts.append("Payment reconciliation reports are affected")
    if "hourly_demand" in affected_names:
        stakeholder_impacts.append("Operational demand planning is using stale data")

    if not stakeholder_impacts:
        stakeholder_impacts.append("No immediate downstream business impact detected")

    output.downstream_impacts = downstream_impacts
    output.stakeholder_impacts = stakeholder_impacts
    output.summary = (
        f"Affected models: {output.affected_count}. "
        f"Downstream impacts: {len(downstream_impacts)}. "
        f"Stakeholder impacts: {len(stakeholder_impacts)}"
    )

    # Call Claude for interpretation
    root_cause_type = rc.root_cause_type if rc else "unknown"

    findings = {
        "root_cause_type": root_cause_type,
        "root_cause_model": root_model,
        "affected_models": affected_names,
        "affected_count": output.affected_count,
        "downstream_impacts": downstream_impacts,
        "stakeholder_impacts": stakeholder_impacts,
        "model_dependency_graph": {model_names.get(k, k): [model_names.get(v, v) for v in vs]
                                    for k, vs in reverse_deps.items()},
    }

    client = Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))
    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1024,
        messages=[{
            "role": "user",
            "content": (
                "You are a data engineering impact analysis expert. "
                "Assess the downstream impact of this pipeline failure.\n\n"
                f"Findings:\n{json.dumps(findings, indent=2)}\n\n"
                "Provide:\n1. Full blast radius (all affected models and consumers)\n"
                "2. Business impact assessment\n"
                "3. Which stakeholders need to be notified\n"
                "4. Mitigation recommendations\n\n"
                "Be specific about business impact and urgency."
            )
        }]
    )
    output.claude_interpretation = response.content[0].text

    completed = state.completed_agents + ["impact_analysis"]
    print(f"[Impact Analysis] {output.summary}")

    return {
        "impact_analysis": output.model_dump(),
        "current_agent": "impact_analysis",
        "completed_agents": completed,
    }
