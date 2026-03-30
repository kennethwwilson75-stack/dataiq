"""Root Cause Agent — traces failures through the dependency graph."""

import json
import os

from anthropic import Anthropic

from .state import DataIQState, RootCauseOutput


def run_root_cause(state: DataIQState) -> dict:
    """Trace root cause of failures through the DAG."""
    print("\n[Root Cause] Tracing failure origins...")

    output = RootCauseOutput()

    # Build dependency graph from manifest
    manifest_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "dataiq", "target", "manifest.json"
    )

    dep_graph = {}
    model_info = {}
    if os.path.exists(manifest_path):
        with open(manifest_path) as f:
            manifest = json.load(f)

        for node_id, node in manifest.get("nodes", {}).items():
            if node.get("resource_type") == "model":
                dep_graph[node_id] = node.get("depends_on", {}).get("nodes", [])
                model_info[node_id] = {
                    "name": node.get("name"),
                    "path": node.get("path"),
                    "raw_code": node.get("raw_code", "")[:200],
                }

    # Gather evidence from previous agents
    evidence = []
    failed_models = []
    root_cause_type = "none"

    pm = state.pipeline_monitor
    dq = state.data_quality

    if pm:
        if pm.failed_models:
            evidence.append(f"Failed models: {pm.failed_models}")
            failed_models = pm.failed_models
        if pm.slow_models:
            evidence.append(f"Slow models: {pm.slow_models}")
        if pm.missing_runs:
            evidence.append("Pipeline has missing/stale runs")

    if dq:
        if dq.failed_tests > 0:
            evidence.append(f"Failed tests: {dq.failed_tests}")
            evidence.append(f"Column issues: {dq.column_issues}")
            for detail in dq.failure_details:
                evidence.append(f"Test failure: {detail['test']} - {detail.get('message', '')}")

    # Trace upstream for each failed model
    dependency_chain = []
    visited = set()

    def trace_upstream(model_id):
        if model_id in visited:
            return
        visited.add(model_id)
        dependency_chain.append(model_id)
        for dep in dep_graph.get(model_id, []):
            if dep.startswith("model."):
                trace_upstream(dep)

    for model in failed_models:
        trace_upstream(model)

    # Determine root cause type
    if pm and pm.failed_models:
        # Check for schema issues (model errors often indicate schema changes)
        root_cause_type = "schema_change"
    if pm and pm.missing_runs:
        root_cause_type = "freshness"
    if dq and dq.status == "failing":
        if not failed_models:
            root_cause_type = "quality"
    if (pm and pm.slow_models
            and not failed_models
            and (not dq or dq.status == "passing")):
        root_cause_type = "performance"
    if not evidence:
        root_cause_type = "none"

    output.root_cause_type = root_cause_type
    output.root_cause_model = failed_models[0] if failed_models else ""
    output.dependency_chain = dependency_chain
    output.evidence = evidence

    output.summary = (
        f"Root cause type: {root_cause_type}. "
        f"Origin: {output.root_cause_model or 'N/A'}. "
        f"Dependency chain: {len(dependency_chain)} models. "
        f"Evidence points: {len(evidence)}"
    )

    # Call Claude for interpretation
    pipeline_status = pm.status if pm else "unknown"
    quality_status = dq.status if dq else "unknown"

    findings = {
        "root_cause_type": root_cause_type,
        "root_cause_model": output.root_cause_model,
        "dependency_chain": dependency_chain,
        "evidence": evidence,
        "dep_graph": {k: v for k, v in dep_graph.items()},
        "model_info": model_info,
        "pipeline_status": pipeline_status,
        "quality_status": quality_status,
    }

    client = Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))
    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1024,
        messages=[{
            "role": "user",
            "content": (
                "You are a data engineering root cause analysis expert. "
                "Analyze these findings and explain the root cause chain.\n\n"
                f"Findings:\n{json.dumps(findings, indent=2)}\n\n"
                "Provide:\n1. Root cause identification\n"
                "2. How the failure propagated through the dependency graph\n"
                "3. Why this specific type of failure occurred\n"
                "4. Recommended fix\n\nBe precise and technical."
            )
        }]
    )
    output.claude_interpretation = response.content[0].text

    completed = state.completed_agents + ["root_cause"]
    print(f"[Root Cause] Type: {root_cause_type} | {output.summary}")

    return {
        "root_cause": output.model_dump(),
        "current_agent": "root_cause",
        "completed_agents": completed,
    }
