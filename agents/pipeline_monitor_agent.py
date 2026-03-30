"""Pipeline Monitor Agent — detects failed models, slow execution, missing runs."""

import json
import os

from anthropic import Anthropic

from .state import DataIQState, PipelineMonitorOutput


def run_pipeline_monitor(state: DataIQState) -> dict:
    """Analyze dbt run results for failures and performance issues."""
    print("\n[Pipeline Monitor] Analyzing dbt run results...")

    output = PipelineMonitorOutput()

    # Parse model results
    failed_models = []
    slow_models = []
    total_time = 0.0
    execution_times = []

    model_results = state.model_results

    for result in model_results:
        status = result.get("status", "unknown")
        model_name = result.get("unique_id", "unknown")
        exec_time = result.get("execution_time", 0.0)
        total_time += exec_time
        execution_times.append(exec_time)

        if status == "error":
            failed_models.append(model_name)
        elif exec_time > 0:
            # Will check for slow models after calculating baseline
            pass

    # Calculate baseline and detect slow models
    if execution_times:
        avg_time = sum(execution_times) / len(execution_times)
        for result in model_results:
            exec_time = result.get("execution_time", 0.0)
            model_name = result.get("unique_id", "unknown")
            if exec_time > avg_time * 2 and exec_time > 1.0:
                slow_models.append(f"{model_name} ({exec_time:.1f}s, {exec_time/avg_time:.1f}x avg)")

    # Check for missing runs (freshness)
    freshness_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "data", "processed", "last_run.json"
    )
    missing_runs = False
    if os.path.exists(freshness_path):
        with open(freshness_path) as f:
            freshness = json.load(f)
        from datetime import datetime, timedelta
        last_run = datetime.fromisoformat(freshness["last_run"])
        if datetime.now() - last_run > timedelta(hours=24):
            missing_runs = True

    output.status = "failed" if failed_models else ("degraded" if slow_models or missing_runs else "healthy")
    output.failed_models = failed_models
    output.slow_models = slow_models
    output.missing_runs = missing_runs
    output.total_models = len(model_results)
    output.total_execution_time = round(total_time, 2)

    output.summary = (
        f"Models: {output.total_models} total, {len(failed_models)} failed, "
        f"{len(slow_models)} slow. Total time: {total_time:.1f}s. "
        f"Missing runs: {missing_runs}. Status: {output.status}"
    )

    # Call Claude for interpretation
    findings = {
        "status": output.status,
        "failed_models": failed_models,
        "slow_models": slow_models,
        "missing_runs": missing_runs,
        "total_models": output.total_models,
        "total_execution_time": output.total_execution_time,
        "model_results": model_results[:10],  # First 10 for context
    }

    client = Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))
    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1024,
        messages=[{
            "role": "user",
            "content": (
                "You are a data pipeline monitoring expert. Analyze these dbt run results "
                "and provide a concise technical assessment.\n\n"
                f"Findings:\n{json.dumps(findings, indent=2)}\n\n"
                "Provide:\n1. Overall health assessment\n2. Key issues found\n"
                "3. Recommended immediate actions\n\nBe specific and actionable."
            )
        }]
    )
    output.claude_interpretation = response.content[0].text

    completed = state.completed_agents + ["pipeline_monitor"]
    print(f"[Pipeline Monitor] Status: {output.status} | {output.summary}")

    return {
        "pipeline_monitor": output.model_dump(),
        "current_agent": "pipeline_monitor",
        "completed_agents": completed,
    }
