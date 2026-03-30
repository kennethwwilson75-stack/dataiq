"""DataIQ Pipeline Runner — loads dbt artifacts and runs the multi-agent pipeline."""

import json
import os
import subprocess
import sys

# Ensure Unicode output works on Windows
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from dotenv import load_dotenv

load_dotenv()

from agents.state import DataIQState
from agents.pipeline import build_pipeline


def run_dbt_build(project_dir: str) -> None:
    """Run dbt build (models + tests) to produce fresh artifacts."""
    print("Running dbt build...")
    result = subprocess.run(
        ["dbt", "build"],
        cwd=project_dir,
        capture_output=True,
        text=True,
    )
    print(result.stdout)
    if result.stderr:
        print(result.stderr)


def load_dbt_artifacts(target_dir: str) -> dict:
    """Load dbt run_results.json and manifest.json."""
    artifacts = {
        "run_metadata": {},
        "model_results": [],
        "test_results": [],
    }

    # Check for performance override first
    override_path = os.path.join(target_dir, "run_results_override.json")
    run_results_path = override_path if os.path.exists(override_path) else os.path.join(target_dir, "run_results.json")

    if os.path.exists(run_results_path):
        with open(run_results_path) as f:
            run_results = json.load(f)

        artifacts["run_metadata"] = run_results.get("metadata", {})
        artifacts["run_metadata"]["elapsed_time"] = run_results.get("elapsed_time", 0)
        artifacts["run_metadata"]["generated_at"] = run_results.get("metadata", {}).get("generated_at", "")

        for result in run_results.get("results", []):
            unique_id = result.get("unique_id", "")
            entry = {
                "unique_id": unique_id,
                "status": result.get("status", "unknown"),
                "execution_time": result.get("execution_time", 0.0),
                "message": result.get("message", ""),
                "failures": result.get("failures"),
            }

            if unique_id.startswith("test."):
                artifacts["test_results"].append(entry)
            elif unique_id.startswith("model."):
                artifacts["model_results"].append(entry)
    else:
        print(f"Warning: No run_results.json found at {target_dir}")
        print("Run 'cd dataiq && dbt build' first.")
        sys.exit(1)

    return artifacts


def detect_scenario() -> str:
    """Detect which failure scenario is active."""
    project_root = os.path.dirname(os.path.abspath(__file__))

    # Check for freshness marker
    freshness_path = os.path.join(project_root, "data", "processed", "last_run.json")
    if os.path.exists(freshness_path):
        return "data_freshness"

    # Check for performance override
    override_path = os.path.join(project_root, "dataiq", "target", "run_results_override.json")
    if os.path.exists(override_path):
        return "performance_degradation"

    return "none"


def main():
    project_root = os.path.dirname(os.path.abspath(__file__))
    target_dir = os.path.join(project_root, "dataiq", "target")

    print("=" * 60)
    print("  DataIQ — Multi-Agent Data Pipeline Monitor")
    print("=" * 60)

    # Run dbt build to get fresh artifacts with both models and tests
    project_dir = os.path.join(project_root, "dataiq")
    run_dbt_build(project_dir)

    # Load artifacts
    print("\nLoading dbt artifacts...")
    artifacts = load_dbt_artifacts(target_dir)

    print(f"  Models: {len(artifacts['model_results'])}")
    print(f"  Tests:  {len(artifacts['test_results'])}")

    # Detect scenario
    scenario = detect_scenario()
    print(f"  Scenario: {scenario}")

    # Build initial state
    state = DataIQState(
        run_metadata=artifacts["run_metadata"],
        model_results=artifacts["model_results"],
        test_results=artifacts["test_results"],
        scenario=scenario,
    )

    # Build and run pipeline
    print("\nStarting agent pipeline...")
    pipeline = build_pipeline()
    final_state = pipeline.invoke(state)

    # Debug: show what final_state actually is
    print(f"\n[DEBUG] type(final_state) = {type(final_state)}")
    print(f"[DEBUG] final_state keys/attrs = {list(final_state.keys()) if isinstance(final_state, dict) else dir(final_state)}")
    if isinstance(final_state, dict):
        print(f"[DEBUG] 'report' in final_state = {'report' in final_state}")
        print(f"[DEBUG] type(final_state['report']) = {type(final_state.get('report'))}")
        print(f"[DEBUG] final_state['report'] = {repr(final_state.get('report'))[:200]}")
    else:
        print(f"[DEBUG] hasattr report = {hasattr(final_state, 'report')}")
        print(f"[DEBUG] final_state.report = {repr(getattr(final_state, 'report', 'MISSING'))[:200]}")

    # Extract report from final state (handles both dict and Pydantic returns)
    if isinstance(final_state, dict):
        report = final_state.get("report")
    else:
        report = getattr(final_state, "report", None)

    if report is not None:
        # Get field values regardless of whether report is a dict or Pydantic model
        if isinstance(report, dict):
            engineer_brief = report.get("data_engineer_brief", "No report generated")
            manager_summary = report.get("analytics_manager_summary", "No report generated")
            stakeholder_note = report.get("business_stakeholder_note", "No report generated")
        else:
            engineer_brief = getattr(report, "data_engineer_brief", "No report generated")
            manager_summary = getattr(report, "analytics_manager_summary", "No report generated")
            stakeholder_note = getattr(report, "business_stakeholder_note", "No report generated")

        print("\n" + "=" * 48)
        print("DATA ENGINEER BRIEF")
        print("=" * 48)
        print(engineer_brief)

        print("\n" + "=" * 48)
        print("ANALYTICS MANAGER SUMMARY")
        print("=" * 48)
        print(manager_summary)

        print("\n" + "=" * 48)
        print("BUSINESS STAKEHOLDER NOTE")
        print("=" * 48)
        print(stakeholder_note)
    else:
        print("\nNo reports generated.")

    # Print completion summary
    if isinstance(final_state, dict):
        completed = final_state.get("completed_agents", [])
        errors = final_state.get("errors", [])
    else:
        completed = final_state.completed_agents if hasattr(final_state, "completed_agents") else []
        errors = final_state.errors if hasattr(final_state, "errors") else []

    print("\n" + "=" * 60)
    print(f"  Pipeline complete. Agents run: {completed}")
    if errors:
        print(f"  Errors: {errors}")
    print("=" * 60)


if __name__ == "__main__":
    main()
