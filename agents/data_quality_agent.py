"""Data Quality Agent — analyzes test results and quality metrics."""

import json
import os

from anthropic import Anthropic

from .state import DataIQState, DataQualityOutput


def run_data_quality(state: DataIQState) -> dict:
    """Analyze dbt test results for quality issues."""
    print("\n[Data Quality] Analyzing test results...")

    output = DataQualityOutput()

    # Parse test results
    passed = 0
    failed = 0
    failure_details = []
    column_issues = set()

    test_results = state.test_results

    for result in test_results:
        status = result.get("status", "unknown")
        test_name = result.get("unique_id", "unknown")

        if status == "pass":
            passed += 1
        elif status in ("fail", "error"):
            failed += 1
            detail = {
                "test": test_name,
                "status": status,
                "message": result.get("message", ""),
                "failures": result.get("failures", 0),
            }
            failure_details.append(detail)

            # Extract column name from test name
            parts = test_name.split(".")
            for part in parts:
                if any(col in part for col in [
                    "fare_amount", "trip_duration", "passenger_count",
                    "vendor_id", "pickup_datetime", "trip_date",
                    "trip_count", "total_revenue"
                ]):
                    column_issues.add(part)

    output.total_tests = passed + failed
    output.passed_tests = passed
    output.failed_tests = failed
    output.failure_details = failure_details
    output.column_issues = list(column_issues)

    if failed == 0:
        output.status = "passing"
    elif failed <= 2:
        output.status = "warning"
    else:
        output.status = "failing"

    output.summary = (
        f"Tests: {output.total_tests} total, {passed} passed, {failed} failed. "
        f"Status: {output.status}. Column issues: {output.column_issues}"
    )

    # Call Claude for interpretation
    findings = {
        "status": output.status,
        "total_tests": output.total_tests,
        "passed_tests": passed,
        "failed_tests": failed,
        "failure_details": failure_details,
        "column_issues": output.column_issues,
    }

    client = Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))
    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1024,
        messages=[{
            "role": "user",
            "content": (
                "You are a data quality expert. Analyze these dbt test results "
                "and assess data quality.\n\n"
                f"Findings:\n{json.dumps(findings, indent=2)}\n\n"
                "Provide:\n1. Quality assessment\n2. Which columns/tests failed and why\n"
                "3. Business impact of quality issues\n4. Remediation steps\n\n"
                "Be specific about which data may be unreliable."
            )
        }]
    )
    output.claude_interpretation = response.content[0].text

    completed = state.completed_agents + ["data_quality"]
    print(f"[Data Quality] Status: {output.status} | {output.summary}")

    return {
        "data_quality": output.model_dump(),
        "current_agent": "data_quality",
        "completed_agents": completed,
    }
