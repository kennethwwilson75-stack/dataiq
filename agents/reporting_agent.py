"""Reporting Agent — generates audience-specific reports."""

import json
import os

from anthropic import Anthropic

from .state import DataIQState, ReportOutput


def run_reporting(state: DataIQState) -> dict:
    """Generate three audience-specific reports from pipeline state."""
    print("\n[Reporting] Generating audience-specific reports...")

    output = ReportOutput()
    client = Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))

    # Build context from all previous agents
    context = {
        "pipeline_status": state.pipeline_monitor.model_dump() if state.pipeline_monitor else {},
        "data_quality": state.data_quality.model_dump() if state.data_quality else {},
        "root_cause": state.root_cause.model_dump() if state.root_cause else {},
        "impact_analysis": state.impact_analysis.model_dump() if state.impact_analysis else {},
        "scenario": state.scenario,
        "errors": state.errors,
    }
    context_str = json.dumps(context, indent=2, default=str)

    # Report 1: Data Engineer Brief
    print("  Generating data engineer brief...")
    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1500,
        messages=[{
            "role": "user",
            "content": (
                "You are writing a technical brief for a data engineer about a dbt pipeline issue.\n\n"
                f"Full pipeline analysis:\n{context_str}\n\n"
                "Write a DATA ENGINEER BRIEF that is:\n"
                "- Technical and specific\n"
                "- Includes exact model names, test names, error messages\n"
                "- Provides step-by-step fix instructions\n"
                "- Mentions SQL/dbt commands to run\n"
                "- Includes estimated fix time\n\n"
                "Format with clear sections: Status, Root Cause, Affected Models, Fix Steps, Prevention."
            )
        }]
    )
    output.data_engineer_brief = response.content[0].text

    # Report 2: Analytics Manager Summary
    print("  Generating analytics manager summary...")
    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1000,
        messages=[{
            "role": "user",
            "content": (
                "You are writing a summary for an analytics manager about a data pipeline issue.\n\n"
                f"Full pipeline analysis:\n{context_str}\n\n"
                "Write an ANALYTICS MANAGER SUMMARY that:\n"
                "- Explains what's broken in plain terms\n"
                "- States which reports/dashboards are affected\n"
                "- Gives an estimated time to resolution\n"
                "- Explains what decisions should be paused until fixed\n"
                "- Is concise (under 200 words)\n\n"
                "Format: Status line, What Happened, Impact, ETA, Action Items."
            )
        }]
    )
    output.analytics_manager_summary = response.content[0].text

    # Report 3: Business Stakeholder Note
    print("  Generating business stakeholder note...")
    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=800,
        messages=[{
            "role": "user",
            "content": (
                "You are writing a note for a business stakeholder (VP/C-level) about a data issue.\n\n"
                f"Full pipeline analysis:\n{context_str}\n\n"
                "Write a BUSINESS STAKEHOLDER NOTE that:\n"
                "- Uses zero technical jargon\n"
                "- Explains what business decisions can't be trusted right now\n"
                "- States when things will be back to normal\n"
                "- Is reassuring but honest\n"
                "- Is very concise (under 100 words)\n\n"
                "Format: One-line status, then 2-3 short paragraphs."
            )
        }]
    )
    output.business_stakeholder_note = response.content[0].text

    completed = state.completed_agents + ["reporting"]
    print("[Reporting] All three reports generated.")

    return {
        "report": output.model_dump(),
        "current_agent": "reporting",
        "completed_agents": completed,
    }
