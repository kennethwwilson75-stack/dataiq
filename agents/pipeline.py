"""DataIQ LangGraph pipeline — orchestrates all 5 agents."""

from langgraph.graph import StateGraph, END

from .state import DataIQState
from .pipeline_monitor_agent import run_pipeline_monitor
from .data_quality_agent import run_data_quality
from .root_cause_agent import run_root_cause
from .impact_analysis_agent import run_impact_analysis
from .reporting_agent import run_reporting


def route_after_monitor(state: DataIQState) -> str:
    """Route based on pipeline monitor results."""
    pm = state.pipeline_monitor
    if pm and (
        pm.status in ("failed", "degraded")
        or pm.failed_models
        or pm.slow_models
        or pm.missing_runs
    ):
        return "data_quality"
    # Even if healthy, run data quality to confirm
    return "data_quality"


def build_pipeline() -> StateGraph:
    """Build and compile the DataIQ agent pipeline."""
    workflow = StateGraph(DataIQState)

    # Add nodes
    workflow.add_node("pipeline_monitor", run_pipeline_monitor)
    workflow.add_node("data_quality", run_data_quality)
    workflow.add_node("root_cause", run_root_cause)
    workflow.add_node("impact_analysis", run_impact_analysis)
    workflow.add_node("reporting", run_reporting)

    # Set entry point
    workflow.set_entry_point("pipeline_monitor")

    # Conditional routing after pipeline_monitor
    workflow.add_conditional_edges(
        "pipeline_monitor",
        route_after_monitor,
        {
            "data_quality": "data_quality",
            "reporting": "reporting",
        }
    )

    # Linear flow: data_quality -> root_cause -> impact_analysis -> reporting
    workflow.add_edge("data_quality", "root_cause")
    workflow.add_edge("root_cause", "impact_analysis")
    workflow.add_edge("impact_analysis", "reporting")
    workflow.add_edge("reporting", END)

    # Compile without checkpointer for Python 3.11 compatibility
    return workflow.compile()
