"""DataIQ pipeline state definitions."""

from typing import List, Optional
from pydantic import BaseModel, Field


class PipelineMonitorOutput(BaseModel):
    """Output from the pipeline monitor agent."""
    status: str = "unknown"  # healthy, degraded, failed
    failed_models: List[str] = Field(default_factory=list)
    slow_models: List[str] = Field(default_factory=list)
    missing_runs: bool = False
    total_models: int = 0
    total_execution_time: float = 0.0
    summary: str = ""
    claude_interpretation: str = ""


class DataQualityOutput(BaseModel):
    """Output from the data quality agent."""
    status: str = "unknown"  # passing, warning, failing
    total_tests: int = 0
    passed_tests: int = 0
    failed_tests: int = 0
    failure_details: List[dict] = Field(default_factory=list)
    column_issues: List[str] = Field(default_factory=list)
    summary: str = ""
    claude_interpretation: str = ""


class RootCauseOutput(BaseModel):
    """Output from the root cause agent."""
    root_cause_type: str = "unknown"  # schema_change, freshness, quality, performance, none
    root_cause_model: str = ""
    dependency_chain: List[str] = Field(default_factory=list)
    evidence: List[str] = Field(default_factory=list)
    summary: str = ""
    claude_interpretation: str = ""


class ImpactAnalysisOutput(BaseModel):
    """Output from the impact analysis agent."""
    affected_models: List[str] = Field(default_factory=list)
    affected_count: int = 0
    downstream_impacts: List[str] = Field(default_factory=list)
    stakeholder_impacts: List[str] = Field(default_factory=list)
    summary: str = ""
    claude_interpretation: str = ""


class ReportOutput(BaseModel):
    """Output from the reporting agent."""
    data_engineer_brief: str = ""
    analytics_manager_summary: str = ""
    business_stakeholder_note: str = ""


class DataIQState(BaseModel):
    """Full pipeline state passed between agents."""
    run_metadata: dict = Field(default_factory=dict)
    model_results: List[dict] = Field(default_factory=list)
    test_results: List[dict] = Field(default_factory=list)
    pipeline_monitor: Optional[PipelineMonitorOutput] = None
    data_quality: Optional[DataQualityOutput] = None
    root_cause: Optional[RootCauseOutput] = None
    impact_analysis: Optional[ImpactAnalysisOutput] = None
    report: Optional[ReportOutput] = None
    current_agent: str = ""
    errors: List[str] = Field(default_factory=list)
    completed_agents: List[str] = Field(default_factory=list)
    scenario: str = "none"
