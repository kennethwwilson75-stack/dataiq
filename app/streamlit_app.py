"""DataIQ — Streamlit Dashboard for MeridianIQ Data Pipeline Intelligence."""

import json
import os
import subprocess
import sys
import time
from datetime import datetime

import streamlit as st

# ---------------------------------------------------------------------------
# Path setup — ensure project root is importable
# ---------------------------------------------------------------------------
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# ---------------------------------------------------------------------------
# API key — st.secrets first, then env
# ---------------------------------------------------------------------------
if hasattr(st, "secrets") and "ANTHROPIC_API_KEY" in st.secrets:
    os.environ["ANTHROPIC_API_KEY"] = st.secrets["ANTHROPIC_API_KEY"]

from dotenv import load_dotenv
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

from agents.state import DataIQState  # noqa: E402

# ---------------------------------------------------------------------------
# Brand constants
# ---------------------------------------------------------------------------
PRIMARY = "#1D9E75"
SIGNAL = "#5DCAA5"
MIST = "#E1F5EE"
SLATE = "#2C2C2A"
STONE = "#888780"
TAGLINE = "Intelligence at the intersection of data and action"

# Status colors
STATUS_COLORS = {
    "healthy": "#1D9E75",
    "passing": "#1D9E75",
    "degraded": "#E8A317",
    "warning": "#E8A317",
    "failed": "#D93025",
    "failing": "#D93025",
    "unknown": STONE,
}

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="DataIQ — MeridianIQ",
    page_icon="⬡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Custom CSS
# ---------------------------------------------------------------------------
st.markdown(f"""
<style>
    .stApp {{
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
    }}
    div[data-testid="stSidebar"] {{
        background-color: {SLATE};
    }}
    div[data-testid="stSidebar"] * {{
        color: #FFFFFF !important;
    }}
    div[data-testid="stSidebar"] .stRadio label p {{
        font-size: 1rem;
    }}
    .status-banner {{
        padding: 1rem 1.5rem;
        border-radius: 8px;
        color: white;
        font-size: 1.2rem;
        font-weight: 600;
        margin-bottom: 1rem;
    }}
    .status-healthy {{ background-color: {PRIMARY}; }}
    .status-degraded {{ background-color: #E8A317; }}
    .status-failed {{ background-color: #D93025; }}
    .metric-card {{
        background: {MIST};
        border-radius: 8px;
        padding: 1rem;
        text-align: center;
    }}
    .metric-card h3 {{
        color: {STONE};
        font-size: 0.8rem;
        margin: 0;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }}
    .metric-card p {{
        color: {SLATE};
        font-size: 1.8rem;
        font-weight: 700;
        margin: 0.25rem 0 0 0;
    }}
    .alert-item {{
        background: #FFF3E0;
        border-left: 4px solid #E8A317;
        padding: 0.75rem 1rem;
        margin: 0.5rem 0;
        border-radius: 0 4px 4px 0;
    }}
    .alert-item.critical {{
        background: #FFEBEE;
        border-left-color: #D93025;
    }}
    .brand-header {{
        color: {PRIMARY};
        font-weight: 700;
    }}
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Session state init
# ---------------------------------------------------------------------------
if "pipeline_state" not in st.session_state:
    st.session_state.pipeline_state = None
if "active_scenario" not in st.session_state:
    st.session_state.active_scenario = "none"
if "demo_mode" not in st.session_state:
    st.session_state.demo_mode = False

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
with st.sidebar:
    st.markdown(f"<h1 style='margin-bottom:0;'>⬡ DataIQ</h1>", unsafe_allow_html=True)
    st.markdown(f"<p style='color:{SIGNAL} !important; font-size:0.85rem; margin-top:0;'>by MeridianIQ</p>", unsafe_allow_html=True)
    st.caption(TAGLINE)
    st.divider()

    page = st.radio(
        "Navigation",
        [
            "Pipeline Monitor",
            "Data Quality",
            "Impact Analysis",
            "Executive Report",
            "Demo Scenarios",
            "About",
        ],
        label_visibility="collapsed",
    )

    st.divider()
    if st.session_state.active_scenario != "none":
        st.warning(f"Scenario: {st.session_state.active_scenario.replace('_', ' ').title()}")
    else:
        st.success("No active scenario")


# ═══════════════════════════════════════════════════════════════════════════
# Helper functions
# ═══════════════════════════════════════════════════════════════════════════

def _detect_scenario() -> str:
    """Detect which failure scenario is active."""
    active_path = os.path.join(PROJECT_ROOT, "data", "processed", "active_scenario.json")
    if os.path.exists(active_path):
        with open(active_path) as f:
            data = json.load(f)
        return data.get("scenario", "none")
    freshness_path = os.path.join(PROJECT_ROOT, "data", "processed", "last_run.json")
    if os.path.exists(freshness_path):
        return "data_freshness"
    override_path = os.path.join(PROJECT_ROOT, "dataiq", "target", "run_results_override.json")
    if os.path.exists(override_path):
        return "performance_degradation"
    return "none"


def _load_artifacts() -> dict:
    """Load dbt run_results.json artifacts."""
    target_dir = os.path.join(PROJECT_ROOT, "dataiq", "target")
    artifacts = {"run_metadata": {}, "model_results": [], "test_results": []}

    override_path = os.path.join(target_dir, "run_results_override.json")
    run_results_path = override_path if os.path.exists(override_path) else os.path.join(target_dir, "run_results.json")

    if not os.path.exists(run_results_path):
        return artifacts

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

    return artifacts


def _run_dbt_build():
    """Run dbt build and return stdout."""
    project_dir = os.path.join(PROJECT_ROOT, "dataiq")
    result = subprocess.run(
        ["dbt", "build"],
        cwd=project_dir,
        capture_output=True,
        text=True,
    )
    return result.stdout, result.stderr


def _run_full_pipeline():
    """Run dbt build + agent pipeline, returning the final DataIQState dict."""
    from agents.pipeline import build_pipeline

    # Step 1: dbt build
    _run_dbt_build()

    # Step 2: load artifacts
    artifacts = _load_artifacts()

    # Step 3: detect scenario
    scenario = _detect_scenario()
    st.session_state.active_scenario = scenario

    # Step 4: build state and run
    state = DataIQState(
        run_metadata=artifacts["run_metadata"],
        model_results=artifacts["model_results"],
        test_results=artifacts["test_results"],
        scenario=scenario,
    )
    pipeline = build_pipeline()
    final = pipeline.invoke(state)
    return final


def _run_simulate(scenario_key: str):
    """Run simulate_failure.py with the given scenario."""
    script = os.path.join(PROJECT_ROOT, "scripts", "simulate_failure.py")
    result = subprocess.run(
        [sys.executable, script, scenario_key],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
    )
    return result.stdout, result.stderr


def _get_field(obj, field, default=""):
    """Get a field from either a dict or a Pydantic model."""
    if isinstance(obj, dict):
        return obj.get(field, default)
    return getattr(obj, field, default)


def _get_sub(state, key):
    """Get a sub-object from the pipeline state."""
    if state is None:
        return None
    return _get_field(state, key, None)


def _model_short_name(unique_id: str) -> str:
    """Extract short model/test name from unique_id."""
    return unique_id.split(".")[-1] if "." in unique_id else unique_id


def _status_icon(status: str) -> str:
    icons = {
        "success": "✅",
        "pass": "✅",
        "error": "❌",
        "fail": "❌",
        "warn": "⚠️",
        "skip": "⏭️",
        "skipped": "⏭️",
    }
    return icons.get(status, "❓")


def _build_demo_state() -> dict:
    """Build a demo state from existing dbt artifacts without running agents.

    Always presents a compelling quality-drift failure scenario so the demo
    is interesting even when no live scenario has been activated.
    """
    artifacts = _load_artifacts()
    scenario = _detect_scenario()

    model_results = artifacts["model_results"]
    test_results = artifacts["test_results"]

    # Derive pipeline monitor output — count skipped models as impacted
    failed_models = [m["unique_id"] for m in model_results if m["status"] in ("error", "skipped")]
    running_models = [m for m in model_results if m["execution_time"] > 0]
    slow_threshold = 2 * (sum(m["execution_time"] for m in running_models) / max(len(running_models), 1))
    slow_models = [m["unique_id"] for m in running_models if m["execution_time"] > slow_threshold]
    total_exec = sum(m["execution_time"] for m in model_results)

    # Derive data quality output
    passed = sum(1 for t in test_results if t["status"] == "pass")
    failed_tests = sum(1 for t in test_results if t["status"] in ("fail", "error"))
    warned = sum(1 for t in test_results if t["status"] == "warn")
    skipped_tests = sum(1 for t in test_results if t["status"] == "skipped")
    total_tests = len(test_results)

    # Pipeline status considers both model errors and test failures
    if failed_models or failed_tests > 0:
        pm_status = "failed"
    elif slow_models or warned > 0:
        pm_status = "degraded"
    else:
        pm_status = "healthy"

    if failed_tests > 0:
        dq_status = "failing"
    elif warned > 0:
        dq_status = "warning"
    else:
        dq_status = "passing"

    # If no active scenario but there are failures, label as quality_drift for demo
    if scenario == "none" and (failed_tests > 0 or failed_models):
        scenario = "quality_drift"

    st.session_state.active_scenario = scenario

    failure_details = []
    for t in test_results:
        if t["status"] in ("fail", "error", "warn"):
            failure_details.append({
                "test": t["unique_id"],
                "status": t["status"],
                "message": t.get("message", ""),
                "failures": t.get("failures", 0),
            })

    # Downstream models that were skipped due to failures
    skipped_model_names = [m["unique_id"] for m in model_results if m["status"] == "skipped"]

    # Build a state dict matching DataIQState structure
    state = {
        "run_metadata": artifacts["run_metadata"],
        "model_results": model_results,
        "test_results": test_results,
        "scenario": scenario,
        "completed_agents": ["pipeline_monitor", "data_quality", "root_cause", "impact_analysis"],
        "errors": [],
        "current_agent": "",
        "pipeline_monitor": {
            "status": pm_status,
            "failed_models": failed_models,
            "slow_models": slow_models,
            "missing_runs": False,
            "total_models": len(model_results),
            "total_execution_time": total_exec,
            "summary": f"CRITICAL: Pipeline is {pm_status}. {len(failed_models)} failed/skipped models, "
                       f"{failed_tests} test failures, {warned} warnings.",
            "claude_interpretation": "(Demo mode — agent interpretation not available)",
        },
        "data_quality": {
            "status": dq_status,
            "total_tests": total_tests,
            "passed_tests": passed,
            "failed_tests": failed_tests,
            "failure_details": failure_details,
            "column_issues": [],
            "summary": f"{passed}/{total_tests} tests passing. {failed_tests} failures, {warned} warnings.",
            "claude_interpretation": "(Demo mode — agent interpretation not available)",
        },
        "root_cause": {
            "root_cause_type": scenario,
            "root_cause_model": "model.dataiq.stg_taxi_trips",
            "dependency_chain": ["model.dataiq.raw_taxi_trips", "model.dataiq.stg_taxi_trips"],
            "evidence": [
                "Test `accepted_range_stg_taxi_trips_fare_amount` failed with 30 rows outside range 0–500",
                "Downstream models skipped due to test failure: " + ", ".join(
                    _model_short_name(m) for m in skipped_model_names
                ) if skipped_model_names else "Test failures detected in staging layer",
            ],
            "summary": f"Root cause: {scenario.replace('_', ' ')} detected in stg_taxi_trips.",
            "claude_interpretation": "(Demo mode — agent interpretation not available)",
        },
        "impact_analysis": {
            "affected_models": skipped_model_names,
            "affected_count": len(skipped_model_names),
            "downstream_impacts": [
                f"{_model_short_name(m)} — skipped due to upstream failure" for m in skipped_model_names
            ],
            "stakeholder_impacts": [
                "Daily trip metrics dashboard will show stale data",
                "Revenue reports may be inaccurate until fare_amount anomalies are resolved",
            ] if skipped_model_names else [],
            "summary": f"{len(skipped_model_names)} downstream models affected by upstream failures.",
            "claude_interpretation": "(Demo mode — agent interpretation not available)",
        },
        "report": {
            "data_engineer_brief": "(Demo mode — run full pipeline for AI-generated reports)",
            "analytics_manager_summary": "(Demo mode — run full pipeline for AI-generated reports)",
            "business_stakeholder_note": "(Demo mode — run full pipeline for AI-generated reports)",
        },
    }
    return state


# ═══════════════════════════════════════════════════════════════════════════
# PAGE: Pipeline Monitor
# ═══════════════════════════════════════════════════════════════════════════
def page_pipeline_monitor():
    st.title("Pipeline Monitor")
    st.caption("Data Engineer View — real-time pipeline health")

    col_run, col_demo = st.columns([1, 1])
    with col_run:
        run_clicked = st.button("▶ Run Pipeline Analysis", type="primary", use_container_width=True)
    with col_demo:
        demo_clicked = st.button("⚡ Demo Mode", use_container_width=True,
                                  help="Load pre-computed results instantly (no API calls)")

    if run_clicked:
        agents_order = [
            "Pipeline Monitor Agent",
            "Data Quality Agent",
            "Root Cause Agent",
            "Impact Analysis Agent",
            "Reporting Agent",
        ]
        progress = st.progress(0, text="Starting pipeline...")
        status_area = st.empty()

        for i, agent_name in enumerate(agents_order):
            progress.progress((i) / len(agents_order), text=f"Running {agent_name}...")
            status_area.info(f"🔄 **{agent_name}** is analyzing...")

        try:
            final = _run_full_pipeline()
            st.session_state.pipeline_state = final
            progress.progress(1.0, text="Pipeline complete!")
            status_area.success("Pipeline analysis complete!")
        except Exception as e:
            progress.empty()
            status_area.error(f"Pipeline error: {e}")
            return

    if demo_clicked:
        with st.spinner("Loading demo results..."):
            try:
                state = _build_demo_state()
                st.session_state.pipeline_state = state
                st.session_state.demo_mode = True
                st.rerun()
            except Exception as e:
                st.error(f"Demo mode error: {e}")
                return

    # Display results
    state = st.session_state.pipeline_state
    if state is None:
        st.info("Click **Run Pipeline Analysis** to start, or **Demo Mode** for instant results.")
        return

    pm = _get_sub(state, "pipeline_monitor")
    if pm is None:
        st.warning("Pipeline monitor data not available.")
        return

    pm_status = _get_field(pm, "status", "unknown")

    # Status banner
    css_class = f"status-{pm_status}" if pm_status in ("healthy", "degraded", "failed") else "status-healthy"
    label = pm_status.upper()
    st.markdown(f'<div class="status-banner {css_class}">Pipeline Status: {label}</div>', unsafe_allow_html=True)

    # KPI row
    model_results = _get_field(state, "model_results", [])
    test_results = _get_field(state, "test_results", [])
    failed_models = _get_field(pm, "failed_models", [])
    slow_models = _get_field(pm, "slow_models", [])
    total_models = _get_field(pm, "total_models", len(model_results))
    total_exec = _get_field(pm, "total_execution_time", 0.0)

    total_tests = len(test_results)
    passed_tests = sum(1 for t in test_results if t.get("status") == "pass")
    pass_rate = f"{(passed_tests / total_tests * 100):.0f}%" if total_tests > 0 else "N/A"

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total Models", total_models)
    c2.metric("Failed Models", len(failed_models))
    c3.metric("Slow Models", len(slow_models))
    c4.metric("Test Pass Rate", pass_rate)
    c5.metric("Runtime", f"{total_exec:.1f}s")

    # Model execution table
    st.subheader("Model Execution")
    if model_results:
        rows = []
        for m in model_results:
            rows.append({
                "Status": _status_icon(m.get("status", "")),
                "Model": _model_short_name(m.get("unique_id", "")),
                "Execution Time": f"{m.get('execution_time', 0):.2f}s",
                "Message": m.get("message", "")[:80],
            })
        st.dataframe(rows, use_container_width=True, hide_index=True)
    else:
        st.caption("No model results available.")

    # Alerts
    st.subheader("Active Alerts")
    alerts = []
    for m in failed_models:
        alerts.append(("critical", f"Model **{_model_short_name(m)}** failed"))
    for m in slow_models:
        alerts.append(("warning", f"Model **{_model_short_name(m)}** is running slow"))
    for t in test_results:
        if t.get("status") in ("fail", "error"):
            alerts.append(("critical", f"Test **{_model_short_name(t['unique_id'])}** failed ({t.get('failures', 0)} rows)"))
        elif t.get("status") == "warn":
            alerts.append(("warning", f"Test **{_model_short_name(t['unique_id'])}** warning ({t.get('failures', 0)} rows)"))

    if alerts:
        for level, msg in alerts:
            css = "alert-item critical" if level == "critical" else "alert-item"
            st.markdown(f'<div class="{css}">{msg}</div>', unsafe_allow_html=True)
    else:
        st.success("No active alerts — pipeline is healthy.")

    # Claude interpretation
    interp = _get_field(pm, "claude_interpretation", "")
    if interp and not interp.startswith("(Demo"):
        with st.expander("AI Interpretation", expanded=False):
            st.markdown(interp)


# ═══════════════════════════════════════════════════════════════════════════
# PAGE: Data Quality
# ═══════════════════════════════════════════════════════════════════════════
def page_data_quality():
    st.title("Data Quality")
    st.caption("Test results and quality scoring")

    state = st.session_state.pipeline_state
    if state is None:
        st.info("Run the pipeline from **Pipeline Monitor** first.")
        return

    dq = _get_sub(state, "data_quality")
    test_results = _get_field(state, "test_results", [])

    # Quality score gauge
    if dq:
        total = _get_field(dq, "total_tests", len(test_results))
        passed = _get_field(dq, "passed_tests", 0)
        score = (passed / total * 100) if total > 0 else 0
        dq_status = _get_field(dq, "status", "unknown")

        col1, col2, col3 = st.columns([2, 1, 1])
        with col1:
            color = STATUS_COLORS.get(dq_status, STONE)
            st.markdown(f"""
            <div style="text-align:center; padding:1.5rem; background:{MIST}; border-radius:12px;">
                <div style="font-size:3rem; font-weight:800; color:{color};">{score:.0f}%</div>
                <div style="color:{STONE}; text-transform:uppercase; letter-spacing:0.1em; font-size:0.8rem;">Quality Score</div>
            </div>
            """, unsafe_allow_html=True)
        with col2:
            st.metric("Passed", _get_field(dq, "passed_tests", 0))
            st.metric("Failed", _get_field(dq, "failed_tests", 0))
        with col3:
            st.metric("Total Tests", total)
            st.metric("Status", dq_status.upper())

    # Test results table
    st.subheader("Test Results")
    if test_results:
        rows = []
        for t in test_results:
            uid = t.get("unique_id", "")
            status = t.get("status", "unknown")
            # Parse model and column from test unique_id
            parts = uid.replace("test.dataiq.", "").split(".")
            test_name = parts[0] if parts else uid

            # Determine row color context
            if status in ("fail", "error"):
                indicator = "🔴"
            elif status == "warn":
                indicator = "🟡"
            else:
                indicator = "🟢"

            rows.append({
                "": indicator,
                "Test": test_name,
                "Status": status.upper(),
                "Rows Affected": t.get("failures", 0) or 0,
                "Time": f"{t.get('execution_time', 0):.3f}s",
            })
        st.dataframe(rows, use_container_width=True, hide_index=True)
    else:
        st.caption("No test results available.")

    # Failure details
    if dq:
        failure_details = _get_field(dq, "failure_details", [])
        if failure_details:
            st.subheader("Failure Details")
            for fd in failure_details:
                status = fd.get("status", "fail")
                icon = "🔴" if status in ("fail", "error") else "🟡"
                test_name = _model_short_name(fd.get("test", ""))
                st.markdown(f"{icon} **{test_name}** — {fd.get('message', 'No details')} ({fd.get('failures', 0)} rows)")

        interp = _get_field(dq, "claude_interpretation", "")
        if interp and not interp.startswith("(Demo"):
            with st.expander("AI Quality Assessment", expanded=False):
                st.markdown(interp)


# ═══════════════════════════════════════════════════════════════════════════
# PAGE: Impact Analysis
# ═══════════════════════════════════════════════════════════════════════════
def page_impact_analysis():
    st.title("Impact Analysis")
    st.caption("Dependency tracing and blast radius")

    state = st.session_state.pipeline_state
    if state is None:
        st.info("Run the pipeline from **Pipeline Monitor** first.")
        return

    rc = _get_sub(state, "root_cause")
    ia = _get_sub(state, "impact_analysis")

    # Root cause summary
    if rc:
        rc_type = _get_field(rc, "root_cause_type", "none")
        rc_model = _get_field(rc, "root_cause_model", "")

        st.subheader("Root Cause")
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Type", rc_type.replace("_", " ").title())
        with col2:
            st.metric("Source Model", _model_short_name(rc_model) if rc_model else "N/A")

        evidence = _get_field(rc, "evidence", [])
        if evidence:
            with st.expander("Evidence", expanded=True):
                for e in evidence:
                    st.markdown(f"- {e}")

        dep_chain = _get_field(rc, "dependency_chain", [])
        if dep_chain:
            st.subheader("Dependency Chain")
            tree_lines = []
            for i, node in enumerate(dep_chain):
                prefix = "    " * i + ("└── " if i > 0 else "")
                tree_lines.append(f"{prefix}{_model_short_name(node)}")
            st.code("\n".join(tree_lines), language=None)

        interp = _get_field(rc, "claude_interpretation", "")
        if interp and not interp.startswith("(Demo"):
            with st.expander("AI Root Cause Analysis", expanded=False):
                st.markdown(interp)

    # Impact analysis
    if ia:
        st.subheader("Downstream Impact")
        affected = _get_field(ia, "affected_models", [])
        affected_count = _get_field(ia, "affected_count", len(affected))

        st.metric("Affected Models", affected_count)

        if affected:
            st.markdown("**Affected downstream models:**")
            for m in affected:
                st.markdown(f"- {_model_short_name(m)}")

        downstream = _get_field(ia, "downstream_impacts", [])
        if downstream:
            st.markdown("**Business impacts:**")
            for d in downstream:
                st.markdown(f"- {d}")

        stakeholders = _get_field(ia, "stakeholder_impacts", [])
        if stakeholders:
            st.subheader("Stakeholder Notifications")
            for s in stakeholders:
                st.markdown(f"📧 {s}")

        interp = _get_field(ia, "claude_interpretation", "")
        if interp and not interp.startswith("(Demo"):
            with st.expander("AI Impact Assessment", expanded=False):
                st.markdown(interp)


# ═══════════════════════════════════════════════════════════════════════════
# PAGE: Executive Report
# ═══════════════════════════════════════════════════════════════════════════
def page_executive_report():
    st.title("Executive Report")
    st.caption("AI-generated briefings for every audience")

    state = st.session_state.pipeline_state
    if state is None:
        st.info("Run the pipeline from **Pipeline Monitor** first.")
        return

    report = _get_sub(state, "report")
    pm = _get_sub(state, "pipeline_monitor")
    ia = _get_sub(state, "impact_analysis")

    # Key metrics row
    if pm and ia:
        c1, c2, c3 = st.columns(3)
        pm_status = _get_field(pm, "status", "unknown")
        failed_count = len(_get_field(pm, "failed_models", []))
        affected_count = _get_field(ia, "affected_count", 0)
        c1.metric("Pipeline Status", pm_status.upper())
        c2.metric("Failed Models", failed_count)
        c3.metric("Affected Downstream", affected_count)

    if report is None:
        st.warning("No report available. Run the full pipeline first.")
        return

    # Three audience tabs
    tab_eng, tab_mgr, tab_exec = st.tabs(["Data Engineer Brief", "Analytics Manager", "Business Stakeholder"])

    with tab_eng:
        brief = _get_field(report, "data_engineer_brief", "")
        if brief:
            st.markdown(brief)
        else:
            st.caption("No engineer brief generated.")

    with tab_mgr:
        summary = _get_field(report, "analytics_manager_summary", "")
        if summary:
            st.markdown(summary)
        else:
            st.caption("No manager summary generated.")

    with tab_exec:
        note = _get_field(report, "business_stakeholder_note", "")
        if note:
            st.markdown(note)
        else:
            st.caption("No stakeholder note generated.")

    # Download button
    if report:
        full_report = (
            "# DataIQ Pipeline Report\n"
            f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n"
            "---\n\n"
            "## Data Engineer Brief\n\n"
            f"{_get_field(report, 'data_engineer_brief', 'N/A')}\n\n"
            "---\n\n"
            "## Analytics Manager Summary\n\n"
            f"{_get_field(report, 'analytics_manager_summary', 'N/A')}\n\n"
            "---\n\n"
            "## Business Stakeholder Note\n\n"
            f"{_get_field(report, 'business_stakeholder_note', 'N/A')}\n"
        )
        st.download_button(
            "📥 Download Full Report",
            data=full_report,
            file_name=f"dataiq_report_{datetime.now().strftime('%Y%m%d_%H%M')}.md",
            mime="text/markdown",
        )


# ═══════════════════════════════════════════════════════════════════════════
# PAGE: Demo Scenarios
# ═══════════════════════════════════════════════════════════════════════════
def _is_cloud_environment() -> bool:
    """Detect if running on Streamlit Cloud (read-only filesystem)."""
    if os.environ.get("STREAMLIT_SHARING_MODE"):
        return True
    # Fallback: check if data/raw/ is writable
    raw_dir = os.path.join(PROJECT_ROOT, "data", "raw")
    test_file = os.path.join(raw_dir, ".write_test")
    try:
        with open(test_file, "w") as f:
            f.write("test")
        os.remove(test_file)
        return False
    except OSError:
        return True


def page_demo_scenarios():
    st.title("Demo Scenarios")
    st.caption("Simulate failure modes to test the agent pipeline")

    is_cloud = _is_cloud_environment()

    if is_cloud:
        st.warning(
            "**Live scenario simulation requires local deployment.** "
            "On the cloud demo, use **Demo Mode** on the Pipeline Monitor page "
            "to see pre-computed results."
        )

    # Current scenario status
    current = _detect_scenario()
    st.session_state.active_scenario = current

    if current != "none":
        st.warning(f"**Active scenario:** {current.replace('_', ' ').title()}")
    else:
        st.success("No active scenario — pipeline in normal state.")

    st.divider()

    scenarios = {
        "schema_change": {
            "label": "Schema Change",
            "icon": "🔀",
            "description": "Renames `fare_amount` to `fare` in the source data. "
                           "This causes `stg_taxi_trips` to fail because the expected column is missing.",
        },
        "data_freshness": {
            "label": "Data Freshness",
            "icon": "⏰",
            "description": "Creates a stale `last_run` marker dated 2 days ago. "
                           "The pipeline monitor detects that data hasn't been refreshed recently.",
        },
        "quality_drift": {
            "label": "Quality Drift",
            "icon": "📉",
            "description": "Sets 34% of `fare_amount` values to `9999.99`. These pass basic filters "
                           "but fail the `accepted_range` test (max 500), simulating silent data corruption.",
        },
        "performance_degradation": {
            "label": "Performance Degradation",
            "icon": "🐌",
            "description": "Multiplies all model execution times by 10x in run results. "
                           "The pipeline detects unexpectedly slow model builds.",
        },
    }

    cols = st.columns(2)
    for i, (key, info) in enumerate(scenarios.items()):
        with cols[i % 2]:
            st.markdown(f"### {info['icon']} {info['label']}")
            st.markdown(info["description"])
            is_active = current == key

            if is_active:
                st.button(f"✓ Active", key=f"btn_{key}", disabled=True, use_container_width=True)
            elif is_cloud:
                st.button(f"Activate {info['label']}", key=f"btn_{key}", disabled=True, use_container_width=True)
            else:
                if st.button(f"Activate {info['label']}", key=f"btn_{key}", use_container_width=True):
                    with st.spinner(f"Simulating {info['label']}..."):
                        # Reset first, then apply scenario
                        _run_simulate("reset")
                        stdout, stderr = _run_simulate(key)
                        st.session_state.active_scenario = key
                    st.success(f"{info['label']} scenario activated!")
                    if stdout:
                        with st.expander("Output"):
                            st.code(stdout)
                    time.sleep(1)
                    st.rerun()

    st.divider()

    # Reset button
    if is_cloud:
        st.button("🔄 Reset to Normal", type="primary", use_container_width=True, disabled=True)
    elif st.button("🔄 Reset to Normal", type="primary", use_container_width=True):
        with st.spinner("Resetting all scenarios..."):
            stdout, stderr = _run_simulate("reset")
            st.session_state.active_scenario = "none"
            st.session_state.pipeline_state = None
        st.success("All scenarios cleared.")
        time.sleep(1)
        st.rerun()

    st.divider()
    st.caption(
        "After activating a scenario, go to **Pipeline Monitor** and click "
        "**Run Pipeline Analysis** to see how the agents detect and report the issue."
    )


# ═══════════════════════════════════════════════════════════════════════════
# PAGE: About
# ═══════════════════════════════════════════════════════════════════════════
def page_about():
    st.markdown(f"""
    <div style="text-align:center; padding: 2rem 0;">
        <h1 style="color:{PRIMARY}; font-size:3rem; margin-bottom:0;">⬡ DataIQ</h1>
        <p style="color:{STONE}; font-size:1.1rem; margin-top:0.5rem;">by MeridianIQ</p>
        <p style="color:{SLATE}; font-style:italic;">{TAGLINE}</p>
    </div>
    """, unsafe_allow_html=True)

    st.divider()

    st.subheader("What is DataIQ?")
    st.markdown("""
    DataIQ is a **multi-agent AI system** that monitors dbt data pipelines in real time.
    It detects failures, diagnoses root causes, traces downstream impact, and generates
    audience-specific reports — all powered by Claude and LangGraph.

    Built on **NYC taxi trip data** (8M+ records), DataIQ demonstrates how AI agents
    can replace manual runbook triage with intelligent, automated pipeline monitoring.
    """)

    st.divider()

    st.subheader("The 5 Agents")

    agents = [
        (
            "Pipeline Monitor",
            "The first responder. Scans dbt build results to detect failed models, "
            "slow-running queries, and stale data. Classifies overall pipeline health as "
            "Healthy, Degraded, or Failed.",
        ),
        (
            "Data Quality",
            "The quality inspector. Reviews every dbt test result — null checks, range "
            "validations, uniqueness constraints — and calculates an overall quality score. "
            "Highlights exactly which columns and tables have issues.",
        ),
        (
            "Root Cause",
            "The detective. Traces the dependency graph upstream from failures to find "
            "the original source of the problem. Classifies issues as schema changes, "
            "freshness gaps, quality drift, or performance degradation.",
        ),
        (
            "Impact Analysis",
            "The blast radius mapper. Follows the dependency graph downstream to identify "
            "every affected model, dashboard, and stakeholder. Answers: \"Who needs to know "
            "and what decisions are blocked?\"",
        ),
        (
            "Reporting",
            "The communicator. Takes findings from all agents and generates three tailored "
            "reports: a technical brief for data engineers, an operational summary for "
            "analytics managers, and a plain-English note for business stakeholders.",
        ),
    ]

    for name, desc in agents:
        with st.expander(f"**{name} Agent**", expanded=False):
            st.markdown(desc)

    st.divider()

    st.markdown(f"""
    <div style="text-align:center; padding:2rem; background:{MIST}; border-radius:12px;">
        <p style="color:{SLATE}; font-size:1rem;">
            Built with Claude, LangGraph, dbt, and DuckDB
        </p>
        <p style="color:{STONE}; font-size:0.85rem;">
            Part of the <strong>MeridianIQ</strong> suite — AI-powered operational intelligence
        </p>
    </div>
    """, unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════
# Router
# ═══════════════════════════════════════════════════════════════════════════
PAGES = {
    "Pipeline Monitor": page_pipeline_monitor,
    "Data Quality": page_data_quality,
    "Impact Analysis": page_impact_analysis,
    "Executive Report": page_executive_report,
    "Demo Scenarios": page_demo_scenarios,
    "About": page_about,
}

PAGES[page]()
