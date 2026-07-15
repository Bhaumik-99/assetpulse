from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import polars as pl
import pandas as pd

from dashboard.queries import (
    get_dim_equipment,
    get_equipment_health_detail,
    get_equipment_health_summary,
    get_ml_features,
    get_overview_metrics,
    get_pipeline_runs,
    get_quality_results,
    get_task_metrics,
)

st.set_page_config(
    page_title="AssetPulse — Industrial Equipment Monitor",
    page_icon="⚙️",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    .stMetric {
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
        padding: 1rem;
        border-radius: 0.75rem;
        border: 1px solid #0f3460;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.3);
    }
    .stMetric [data-testid="stMetricLabel"] {
        color: #b0b0c0 !important;
        font-weight: 600;
    }
    .stMetric [data-testid="stMetricValue"] {
        color: #ffffff !important;
        font-weight: bold;
    }
    .block-container { padding-top: 4rem; }
    h1 { color: #e94560; }
    h2 { color: #0f3460; }
</style>
""", unsafe_allow_html=True)


def render_overview():
    st.header("⚙️ System Overview")
    metrics = get_overview_metrics()
    summary = get_equipment_health_summary()
    quality = get_quality_results(limit=500)

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Equipment Units", metrics["total_units"])
    with col2:
        st.metric("Avg Health Score", f"{metrics['avg_health_score']:.1f}")
    with col3:
        st.metric("High Risk Units", metrics["high_risk_count"])
    with col4:
        st.metric("Critical Units", metrics["critical_count"])

    if quality:
        quality_df = pl.DataFrame(quality)
        passed = len(quality_df.filter(pl.col("status") == "PASS"))
        total = len(quality_df)
        pct = round(passed / total * 100, 1) if total > 0 else 0.0

        col5, col6 = st.columns(2)
        with col5:
            st.metric("Quality Pass Rate", f"{pct}%")
        with col6:
            st.metric("Total Quality Checks", total)

    if summary is not None and len(summary) > 0:
        st.subheader("Equipment Health Distribution")
        pdf = summary.to_pandas()
        fig = px.histogram(
            pdf, x="health_score", nbins=20,
            color_discrete_sequence=["#e94560"],
            title="Health Score Distribution",
        )
        fig.update_layout(
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
        )
        st.plotly_chart(fig, use_container_width=True)


def render_equipment_health():
    st.header("🔧 Equipment Health")
    summary = get_equipment_health_summary()
    health_detail = get_equipment_health_detail()

    if summary is None or len(summary) == 0:
        st.warning("No equipment health data available. Run the pipeline first.")
        return

    unit_ids = sorted(summary["unit_id"].to_list())
    selected_unit = st.selectbox("Select Equipment Unit", unit_ids)

    if selected_unit is not None:
        unit_summary = summary.filter(pl.col("unit_id") == selected_unit)
        if len(unit_summary) > 0:
            row = unit_summary.row(0, named=True)
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Health Score", f"{row['health_score']:.1f}")
            with col2:
                st.metric("Estimated RUL", row.get("estimated_rul", "N/A"))
            with col3:
                st.metric("Risk Level", row.get("risk_level", "N/A"))
            with col4:
                st.metric("Latest Cycle", row.get("latest_cycle", "N/A"))

        if health_detail is not None:
            unit_health = health_detail.filter(
                pl.col("equipment_key").str.starts_with(str(selected_unit))
            ).sort("cycle")

            if len(unit_health) > 0:
                st.subheader("Health Score Trend")
                pdf = unit_health.to_pandas()
                fig = px.line(
                    pdf, x="cycle", y="health_score",
                    color_discrete_sequence=["#e94560"],
                    title=f"Unit {selected_unit} — Health Score Over Cycles",
                )
                fig.update_layout(
                    plot_bgcolor="rgba(0,0,0,0)",
                    paper_bgcolor="rgba(0,0,0,0)",
                )
                st.plotly_chart(fig, use_container_width=True)

    features = get_ml_features()
    if features is not None and selected_unit is not None:
        unit_features = features.filter(pl.col("unit_id") == selected_unit).sort("cycle")
        if len(unit_features) > 0:
            sensor_cols = [c for c in unit_features.columns if c.startswith("sensor_") and "_rolling" not in c and "_rate" not in c]
            if sensor_cols:
                selected_sensor = st.selectbox("Select Sensor", sensor_cols[:10])
                if selected_sensor and selected_sensor in unit_features.columns:
                    pdf = unit_features.select(["cycle", selected_sensor]).to_pandas()
                    fig = px.line(
                        pdf, x="cycle", y=selected_sensor,
                        title=f"Unit {selected_unit} — {selected_sensor}",
                        color_discrete_sequence=["#0f3460"],
                    )
                    st.plotly_chart(fig, use_container_width=True)


def render_maintenance_priority():
    st.header("🔴 Maintenance Priority")
    summary = get_equipment_health_summary()

    if summary is None or len(summary) == 0:
        st.warning("No data available.")
        return

    priority = summary.sort("health_score")
    priority = priority.with_row_index("priority_rank", offset=1)

    display_cols = ["priority_rank", "unit_id", "health_score", "estimated_rul", "risk_level", "sensors_in_warning_state"]
    available_cols = [c for c in display_cols if c in priority.columns]

    st.dataframe(
        priority.select(available_cols).to_pandas(),
        use_container_width=True,
        hide_index=True,
    )


def render_data_quality():
    st.header("✅ Data Quality")
    quality = get_quality_results(limit=500)

    if not quality:
        st.warning("No quality results available.")
        return

    qdf = pl.DataFrame(quality)

    passed = len(qdf.filter(pl.col("status") == "PASS"))
    warned = len(qdf.filter(pl.col("status") == "WARN"))
    failed = len(qdf.filter(pl.col("status") == "FAIL"))
    total = len(qdf)

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Passed", passed)
    with col2:
        st.metric("Warnings", warned)
    with col3:
        st.metric("Failed", failed)
    with col4:
        pct = round(passed / total * 100, 1) if total > 0 else 0.0
        st.metric("Pass Rate", f"{pct}%")

    if failed > 0:
        st.subheader("Failed Checks")
        failed_df = qdf.filter(pl.col("status") == "FAIL")
        display = ["check_name", "check_type", "dataset_layer", "failed_records", "failure_percentage", "details"]
        available = [c for c in display if c in failed_df.columns]
        st.dataframe(failed_df.select(available).to_pandas(), use_container_width=True, hide_index=True)

    st.subheader("Quality Check Distribution")
    status_counts = {"PASS": passed, "WARN": warned, "FAIL": failed}
    fig = px.pie(
        names=list(status_counts.keys()),
        values=list(status_counts.values()),
        color=list(status_counts.keys()),
        color_discrete_map={"PASS": "#2ecc71", "WARN": "#f39c12", "FAIL": "#e74c3c"},
        title="Quality Check Results",
    )
    st.plotly_chart(fig, use_container_width=True)


def render_pipeline_monitoring():
    st.header("📊 Pipeline Monitoring")
    runs = get_pipeline_runs()

    if not runs:
        st.warning("No pipeline runs recorded.")
        return

    runs_df = pl.DataFrame(runs)

    st.subheader("Pipeline Run History")
    display_cols = ["pipeline_run_id", "pipeline_name", "status", "started_at", "completed_at",
                    "records_ingested", "records_silver", "quality_checks_passed", "quality_checks_failed"]
    available = [c for c in display_cols if c in runs_df.columns]
    st.dataframe(runs_df.select(available).to_pandas(), use_container_width=True, hide_index=True)

    if len(runs) > 0:
        selected_run = st.selectbox("Select Run for Details", [r["pipeline_run_id"] for r in runs])
        if selected_run:
            tasks = get_task_metrics(selected_run)
            if tasks:
                st.subheader("Task Metrics")
                tasks_df = pl.DataFrame(tasks)
                task_cols = ["task_name", "status", "duration_seconds", "records_processed", "started_at"]
                avail = [c for c in task_cols if c in tasks_df.columns]
                st.dataframe(tasks_df.select(avail).to_pandas(), use_container_width=True, hide_index=True)


def main():
    st.sidebar.title("⚙️ AssetPulse")
    st.sidebar.markdown("Industrial Equipment Monitor")

    page = st.sidebar.radio(
        "Navigation",
        ["Overview", "Equipment Health", "Maintenance Priority", "Data Quality", "Pipeline Monitoring"],
    )

    if page == "Overview":
        render_overview()
    elif page == "Equipment Health":
        render_equipment_health()
    elif page == "Maintenance Priority":
        render_maintenance_priority()
    elif page == "Data Quality":
        render_data_quality()
    elif page == "Pipeline Monitoring":
        render_pipeline_monitoring()


if __name__ == "__main__":
    main()
