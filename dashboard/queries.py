from __future__ import annotations

from pathlib import Path

import duckdb
import polars as pl

from src.utils.config import get_project_root, load_config


def _get_gold_path(subpath: str) -> Path:
    config = load_config()
    return get_project_root() / config.data_paths.gold_dir / subpath


def _read_gold_parquet(subpath: str) -> pl.DataFrame | None:
    path = _get_gold_path(subpath)
    if not path.exists():
        return None
    return pl.read_parquet(path)


def get_equipment_health_summary() -> pl.DataFrame | None:
    return _read_gold_parquet("analytics/equipment_health_summary.parquet")


def get_equipment_health_detail() -> pl.DataFrame | None:
    return _read_gold_parquet("analytics/fact_equipment_health.parquet")


def get_dim_equipment() -> pl.DataFrame | None:
    return _read_gold_parquet("analytics/dim_equipment.parquet")


def get_ml_features() -> pl.DataFrame | None:
    return _read_gold_parquet("ml_features/ml_features.parquet")


def get_pipeline_runs() -> list[dict]:
    config = load_config()
    db_path = get_project_root() / config.duckdb.database_path
    if not db_path.exists():
        return []
    conn = duckdb.connect(str(db_path), read_only=True)
    result = conn.execute("SELECT * FROM pipeline_runs ORDER BY started_at DESC LIMIT 50")
    columns = [desc[0] for desc in result.description]
    data = [dict(zip(columns, row)) for row in result.fetchall()]
    conn.close()
    return data


def get_quality_results(limit: int = 200) -> list[dict]:
    config = load_config()
    db_path = get_project_root() / config.duckdb.database_path
    if not db_path.exists():
        return []
    conn = duckdb.connect(str(db_path), read_only=True)
    result = conn.execute(
        "SELECT * FROM data_quality_results ORDER BY checked_at DESC LIMIT ?", [limit]
    )
    columns = [desc[0] for desc in result.description]
    data = [dict(zip(columns, row)) for row in result.fetchall()]
    conn.close()
    return data


def get_task_metrics(run_id: str | None = None) -> list[dict]:
    config = load_config()
    db_path = get_project_root() / config.duckdb.database_path
    if not db_path.exists():
        return []
    conn = duckdb.connect(str(db_path), read_only=True)
    if run_id:
        result = conn.execute(
            "SELECT * FROM pipeline_task_metrics WHERE pipeline_run_id = ? ORDER BY started_at",
            [run_id],
        )
    else:
        result = conn.execute("SELECT * FROM pipeline_task_metrics ORDER BY started_at DESC LIMIT 100")
    columns = [desc[0] for desc in result.description]
    data = [dict(zip(columns, row)) for row in result.fetchall()]
    conn.close()
    return data


def get_overview_metrics() -> dict:
    summary = get_equipment_health_summary()
    if summary is None or len(summary) == 0:
        return {
            "total_units": 0,
            "avg_health_score": 0.0,
            "high_risk_count": 0,
            "critical_count": 0,
        }

    return {
        "total_units": len(summary),
        "avg_health_score": round(summary["health_score"].mean() or 0.0, 1),
        "high_risk_count": len(summary.filter(
            (pl.col("risk_level") == "HIGH") | (pl.col("risk_level") == "CRITICAL")
        )),
        "critical_count": len(summary.filter(pl.col("risk_level") == "CRITICAL")),
    }
