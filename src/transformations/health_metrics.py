from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from src.monitoring.pipeline_logger import get_logger
from src.storage.storage_manager import StorageManager
from src.utils.config import AssetPulseConfig, get_project_root, load_config
from src.utils.exceptions import TransformationError

logger = get_logger(__name__)

SENSOR_COLUMNS = [f"sensor_{i:02d}" for i in range(1, 22)]


class HealthMetricsCalculator:
    def __init__(
        self,
        config: AssetPulseConfig | None = None,
        storage: StorageManager | None = None,
    ) -> None:
        self._config = config or load_config()
        self._project_root = get_project_root()
        self._storage = storage or StorageManager(self._project_root)
        self._hs = self._config.health_score

    def calculate_rul(self, df: pl.DataFrame) -> pl.DataFrame:
        if "unit_id" not in df.columns or "cycle" not in df.columns:
            raise TransformationError("DataFrame must have unit_id and cycle columns")

        return df.with_columns((pl.col("cycle").max().over("unit_id") - pl.col("cycle")).alias("remaining_useful_life"))

    def calculate_health_score(self, df: pl.DataFrame) -> pl.DataFrame:
        available_sensors = [s for s in SENSOR_COLUMNS if s in df.columns]
        if not available_sensors:
            return df.with_columns(pl.lit(100.0).alias("health_score"))

        sensor_means = {s: df[s].mean() for s in available_sensors if df[s].mean() is not None}
        sensor_stds = {s: df[s].std() for s in available_sensors if df[s].std() is not None and df[s].std() > 0}

        deviation_exprs: list[pl.Expr] = []
        for sensor in available_sensors:
            if sensor in sensor_means and sensor in sensor_stds:
                deviation_exprs.append(((pl.col(sensor) - sensor_means[sensor]) / sensor_stds[sensor]).abs())

        if not deviation_exprs:
            return df.with_columns(pl.lit(100.0).alias("health_score"))

        avg_deviation = deviation_exprs[0]
        for expr in deviation_exprs[1:]:
            avg_deviation = avg_deviation + expr
        avg_deviation = avg_deviation / len(deviation_exprs)

        sensor_penalty = avg_deviation.clip(0, 50) * self._hs.sensor_deviation_weight

        spike_penalty = pl.lit(0.0)
        if "has_sensor_spike" in df.columns:
            spike_penalty = pl.col("has_sensor_spike").cast(pl.Float64) * 10.0 * self._hs.spike_penalty_weight

        max_cycle = pl.col("cycle").max().over("unit_id")
        degradation_ratio = pl.col("cycle") / max_cycle
        degradation_penalty = degradation_ratio * 30.0 * self._hs.degradation_penalty_weight

        quality_penalty = pl.lit(0.0)
        if "has_sensor_range_violation" in df.columns:
            quality_penalty = (
                pl.col("has_sensor_range_violation").cast(pl.Float64) * 15.0 * self._hs.quality_penalty_weight
            )

        raw_score = pl.lit(100.0) - sensor_penalty - spike_penalty - degradation_penalty - quality_penalty
        health_score = raw_score.clip(0, 100)

        return df.with_columns(health_score.alias("health_score"))

    def assign_risk_level(self, df: pl.DataFrame) -> pl.DataFrame:
        if "health_score" not in df.columns:
            raise TransformationError("health_score column required")

        thresholds = self._hs.risk_thresholds
        return df.with_columns(
            pl.when(pl.col("health_score") <= thresholds.critical)
            .then(pl.lit("CRITICAL"))
            .when(pl.col("health_score") <= thresholds.high)
            .then(pl.lit("HIGH"))
            .when(pl.col("health_score") <= thresholds.medium)
            .then(pl.lit("MEDIUM"))
            .otherwise(pl.lit("LOW"))
            .alias("risk_level")
        )

    def calculate_degradation_score(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.with_columns((pl.lit(100.0) - pl.col("health_score")).alias("degradation_score"))

    def build_equipment_health(self, silver_df: pl.DataFrame) -> pl.DataFrame:
        df = silver_df.sort(["unit_id", "cycle"])

        df = self.calculate_rul(df)
        df = self.calculate_health_score(df)
        df = self.assign_risk_level(df)
        df = self.calculate_degradation_score(df)

        now = datetime.now(UTC)

        equipment_key_expr = (
            pl.col("unit_id").cast(pl.Utf8) + pl.lit("_") + pl.col("dataset_id").cast(pl.Utf8)
            if "dataset_id" in df.columns
            else pl.col("unit_id").cast(pl.Utf8)
        )

        health_df = df.select(
            [
                equipment_key_expr.alias("equipment_key"),
                "unit_id",
                "cycle",
                "health_score",
                "degradation_score",
                "remaining_useful_life",
                "risk_level",
            ]
        ).with_columns(pl.lit(now).alias("calculated_at"))

        output_path = Path(self._config.data_paths.gold_dir) / "analytics" / "fact_equipment_health.parquet"
        self._storage.write_parquet(health_df, output_path)

        return health_df

    def build_equipment_summary(self, health_df: pl.DataFrame, silver_df: pl.DataFrame) -> pl.DataFrame:
        summary = health_df.group_by("equipment_key", "unit_id").agg(
            [
                pl.col("cycle").max().alias("latest_cycle"),
                pl.col("health_score").last().alias("health_score"),
                pl.col("remaining_useful_life").last().alias("estimated_rul"),
                pl.col("risk_level").last().alias("risk_level"),
            ]
        )

        warning_counts = pl.DataFrame()
        if "has_sensor_range_violation" in silver_df.columns:
            warning_counts = silver_df.group_by("unit_id").agg(
                pl.col("has_sensor_range_violation").sum().alias("sensors_in_warning_state")
            )

        if len(warning_counts) > 0:
            summary = summary.join(warning_counts, on="unit_id", how="left")
            summary = summary.with_columns(pl.col("sensors_in_warning_state").fill_null(0))
        else:
            summary = summary.with_columns(pl.lit(0).alias("sensors_in_warning_state"))

        now = datetime.now(UTC)
        summary = summary.with_columns(pl.lit(now).alias("last_updated_at"))

        output_path = Path(self._config.data_paths.gold_dir) / "analytics" / "equipment_health_summary.parquet"
        self._storage.write_parquet(summary, output_path)

        return summary

    def build_dim_equipment(self, silver_df: pl.DataFrame) -> pl.DataFrame:
        dataset_id_col = "dataset_id" if "dataset_id" in silver_df.columns else None

        group_cols = ["unit_id"]
        if dataset_id_col:
            group_cols.append(dataset_id_col)

        dim = silver_df.group_by(group_cols).agg(
            [
                pl.col("cycle").min().alias("first_observed_cycle"),
                pl.col("cycle").max().alias("last_observed_cycle"),
                pl.col("cycle").count().alias("total_operational_cycles"),
            ]
        )

        key_expr = pl.col("unit_id").cast(pl.Utf8)
        if dataset_id_col:
            key_expr = key_expr + pl.lit("_") + pl.col(dataset_id_col).cast(pl.Utf8)

        dim = dim.with_columns(
            [
                key_expr.alias("equipment_key"),
                pl.lit("turbofan_engine").alias("equipment_type"),
            ]
        )

        output_path = Path(self._config.data_paths.gold_dir) / "analytics" / "dim_equipment.parquet"
        self._storage.write_parquet(dim, output_path)

        return dim

    def run_all(self, silver_df: pl.DataFrame) -> dict[str, pl.DataFrame]:
        dim_equipment = self.build_dim_equipment(silver_df)
        health_df = self.build_equipment_health(silver_df)
        summary_df = self.build_equipment_summary(health_df, silver_df)

        return {
            "dim_equipment": dim_equipment,
            "fact_equipment_health": health_df,
            "equipment_health_summary": summary_df,
        }


def run_health_metrics(dataset_id: str | None = None) -> None:
    config = load_config()
    project_root = get_project_root()
    storage = StorageManager(project_root)

    silver_files = storage.list_parquet_files(config.data_paths.silver_dir)
    if not silver_files:
        logger.warning("No Silver files found")
        return

    frames = [pl.read_parquet(f) for f in silver_files]
    silver_df = pl.concat(frames) if len(frames) > 1 else frames[0]

    calculator = HealthMetricsCalculator(config=config, storage=storage)
    results = calculator.run_all(silver_df)

    for name, df in results.items():
        logger.info(f"Generated {name}: {len(df)} records")


if __name__ == "__main__":
    run_health_metrics()
