from __future__ import annotations

import sys
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from src.monitoring.pipeline_logger import get_logger
from src.storage.storage_manager import StorageManager
from src.utils.config import AssetPulseConfig, get_project_root, load_config, load_sensor_ranges
from src.validation.duplicate_checks import quarantine_duplicates

logger = get_logger(__name__)

SENSOR_COLUMNS = [f"sensor_{i:02d}" for i in range(1, 22)]


class BronzeToSilverTransformer:
    def __init__(
        self,
        config: AssetPulseConfig | None = None,
        storage: StorageManager | None = None,
    ) -> None:
        self._config = config or load_config()
        self._project_root = get_project_root()
        self._storage = storage or StorageManager(self._project_root)

    def transform(self, bronze_df: pl.DataFrame, dataset_id: str = "FD001") -> pl.DataFrame:
        logger.info("Starting Bronze → Silver transformation", extra={"event": "silver_start"})

        df = self._enforce_types(bronze_df)
        df = self._sort_timeseries(df)

        quarantine_dir = self._project_root / self._config.data_paths.quarantine_dir / "duplicates"
        df, _ = quarantine_duplicates(df, quarantine_dir)

        df = self._add_cycle_gap_flags(df)
        df = self._add_range_violation_flags(df)
        df = self._add_spike_flags(df)
        df = self._compute_quality_status(df)

        df = df.with_columns(pl.lit(datetime.now(UTC)).alias("silver_processed_at"))

        silver_path = (
            Path(self._config.data_paths.silver_dir)
            / "sensor_readings"
            / f"dataset_id={dataset_id}"
            / "sensor_readings.parquet"
        )
        self._storage.write_parquet(df, silver_path)

        logger.info(
            f"Silver transformation complete: {len(df)} records",
            extra={"event": "silver_complete"},
        )

        return df

    def _enforce_types(self, df: pl.DataFrame) -> pl.DataFrame:
        cast_exprs = [
            pl.col("unit_id").cast(pl.Int64),
            pl.col("cycle").cast(pl.Int64),
        ]
        for col in ["operational_setting_1", "operational_setting_2", "operational_setting_3"]:
            if col in df.columns:
                cast_exprs.append(pl.col(col).cast(pl.Float64))
        for sensor in SENSOR_COLUMNS:
            if sensor in df.columns:
                cast_exprs.append(pl.col(sensor).cast(pl.Float64))

        return df.with_columns(cast_exprs)

    def _sort_timeseries(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.sort(["unit_id", "cycle"])

    def _add_cycle_gap_flags(self, df: pl.DataFrame) -> pl.DataFrame:
        sorted_df = df.sort(["unit_id", "cycle"])
        gap_col = (pl.col("cycle") - pl.col("cycle").shift(1).over("unit_id")).alias("_cycle_diff")

        flagged = sorted_df.with_columns(gap_col)
        flagged = flagged.with_columns(
            ((pl.col("_cycle_diff") > 1) & pl.col("_cycle_diff").is_not_null()).alias("has_cycle_gap")
        )
        return flagged.drop("_cycle_diff")

    def _add_range_violation_flags(self, df: pl.DataFrame) -> pl.DataFrame:
        sensor_ranges = load_sensor_ranges()
        violation_exprs: list[pl.Expr] = []

        for sensor_name, range_entry in sensor_ranges.items():
            if sensor_name not in df.columns:
                continue
            violation_exprs.append((pl.col(sensor_name) < range_entry.min) | (pl.col(sensor_name) > range_entry.max))

        if not violation_exprs:
            return df.with_columns(pl.lit(False).alias("has_sensor_range_violation"))

        combined = violation_exprs[0]
        for expr in violation_exprs[1:]:
            combined = combined | expr

        return df.with_columns(combined.alias("has_sensor_range_violation"))

    def _add_spike_flags(self, df: pl.DataFrame) -> pl.DataFrame:
        window = self._config.pipeline.spike_detection_window
        sigma = self._config.pipeline.spike_threshold_sigma

        sorted_df = df.sort(["unit_id", "cycle"])
        spike_exprs: list[pl.Expr] = []

        for sensor in SENSOR_COLUMNS:
            if sensor not in sorted_df.columns:
                continue
            rolling_mean = pl.col(sensor).rolling_mean(window_size=window).over("unit_id")
            rolling_std = pl.col(sensor).rolling_std(window_size=window).over("unit_id")
            spike = ((pl.col(sensor) - rolling_mean).abs() > (sigma * rolling_std)).fill_null(False)
            spike_exprs.append(spike)

        if not spike_exprs:
            return sorted_df.with_columns(pl.lit(False).alias("has_sensor_spike"))

        combined = spike_exprs[0]
        for expr in spike_exprs[1:]:
            combined = combined | expr

        return sorted_df.with_columns(combined.alias("has_sensor_spike"))

    def _compute_quality_status(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.with_columns(
            pl.when(pl.col("has_sensor_range_violation") & pl.col("has_sensor_spike"))
            .then(pl.lit("QUARANTINED"))
            .when(pl.col("has_cycle_gap") | pl.col("has_sensor_range_violation") | pl.col("has_sensor_spike"))
            .then(pl.lit("WARNING"))
            .otherwise(pl.lit("VALID"))
            .alias("record_quality_status")
        )


def run_silver_transform(dataset_id: str | None = None) -> None:
    config = load_config()
    project_root = get_project_root()
    storage = StorageManager(project_root)

    target_dataset = dataset_id or config.dataset.default_id

    bronze_files = storage.list_parquet_files(config.data_paths.bronze_dir)
    if not bronze_files:
        logger.warning("No Bronze files found")
        return

    train_files = [f for f in bronze_files if "rul" not in f.stem.lower()]
    if not train_files:
        logger.warning("No non-RUL Bronze files found")
        return

    frames = [pl.read_parquet(f) for f in train_files]
    bronze_df = pl.concat(frames) if len(frames) > 1 else frames[0]

    transformer = BronzeToSilverTransformer(config=config, storage=storage)
    transformer.transform(bronze_df, target_dataset)


if __name__ == "__main__":
    ds_id = sys.argv[1] if len(sys.argv) > 1 else None
    run_silver_transform(ds_id)
