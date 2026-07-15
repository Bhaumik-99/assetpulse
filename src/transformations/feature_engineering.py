from __future__ import annotations

from pathlib import Path

import polars as pl
import yaml

from src.monitoring.pipeline_logger import get_logger
from src.storage.storage_manager import StorageManager
from src.utils.config import AssetPulseConfig, get_project_root, load_config
from src.utils.exceptions import TransformationError

logger = get_logger(__name__)


def load_feature_config(config_path: Path | None = None) -> dict:
    if config_path is None:
        config_path = get_project_root() / "config" / "features.yaml"
    if not config_path.exists():
        raise TransformationError(f"Feature config not found: {config_path}")
    with open(config_path) as f:
        return yaml.safe_load(f)


class FeatureEngineer:
    def __init__(
        self,
        config: AssetPulseConfig | None = None,
        storage: StorageManager | None = None,
        feature_config: dict | None = None,
    ) -> None:
        self._config = config or load_config()
        self._project_root = get_project_root()
        self._storage = storage or StorageManager(self._project_root)
        self._feature_config = feature_config or load_feature_config()

    def generate_features(self, silver_df: pl.DataFrame) -> pl.DataFrame:
        logger.info("Starting feature engineering", extra={"event": "features_start"})

        df = silver_df.sort(["unit_id", "cycle"])
        df = self._add_rolling_features(df)
        df = self._add_rate_of_change(df)
        df = self._add_lifecycle_features(df)
        df = self._add_quality_summary_features(df)

        output_path = Path(self._config.data_paths.gold_dir) / "ml_features" / "ml_features.parquet"
        self._storage.write_parquet(df, output_path)

        logger.info(
            f"Feature engineering complete: {len(df)} records, {len(df.columns)} columns",
            extra={"event": "features_complete"},
        )

        return df

    def _add_rolling_features(self, df: pl.DataFrame) -> pl.DataFrame:
        features_conf = self._feature_config.get("features", {})
        new_columns: list[pl.Expr] = []

        for sensor_name, sensor_conf in features_conf.items():
            if sensor_name not in df.columns:
                continue

            windows = sensor_conf.get("rolling_windows", [])
            statistics = sensor_conf.get("statistics", [])

            for window in windows:
                for stat in statistics:
                    col_name = f"{sensor_name}_rolling_{stat}_{window}"
                    if stat == "mean":
                        new_columns.append(
                            pl.col(sensor_name).rolling_mean(window_size=window).over("unit_id").alias(col_name)
                        )
                    elif stat == "std":
                        new_columns.append(
                            pl.col(sensor_name).rolling_std(window_size=window).over("unit_id").alias(col_name)
                        )
                    elif stat == "min":
                        new_columns.append(
                            pl.col(sensor_name).rolling_min(window_size=window).over("unit_id").alias(col_name)
                        )
                    elif stat == "max":
                        new_columns.append(
                            pl.col(sensor_name).rolling_max(window_size=window).over("unit_id").alias(col_name)
                        )

        if new_columns:
            df = df.with_columns(new_columns)

        return df

    def _add_rate_of_change(self, df: pl.DataFrame) -> pl.DataFrame:
        features_conf = self._feature_config.get("features", {})
        roc_columns: list[pl.Expr] = []

        for sensor_name, sensor_conf in features_conf.items():
            if sensor_name not in df.columns:
                continue
            if sensor_conf.get("rate_of_change", False):
                roc_columns.append(
                    (pl.col(sensor_name) - pl.col(sensor_name).shift(1).over("unit_id")).alias(
                        f"{sensor_name}_rate_of_change"
                    )
                )

        if roc_columns:
            df = df.with_columns(roc_columns)

        return df

    def _add_lifecycle_features(self, df: pl.DataFrame) -> pl.DataFrame:
        df = df.with_columns(
            [
                (pl.col("cycle") - pl.col("cycle").min().over("unit_id")).alias("cycles_since_first_observation"),
            ]
        )
        return df

    def _add_quality_summary_features(self, df: pl.DataFrame) -> pl.DataFrame:
        quality_cols: list[pl.Expr] = []

        if "has_cycle_gap" in df.columns:
            quality_cols.append(
                pl.col("has_cycle_gap").cast(pl.Int32).cum_sum().over("unit_id").alias("cycle_gap_count")
            )

        if "has_sensor_range_violation" in df.columns:
            quality_cols.append(
                pl.col("has_sensor_range_violation")
                .cast(pl.Int32)
                .cum_sum()
                .over("unit_id")
                .alias("sensor_warning_count")
            )

        if "has_sensor_spike" in df.columns:
            quality_cols.append(
                pl.col("has_sensor_spike").cast(pl.Int32).cum_sum().over("unit_id").alias("sensor_spike_count")
            )

        if quality_cols:
            df = df.with_columns(quality_cols)

        return df


def run_feature_engineering(dataset_id: str | None = None) -> None:
    config = load_config()
    project_root = get_project_root()
    storage = StorageManager(project_root)

    silver_files = storage.list_parquet_files(config.data_paths.silver_dir)
    if not silver_files:
        logger.warning("No Silver files found")
        return

    frames = [pl.read_parquet(f) for f in silver_files]
    silver_df = pl.concat(frames) if len(frames) > 1 else frames[0]

    engineer = FeatureEngineer(config=config, storage=storage)
    engineer.generate_features(silver_df)


if __name__ == "__main__":
    run_feature_engineering()
