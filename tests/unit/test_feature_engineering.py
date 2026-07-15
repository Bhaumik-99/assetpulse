from __future__ import annotations

import polars as pl
import pytest

from src.storage.storage_manager import StorageManager
from src.transformations.feature_engineering import FeatureEngineer
from src.utils.config import load_config, reset_config_cache


@pytest.fixture(autouse=True)
def _clear_config():
    reset_config_cache()
    yield
    reset_config_cache()


@pytest.fixture
def sample_silver_df():
    n = 30
    return pl.DataFrame(
        {
            "unit_id": [1] * n,
            "cycle": list(range(1, n + 1)),
            "dataset_id": ["FD001"] * n,
            "operational_setting_1": [0.001] * n,
            "operational_setting_2": [0.0] * n,
            "operational_setting_3": [100.0] * n,
            **{f"sensor_{i:02d}": [float(v + i * 10) for v in range(n)] for i in range(1, 22)},
            "has_cycle_gap": [False] * n,
            "has_sensor_range_violation": [False] * n,
            "has_sensor_spike": [False] * n,
            "record_quality_status": ["VALID"] * n,
            "source_file": ["train_FD001.txt"] * n,
            "ingested_at": ["2024-01-01"] * n,
            "pipeline_run_id": ["test"] * n,
            "source_row_number": list(range(1, n + 1)),
            "silver_processed_at": ["2024-01-01"] * n,
        }
    )


class TestFeatureEngineer:
    def test_rolling_features_created(self, sample_silver_df, tmp_path):
        feature_config = {
            "features": {
                "sensor_02": {
                    "rolling_windows": [5],
                    "statistics": ["mean", "std"],
                    "rate_of_change": True,
                },
            }
        }
        storage = StorageManager(tmp_path)
        config = load_config()
        engineer = FeatureEngineer(config=config, storage=storage, feature_config=feature_config)
        result = engineer.generate_features(sample_silver_df)

        assert "sensor_02_rolling_mean_5" in result.columns
        assert "sensor_02_rolling_std_5" in result.columns
        assert "sensor_02_rate_of_change" in result.columns

    def test_lifecycle_features(self, sample_silver_df, tmp_path):
        feature_config = {"features": {}}
        storage = StorageManager(tmp_path)
        config = load_config()
        engineer = FeatureEngineer(config=config, storage=storage, feature_config=feature_config)
        result = engineer.generate_features(sample_silver_df)

        assert "cycles_since_first_observation" in result.columns
        first_val = result.filter(pl.col("cycle") == 1)["cycles_since_first_observation"].to_list()[0]
        assert first_val == 0

    def test_quality_summary_features(self, sample_silver_df, tmp_path):
        feature_config = {"features": {}}
        storage = StorageManager(tmp_path)
        config = load_config()
        engineer = FeatureEngineer(config=config, storage=storage, feature_config=feature_config)
        result = engineer.generate_features(sample_silver_df)

        assert "cycle_gap_count" in result.columns
        assert "sensor_warning_count" in result.columns
        assert "sensor_spike_count" in result.columns

    def test_output_parquet_written(self, sample_silver_df, tmp_path):
        feature_config = {"features": {}}
        storage = StorageManager(tmp_path)
        config = load_config()
        engineer = FeatureEngineer(config=config, storage=storage, feature_config=feature_config)
        engineer.generate_features(sample_silver_df)

        parquet_files = list(tmp_path.rglob("*.parquet"))
        assert len(parquet_files) > 0
