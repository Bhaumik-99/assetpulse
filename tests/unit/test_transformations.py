from __future__ import annotations

import polars as pl
import pytest

from src.storage.storage_manager import StorageManager
from src.transformations.bronze_to_silver import BronzeToSilverTransformer
from src.utils.config import load_config, reset_config_cache


@pytest.fixture(autouse=True)
def _clear_config():
    reset_config_cache()
    yield
    reset_config_cache()


@pytest.fixture
def silver_input_df():
    return pl.DataFrame(
        {
            "unit_id": [1, 1, 1, 2, 2, 2],
            "cycle": [1, 2, 3, 1, 2, 3],
            "operational_setting_1": [0.0] * 6,
            "operational_setting_2": [0.0] * 6,
            "operational_setting_3": [100.0] * 6,
            **{f"sensor_{i:02d}": [float(i * 100 + j) for j in range(6)] for i in range(1, 22)},
            "source_file": ["train_FD001.txt"] * 6,
            "dataset_id": ["FD001"] * 6,
            "ingested_at": ["2024-01-01T00:00:00"] * 6,
            "pipeline_run_id": ["test-run"] * 6,
            "source_row_number": list(range(1, 7)),
        }
    )


class TestBronzeToSilverTransformer:
    def test_transform_adds_quality_columns(self, silver_input_df, tmp_path):
        storage = StorageManager(tmp_path)
        config = load_config()
        transformer = BronzeToSilverTransformer(config=config, storage=storage)
        result = transformer.transform(silver_input_df, "FD001")

        assert "record_quality_status" in result.columns
        assert "has_cycle_gap" in result.columns
        assert "has_sensor_range_violation" in result.columns
        assert "has_sensor_spike" in result.columns
        assert "silver_processed_at" in result.columns

    def test_transform_sorts_by_unit_cycle(self, silver_input_df, tmp_path):
        storage = StorageManager(tmp_path)
        config = load_config()
        transformer = BronzeToSilverTransformer(config=config, storage=storage)
        result = transformer.transform(silver_input_df, "FD001")

        for uid in result["unit_id"].unique().to_list():
            unit_cycles = result.filter(pl.col("unit_id") == uid)["cycle"].to_list()
            assert unit_cycles == sorted(unit_cycles)

    def test_transform_quality_status_values(self, silver_input_df, tmp_path):
        storage = StorageManager(tmp_path)
        config = load_config()
        transformer = BronzeToSilverTransformer(config=config, storage=storage)
        result = transformer.transform(silver_input_df, "FD001")

        valid_statuses = {"VALID", "WARNING", "QUARANTINED"}
        actual_statuses = set(result["record_quality_status"].unique().to_list())
        assert actual_statuses.issubset(valid_statuses)

    def test_transform_writes_parquet(self, silver_input_df, tmp_path):
        storage = StorageManager(tmp_path)
        config = load_config()
        transformer = BronzeToSilverTransformer(config=config, storage=storage)
        transformer.transform(silver_input_df, "FD001")

        silver_files = list(tmp_path.rglob("*.parquet"))
        assert len(silver_files) > 0

    def test_no_cycle_gaps_in_sequential_data(self, tmp_path):
        df = pl.DataFrame(
            {
                "unit_id": [1, 1, 1],
                "cycle": [1, 2, 3],
                "operational_setting_1": [0.0] * 3,
                "operational_setting_2": [0.0] * 3,
                "operational_setting_3": [100.0] * 3,
                **{f"sensor_{i:02d}": [640.0, 641.0, 642.0] for i in range(1, 22)},
                "source_file": ["a.txt"] * 3,
                "dataset_id": ["FD001"] * 3,
                "ingested_at": ["2024-01-01"] * 3,
                "pipeline_run_id": ["x"] * 3,
                "source_row_number": [1, 2, 3],
            }
        )
        storage = StorageManager(tmp_path)
        config = load_config()
        transformer = BronzeToSilverTransformer(config=config, storage=storage)
        result = transformer.transform(df, "FD001")

        gap_flags = result["has_cycle_gap"].to_list()
        assert not any(gap_flags)
