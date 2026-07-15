from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from scripts.seed_data import generate_synthetic_cmapss
from src.ingestion.cmapss_ingestor import CMAPSSIngestor
from src.monitoring.run_tracker import RunTracker
from src.storage.storage_manager import StorageManager
from src.transformations.bronze_to_silver import BronzeToSilverTransformer
from src.transformations.feature_engineering import FeatureEngineer
from src.transformations.health_metrics import HealthMetricsCalculator
from src.utils.config import load_config, reset_config_cache
from src.validation.quality_runner import QualityRunner


@pytest.fixture(autouse=True)
def _clear_config():
    reset_config_cache()
    yield
    reset_config_cache()


@pytest.mark.integration
class TestFullPipeline:
    def test_end_to_end_pipeline(self, tmp_path: Path):
        source_dir = tmp_path / "data" / "source"
        generate_synthetic_cmapss(source_dir, n_units=3, min_cycles=30, max_cycles=60, seed=42)

        config = load_config()
        db_path = tmp_path / "metadata" / "test.duckdb"
        tracker = RunTracker(db_path)
        storage = StorageManager(tmp_path)

        run_id = tracker.create_pipeline_run("integration_test", "FD001")

        ingestor = CMAPSSIngestor(config=config, run_tracker=tracker, storage=storage)
        train_file = source_dir / "train_FD001.txt"
        bronze_df = ingestor.ingest_file(train_file, run_id)

        assert bronze_df is not None
        assert len(bronze_df) > 0
        assert "unit_id" in bronze_df.columns
        assert "sensor_21" in bronze_df.columns
        assert "source_file" in bronze_df.columns

        bronze_files = storage.list_parquet_files("data/bronze")
        assert len(bronze_files) > 0

        runner = QualityRunner(config=config, run_tracker=tracker)
        quality_results = runner.validate_bronze(bronze_df, run_id)
        assert len(quality_results) > 0

        passed = sum(1 for r in quality_results if r.status == "PASS")
        assert passed > 0

        transformer = BronzeToSilverTransformer(config=config, storage=storage)
        silver_df = transformer.transform(bronze_df, "FD001")

        assert len(silver_df) > 0
        assert "record_quality_status" in silver_df.columns
        assert "has_cycle_gap" in silver_df.columns
        assert "silver_processed_at" in silver_df.columns

        silver_files = storage.list_parquet_files("data/silver")
        assert len(silver_files) > 0

        feature_config = {
            "features": {
                "sensor_02": {
                    "rolling_windows": [5],
                    "statistics": ["mean"],
                    "rate_of_change": True,
                },
            }
        }
        engineer = FeatureEngineer(config=config, storage=storage, feature_config=feature_config)
        feature_df = engineer.generate_features(silver_df)

        assert len(feature_df) > 0
        assert "sensor_02_rolling_mean_5" in feature_df.columns
        assert "sensor_02_rate_of_change" in feature_df.columns
        assert "cycles_since_first_observation" in feature_df.columns

        gold_feature_files = storage.list_parquet_files("data/gold/ml_features")
        assert len(gold_feature_files) > 0

        calculator = HealthMetricsCalculator(config=config, storage=storage)
        results = calculator.run_all(silver_df)

        assert "dim_equipment" in results
        assert "fact_equipment_health" in results
        assert "equipment_health_summary" in results

        dim_eq = results["dim_equipment"]
        assert len(dim_eq) == 3
        assert "equipment_key" in dim_eq.columns

        health = results["fact_equipment_health"]
        assert "health_score" in health.columns
        assert "remaining_useful_life" in health.columns
        assert "risk_level" in health.columns

        scores = health["health_score"].to_list()
        assert all(0 <= s <= 100 for s in scores)

        last_cycles = health.group_by("unit_id").agg(pl.col("remaining_useful_life").min())
        for rul in last_cycles["remaining_useful_life"].to_list():
            assert rul == 0

        gold_files = storage.list_parquet_files("data/gold")
        assert len(gold_files) >= 3

        gold_results = runner.validate_gold(feature_df, run_id)
        assert len(gold_results) > 0

        tracker.complete_pipeline_run(run_id, "SUCCESS")
        runs = tracker.get_pipeline_runs()
        assert len(runs) >= 1
        assert runs[0]["status"] == "SUCCESS"

        quality_stored = tracker.get_quality_results(run_id)
        assert len(quality_stored) > 0
