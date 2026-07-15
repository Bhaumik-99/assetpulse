from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from scripts.seed_data import generate_synthetic_cmapss
from src.ingestion.cmapss_ingestor import CMAPSSIngestor, compute_file_checksum, detect_dataset_info
from src.monitoring.run_tracker import RunTracker
from src.storage.storage_manager import StorageManager
from src.utils.config import load_config, reset_config_cache
from src.utils.exceptions import IngestionError


@pytest.fixture(autouse=True)
def _clear_config():
    reset_config_cache()
    yield
    reset_config_cache()


@pytest.fixture
def tmp_workspace(tmp_path: Path):
    source_dir = tmp_path / "data" / "source"
    generate_synthetic_cmapss(source_dir, n_units=3, min_cycles=30, max_cycles=60, seed=99)
    return tmp_path


@pytest.fixture
def tracker(tmp_path: Path):
    db_path = tmp_path / "metadata" / "test.duckdb"
    return RunTracker(db_path)


@pytest.fixture
def storage(tmp_workspace: Path):
    return StorageManager(tmp_workspace)


@pytest.fixture
def ingestor(tmp_workspace: Path, tracker: RunTracker, storage: StorageManager):
    config = load_config()
    return CMAPSSIngestor(config=config, run_tracker=tracker, storage=storage)


class TestDetectDatasetInfo:
    def test_train_fd001(self):
        dataset_id, file_type = detect_dataset_info(Path("train_FD001.txt"))
        assert dataset_id == "FD001"
        assert file_type == "train"

    def test_test_fd002(self):
        dataset_id, file_type = detect_dataset_info(Path("test_FD002.txt"))
        assert dataset_id == "FD002"
        assert file_type == "test"

    def test_rul_fd003(self):
        dataset_id, file_type = detect_dataset_info(Path("RUL_FD003.txt"))
        assert dataset_id == "FD003"
        assert file_type == "RUL"

    def test_invalid_filename(self):
        with pytest.raises(IngestionError):
            detect_dataset_info(Path("random_file.txt"))


class TestFileChecksum:
    def test_checksum_deterministic(self, tmp_path: Path):
        f = tmp_path / "test.txt"
        f.write_text("hello world")
        c1 = compute_file_checksum(f)
        c2 = compute_file_checksum(f)
        assert c1 == c2
        assert len(c1) == 64

    def test_checksum_changes_with_content(self, tmp_path: Path):
        f = tmp_path / "test.txt"
        f.write_text("hello")
        c1 = compute_file_checksum(f)
        f.write_text("world")
        c2 = compute_file_checksum(f)
        assert c1 != c2


class TestCMAPSSIngestor:
    def test_ingest_train_file(self, ingestor: CMAPSSIngestor, tmp_workspace: Path, tracker: RunTracker):
        run_id = tracker.create_pipeline_run("test", "FD001")
        source = tmp_workspace / "data" / "source" / "train_FD001.txt"
        df = ingestor.ingest_file(source, run_id)

        assert df is not None
        assert "unit_id" in df.columns
        assert "cycle" in df.columns
        assert "sensor_01" in df.columns
        assert "sensor_21" in df.columns
        assert "source_file" in df.columns
        assert "dataset_id" in df.columns
        assert "ingested_at" in df.columns
        assert "pipeline_run_id" in df.columns
        assert "source_row_number" in df.columns
        assert len(df) > 0

    def test_ingest_rul_file(self, ingestor: CMAPSSIngestor, tmp_workspace: Path, tracker: RunTracker):
        run_id = tracker.create_pipeline_run("test", "FD001")
        source = tmp_workspace / "data" / "source" / "RUL_FD001.txt"
        df = ingestor.ingest_file(source, run_id)

        assert df is not None
        assert "unit_id" in df.columns
        assert "remaining_useful_life" in df.columns
        assert len(df) == 3

    def test_idempotent_ingestion(self, ingestor: CMAPSSIngestor, tmp_workspace: Path, tracker: RunTracker):
        run_id = tracker.create_pipeline_run("test", "FD001")
        source = tmp_workspace / "data" / "source" / "train_FD001.txt"

        df1 = ingestor.ingest_file(source, run_id)
        assert df1 is not None

        df2 = ingestor.ingest_file(source, run_id)
        assert df2 is None

    def test_column_types(self, ingestor: CMAPSSIngestor, tmp_workspace: Path, tracker: RunTracker):
        run_id = tracker.create_pipeline_run("test", "FD001")
        source = tmp_workspace / "data" / "source" / "train_FD001.txt"
        df = ingestor.ingest_file(source, run_id)

        assert df.schema["unit_id"] == pl.Int64
        assert df.schema["cycle"] == pl.Int64
        assert df.schema["sensor_01"] == pl.Float64
        assert df.schema["operational_setting_1"] == pl.Float64

    def test_source_row_numbers_sequential(self, ingestor: CMAPSSIngestor, tmp_workspace: Path, tracker: RunTracker):
        run_id = tracker.create_pipeline_run("test", "FD001")
        source = tmp_workspace / "data" / "source" / "train_FD001.txt"
        df = ingestor.ingest_file(source, run_id)

        row_nums = df["source_row_number"].to_list()
        assert row_nums == list(range(1, len(df) + 1))

    def test_bronze_parquet_written(self, ingestor: CMAPSSIngestor, tmp_workspace: Path, tracker: RunTracker):
        run_id = tracker.create_pipeline_run("test", "FD001")
        source = tmp_workspace / "data" / "source" / "train_FD001.txt"
        ingestor.ingest_file(source, run_id)

        bronze_path = tmp_workspace / "data" / "bronze" / "sensor_readings" / "train_FD001.parquet"
        assert bronze_path.exists()
        df = pl.read_parquet(bronze_path)
        assert len(df) > 0
