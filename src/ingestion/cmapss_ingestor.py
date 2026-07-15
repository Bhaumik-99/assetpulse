from __future__ import annotations

import hashlib
import re
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from src.monitoring.pipeline_logger import get_logger
from src.monitoring.run_tracker import RunTracker
from src.storage.storage_manager import StorageManager
from src.utils.config import MachinaFlowConfig, get_project_root, load_config
from src.utils.exceptions import IngestionError

logger = get_logger(__name__)

DATASET_PATTERN = re.compile(r"(train|test|RUL)_FD(\d{3})")


def compute_file_checksum(file_path: Path) -> str:
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


def detect_dataset_info(file_path: Path) -> tuple[str, str]:
    match = DATASET_PATTERN.search(file_path.stem)
    if not match:
        raise IngestionError(f"Cannot detect dataset version from filename: {file_path.name}")
    file_type = match.group(1)
    dataset_id = f"FD{match.group(2)}"
    return dataset_id, file_type


class CMAPSSIngestor:
    def __init__(
        self,
        config: MachinaFlowConfig | None = None,
        run_tracker: RunTracker | None = None,
        storage: StorageManager | None = None,
    ) -> None:
        self._config = config or load_config()
        self._project_root = get_project_root()
        self._column_names = self._config.column_names
        self._storage = storage or StorageManager(self._project_root)

        if run_tracker:
            self._tracker = run_tracker
        else:
            db_path = self._project_root / self._config.duckdb.database_path
            self._tracker = RunTracker(db_path)

    def ingest_file(self, source_path: Path, pipeline_run_id: str) -> pl.DataFrame | None:
        if not source_path.exists():
            raise IngestionError(f"Source file not found: {source_path}")

        dataset_id, file_type = detect_dataset_info(source_path)
        checksum = compute_file_checksum(source_path)

        logger.info(
            "Checking ingestion manifest",
            extra={"event": "manifest_check", "dataset_id": dataset_id},
        )

        if self._tracker.check_manifest(str(source_path.name), checksum):
            logger.info(
                "File already ingested, skipping (idempotent)",
                extra={"event": "skip_duplicate_ingestion", "dataset_id": dataset_id},
            )
            return None

        logger.info(
            f"Ingesting {source_path.name} as {dataset_id}/{file_type}",
            extra={"event": "ingestion_start", "dataset_id": dataset_id},
        )

        if file_type == "RUL":
            df = self._read_rul_file(source_path, dataset_id, pipeline_run_id)
        else:
            df = self._read_sensor_file(source_path, dataset_id, pipeline_run_id)

        bronze_path = Path(self._config.data_paths.bronze_dir) / "sensor_readings" / f"{source_path.stem}.parquet"
        self._storage.write_parquet(df, bronze_path)

        self._tracker.record_ingestion(
            source_file=source_path.name,
            file_checksum=checksum,
            dataset_id=dataset_id,
            row_count=len(df),
            run_id=pipeline_run_id,
        )

        logger.info(
            f"Ingested {len(df)} records to bronze",
            extra={"event": "ingestion_complete", "dataset_id": dataset_id},
        )

        return df

    def _read_sensor_file(self, source_path: Path, dataset_id: str, pipeline_run_id: str) -> pl.DataFrame:
        try:
            raw_text = source_path.read_text().strip()
        except Exception as e:
            raise IngestionError(f"Failed to read source file {source_path}: {e}") from e

        rows: list[list[str]] = []
        for line in raw_text.split("\n"):
            values = line.strip().split()
            rows.append(values)

        if not rows:
            raise IngestionError(f"Source file is empty: {source_path}")

        n_cols = len(rows[0])
        expected_cols = len(self._column_names)

        if n_cols > expected_cols:
            rows = [row[:expected_cols] for row in rows]
        elif n_cols < expected_cols:
            raise IngestionError(f"Source file has {n_cols} columns, expected at least {expected_cols}: {source_path}")

        df = pl.DataFrame({self._column_names[i]: [row[i] for row in rows] for i in range(expected_cols)})

        df = df.with_columns(
            [
                pl.col("unit_id").cast(pl.Int64),
                pl.col("cycle").cast(pl.Int64),
                *[pl.col(c).cast(pl.Float64) for c in self._column_names[2:]],
            ]
        )

        now = datetime.now(UTC)
        df = df.with_columns(
            [
                pl.lit(source_path.name).alias("source_file"),
                pl.lit(dataset_id).alias("dataset_id"),
                pl.lit(now).alias("ingested_at"),
                pl.lit(pipeline_run_id).alias("pipeline_run_id"),
                pl.arange(1, len(df) + 1, eager=True).alias("source_row_number"),
            ]
        )

        return df

    def _read_rul_file(self, source_path: Path, dataset_id: str, pipeline_run_id: str) -> pl.DataFrame:
        try:
            raw_text = source_path.read_text().strip()
        except Exception as e:
            raise IngestionError(f"Failed to read RUL file {source_path}: {e}") from e

        values = [int(line.strip()) for line in raw_text.split("\n") if line.strip()]

        df = pl.DataFrame(
            {
                "unit_id": list(range(1, len(values) + 1)),
                "remaining_useful_life": values,
            }
        )

        df = df.with_columns(
            [
                pl.col("unit_id").cast(pl.Int64),
                pl.col("remaining_useful_life").cast(pl.Int64),
            ]
        )

        now = datetime.now(UTC)
        df = df.with_columns(
            [
                pl.lit(source_path.name).alias("source_file"),
                pl.lit(dataset_id).alias("dataset_id"),
                pl.lit(now).alias("ingested_at"),
                pl.lit(pipeline_run_id).alias("pipeline_run_id"),
                pl.arange(1, len(df) + 1, eager=True).alias("source_row_number"),
            ]
        )

        return df

    def ingest_dataset(self, dataset_id: str, pipeline_run_id: str) -> dict[str, int]:
        source_dir = self._project_root / self._config.data_paths.source_dir
        if not source_dir.exists():
            raise IngestionError(f"Source directory not found: {source_dir}")

        results: dict[str, int] = {}
        for pattern in [f"train_{dataset_id}.txt", f"test_{dataset_id}.txt", f"RUL_{dataset_id}.txt"]:
            matched_files = list(source_dir.glob(pattern))
            for source_file in matched_files:
                df = self.ingest_file(source_file, pipeline_run_id)
                results[source_file.name] = len(df) if df is not None else 0

        return results


def run_ingestion(dataset_id: str | None = None) -> None:
    config = load_config()
    project_root = get_project_root()
    db_path = project_root / config.duckdb.database_path
    tracker = RunTracker(db_path)

    target_dataset = dataset_id or config.dataset.default_id
    run_id = tracker.create_pipeline_run("bronze_ingestion", target_dataset)
    tracker.start_task(run_id, "ingest_bronze")

    try:
        ingestor = CMAPSSIngestor(config=config, run_tracker=tracker)
        results = ingestor.ingest_dataset(target_dataset, run_id)
        total = sum(results.values())
        tracker.complete_task(run_id, "ingest_bronze", "SUCCESS", total)
        tracker.complete_pipeline_run(run_id, "SUCCESS", records_ingested=total)
        logger.info(f"Bronze ingestion complete: {results}")
    except Exception as e:
        tracker.complete_task(run_id, "ingest_bronze", "FAILED")
        tracker.complete_pipeline_run(run_id, "FAILED", error_message=str(e))
        raise


if __name__ == "__main__":
    import sys

    ds_id = sys.argv[1] if len(sys.argv) > 1 else None
    run_ingestion(ds_id)
