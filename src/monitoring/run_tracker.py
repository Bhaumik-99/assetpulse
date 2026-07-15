from __future__ import annotations

import uuid
from datetime import UTC, datetime
from pathlib import Path

import duckdb


class RunTracker:
    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = duckdb.connect(str(db_path))
        self._init_tables()

    def _init_tables(self) -> None:
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                pipeline_run_id VARCHAR PRIMARY KEY,
                pipeline_name VARCHAR NOT NULL,
                dataset_id VARCHAR,
                started_at TIMESTAMP WITH TIME ZONE NOT NULL,
                completed_at TIMESTAMP WITH TIME ZONE,
                status VARCHAR NOT NULL DEFAULT 'RUNNING',
                records_ingested INTEGER DEFAULT 0,
                records_silver INTEGER DEFAULT 0,
                records_gold INTEGER DEFAULT 0,
                quality_checks_passed INTEGER DEFAULT 0,
                quality_checks_failed INTEGER DEFAULT 0,
                error_message VARCHAR
            )
        """)
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_task_metrics (
                pipeline_run_id VARCHAR NOT NULL,
                task_name VARCHAR NOT NULL,
                started_at TIMESTAMP WITH TIME ZONE NOT NULL,
                completed_at TIMESTAMP WITH TIME ZONE,
                duration_seconds DOUBLE,
                records_processed INTEGER DEFAULT 0,
                status VARCHAR NOT NULL DEFAULT 'RUNNING',
                PRIMARY KEY (pipeline_run_id, task_name)
            )
        """)
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS data_quality_results (
                quality_result_id VARCHAR PRIMARY KEY,
                pipeline_run_id VARCHAR NOT NULL,
                dataset_layer VARCHAR NOT NULL,
                dataset_name VARCHAR NOT NULL,
                check_name VARCHAR NOT NULL,
                check_type VARCHAR NOT NULL,
                status VARCHAR NOT NULL,
                records_checked INTEGER DEFAULT 0,
                failed_records INTEGER DEFAULT 0,
                failure_percentage DOUBLE DEFAULT 0.0,
                checked_at TIMESTAMP WITH TIME ZONE NOT NULL,
                details VARCHAR
            )
        """)
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS ingestion_manifest (
                source_file VARCHAR NOT NULL,
                file_checksum VARCHAR NOT NULL,
                dataset_id VARCHAR NOT NULL,
                row_count INTEGER NOT NULL,
                ingested_at TIMESTAMP WITH TIME ZONE NOT NULL,
                pipeline_run_id VARCHAR NOT NULL,
                status VARCHAR NOT NULL DEFAULT 'SUCCESS',
                PRIMARY KEY (source_file, file_checksum)
            )
        """)

    def create_pipeline_run(self, pipeline_name: str, dataset_id: str) -> str:
        run_id = str(uuid.uuid4())
        self._conn.execute(
            """
            INSERT INTO pipeline_runs (pipeline_run_id, pipeline_name, dataset_id, started_at, status)
            VALUES (?, ?, ?, ?, 'RUNNING')
            """,
            [run_id, pipeline_name, dataset_id, datetime.now(UTC)],
        )
        return run_id

    def complete_pipeline_run(
        self,
        run_id: str,
        status: str,
        records_ingested: int = 0,
        records_silver: int = 0,
        records_gold: int = 0,
        quality_checks_passed: int = 0,
        quality_checks_failed: int = 0,
        error_message: str | None = None,
    ) -> None:
        self._conn.execute(
            """
            UPDATE pipeline_runs
            SET completed_at = ?,
                status = ?,
                records_ingested = ?,
                records_silver = ?,
                records_gold = ?,
                quality_checks_passed = ?,
                quality_checks_failed = ?,
                error_message = ?
            WHERE pipeline_run_id = ?
            """,
            [
                datetime.now(UTC),
                status,
                records_ingested,
                records_silver,
                records_gold,
                quality_checks_passed,
                quality_checks_failed,
                error_message,
                run_id,
            ],
        )

    def start_task(self, run_id: str, task_name: str) -> None:
        self._conn.execute(
            """
            INSERT INTO pipeline_task_metrics (pipeline_run_id, task_name, started_at, status)
            VALUES (?, ?, ?, 'RUNNING')
            ON CONFLICT (pipeline_run_id, task_name) DO UPDATE
            SET started_at = EXCLUDED.started_at, status = 'RUNNING'
            """,
            [run_id, task_name, datetime.now(UTC)],
        )

    def complete_task(
        self,
        run_id: str,
        task_name: str,
        status: str,
        records_processed: int = 0,
    ) -> None:
        now = datetime.now(UTC)
        self._conn.execute(
            """
            UPDATE pipeline_task_metrics
            SET completed_at = ?,
                duration_seconds = EXTRACT(EPOCH FROM (? - started_at)),
                records_processed = ?,
                status = ?
            WHERE pipeline_run_id = ? AND task_name = ?
            """,
            [now, now, records_processed, status, run_id, task_name],
        )

    def record_quality_result(
        self,
        run_id: str,
        dataset_layer: str,
        dataset_name: str,
        check_name: str,
        check_type: str,
        status: str,
        records_checked: int = 0,
        failed_records: int = 0,
        failure_percentage: float = 0.0,
        details: str | None = None,
    ) -> str:
        result_id = str(uuid.uuid4())
        self._conn.execute(
            """
            INSERT INTO data_quality_results
            (quality_result_id, pipeline_run_id, dataset_layer, dataset_name,
             check_name, check_type, status, records_checked, failed_records,
             failure_percentage, checked_at, details)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                result_id,
                run_id,
                dataset_layer,
                dataset_name,
                check_name,
                check_type,
                status,
                records_checked,
                failed_records,
                failure_percentage,
                datetime.now(UTC),
                details,
            ],
        )
        return result_id

    def check_manifest(self, source_file: str, file_checksum: str) -> bool:
        result = self._conn.execute(
            """
            SELECT COUNT(*) FROM ingestion_manifest
            WHERE source_file = ? AND file_checksum = ? AND status = 'SUCCESS'
            """,
            [source_file, file_checksum],
        ).fetchone()
        return result is not None and result[0] > 0

    def record_ingestion(
        self,
        source_file: str,
        file_checksum: str,
        dataset_id: str,
        row_count: int,
        run_id: str,
    ) -> None:
        self._conn.execute(
            """
            INSERT INTO ingestion_manifest
            (source_file, file_checksum, dataset_id, row_count, ingested_at, pipeline_run_id, status)
            VALUES (?, ?, ?, ?, ?, ?, 'SUCCESS')
            """,
            [source_file, file_checksum, dataset_id, row_count, datetime.now(UTC), run_id],
        )

    def get_pipeline_runs(self, limit: int = 50) -> list[dict]:
        result = self._conn.execute(
            "SELECT * FROM pipeline_runs ORDER BY started_at DESC LIMIT ?",
            [limit],
        )
        columns = [desc[0] for desc in result.description]
        return [dict(zip(columns, row, strict=False)) for row in result.fetchall()]

    def get_quality_results(self, run_id: str | None = None, limit: int = 200) -> list[dict]:
        if run_id:
            result = self._conn.execute(
                "SELECT * FROM data_quality_results WHERE pipeline_run_id = ? ORDER BY checked_at DESC",
                [run_id],
            )
        else:
            result = self._conn.execute(
                "SELECT * FROM data_quality_results ORDER BY checked_at DESC LIMIT ?",
                [limit],
            )
        columns = [desc[0] for desc in result.description]
        return [dict(zip(columns, row, strict=False)) for row in result.fetchall()]

    def get_task_metrics(self, run_id: str) -> list[dict]:
        result = self._conn.execute(
            "SELECT * FROM pipeline_task_metrics WHERE pipeline_run_id = ? ORDER BY started_at",
            [run_id],
        )
        columns = [desc[0] for desc in result.description]
        return [dict(zip(columns, row, strict=False)) for row in result.fetchall()]

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        return self._conn

    def close(self) -> None:
        self._conn.close()
