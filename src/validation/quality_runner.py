from __future__ import annotations

import sys

import polars as pl

from src.monitoring.pipeline_logger import get_logger
from src.monitoring.run_tracker import RunTracker
from src.storage.storage_manager import StorageManager
from src.utils.config import AssetPulseConfig, get_project_root, load_config
from src.validation.completeness_checks import run_completeness_checks
from src.validation.cycle_gap_checks import detect_cycle_gaps
from src.validation.duplicate_checks import detect_duplicates
from src.validation.freshness_checks import check_freshness
from src.validation.range_checks import check_sensor_ranges, quarantine_range_violations
from src.validation.schema_checks import QualityResult, run_schema_checks
from src.validation.spike_checks import detect_spikes

logger = get_logger(__name__)


class QualityRunner:
    def __init__(
        self,
        config: AssetPulseConfig | None = None,
        run_tracker: RunTracker | None = None,
    ) -> None:
        self._config = config or load_config()
        self._project_root = get_project_root()

        if run_tracker:
            self._tracker = run_tracker
        else:
            db_path = self._project_root / self._config.duckdb.database_path
            self._tracker = RunTracker(db_path)

    def validate_bronze(
        self,
        df: pl.DataFrame,
        pipeline_run_id: str,
        dataset_name: str = "sensor_readings",
    ) -> list[QualityResult]:
        all_results: list[QualityResult] = []

        all_results.extend(run_schema_checks(df, include_metadata=True))
        all_results.extend(run_completeness_checks(df))

        dup_result, _ = detect_duplicates(df)
        all_results.append(dup_result)

        gap_result, _ = detect_cycle_gaps(df)
        all_results.append(gap_result)

        range_results, violations = check_sensor_ranges(df)
        all_results.extend(range_results)

        if len(violations) > 0:
            quarantine_dir = self._project_root / self._config.data_paths.quarantine_dir / "sensor_range_violations"
            quarantine_range_violations(violations, quarantine_dir)

        freshness_result = check_freshness(df, "bronze", self._config.freshness)
        all_results.append(freshness_result)

        spike_results, _ = detect_spikes(
            df,
            window_size=self._config.pipeline.spike_detection_window,
            threshold_sigma=self._config.pipeline.spike_threshold_sigma,
        )
        all_results.extend(spike_results)

        for result in all_results:
            result.pipeline_run_id = pipeline_run_id
            self._tracker.record_quality_result(
                run_id=pipeline_run_id,
                dataset_layer="bronze",
                dataset_name=dataset_name,
                check_name=result.check_name,
                check_type=result.check_type,
                status=result.status,
                records_checked=result.records_checked,
                failed_records=result.failed_records,
                failure_percentage=result.failure_percentage,
                details=result.details,
            )

        passed = sum(1 for r in all_results if r.status == "PASS")
        warned = sum(1 for r in all_results if r.status == "WARN")
        failed = sum(1 for r in all_results if r.status == "FAIL")

        logger.info(
            f"Bronze validation: {passed} PASS, {warned} WARN, {failed} FAIL",
            extra={"event": "validation_complete", "dataset_id": dataset_name},
        )

        return all_results

    def validate_silver(
        self,
        df: pl.DataFrame,
        pipeline_run_id: str,
        dataset_name: str = "sensor_readings",
    ) -> list[QualityResult]:
        all_results: list[QualityResult] = []

        all_results.extend(run_schema_checks(df, include_metadata=True))
        all_results.extend(run_completeness_checks(df))

        freshness_result = check_freshness(df, "silver", self._config.freshness, timestamp_column="silver_processed_at")
        all_results.append(freshness_result)

        for result in all_results:
            result.pipeline_run_id = pipeline_run_id
            self._tracker.record_quality_result(
                run_id=pipeline_run_id,
                dataset_layer="silver",
                dataset_name=dataset_name,
                check_name=result.check_name,
                check_type=result.check_type,
                status=result.status,
                records_checked=result.records_checked,
                failed_records=result.failed_records,
                failure_percentage=result.failure_percentage,
                details=result.details,
            )

        return all_results

    def validate_gold(
        self,
        df: pl.DataFrame,
        pipeline_run_id: str,
        dataset_name: str = "ml_features",
    ) -> list[QualityResult]:
        all_results: list[QualityResult] = []

        total = len(df)
        result = QualityResult(
            check_name="gold_not_empty",
            check_type="completeness",
            status="PASS" if total > 0 else "FAIL",
            records_checked=total,
            details=f"Gold dataset has {total} records",
        )
        all_results.append(result)

        if "unit_id" in df.columns and "cycle" in df.columns:
            dup_result, _ = detect_duplicates(df, ["unit_id", "cycle"])
            all_results.append(dup_result)

        for res in all_results:
            res.pipeline_run_id = pipeline_run_id
            self._tracker.record_quality_result(
                run_id=pipeline_run_id,
                dataset_layer="gold",
                dataset_name=dataset_name,
                check_name=res.check_name,
                check_type=res.check_type,
                status=res.status,
                records_checked=res.records_checked,
                failed_records=res.failed_records,
                failure_percentage=res.failure_percentage,
                details=res.details,
            )

        return all_results

    def get_summary(self, results: list[QualityResult]) -> dict:
        total = len(results)
        passed = sum(1 for r in results if r.status == "PASS")
        warned = sum(1 for r in results if r.status == "WARN")
        failed = sum(1 for r in results if r.status == "FAIL")
        return {
            "total_checks": total,
            "passed": passed,
            "warned": warned,
            "failed": failed,
            "pass_percentage": round(passed / total * 100, 2) if total > 0 else 0.0,
        }


if __name__ == "__main__":
    layer = "bronze"
    if len(sys.argv) > 2 and sys.argv[1] == "--layer":
        layer = sys.argv[2]

    config = load_config()
    project_root = get_project_root()
    storage = StorageManager(project_root)

    if layer == "bronze":
        parquet_files = storage.list_parquet_files(config.data_paths.bronze_dir)
    elif layer == "silver":
        parquet_files = storage.list_parquet_files(config.data_paths.silver_dir)
    else:
        parquet_files = storage.list_parquet_files(config.data_paths.gold_dir)

    if not parquet_files:
        logger.warning(f"No parquet files found for layer: {layer}")
        sys.exit(0)

    db_path = project_root / config.duckdb.database_path
    tracker = RunTracker(db_path)
    run_id = tracker.create_pipeline_run(f"validate_{layer}", config.dataset.default_id)
    runner = QualityRunner(config=config, run_tracker=tracker)

    for pf in parquet_files:
        if "rul" in pf.name.lower():
            logger.info(f"Skipping validation for RUL file: {pf.name}")
            continue

        df = pl.read_parquet(pf)
        if layer == "bronze":
            results = runner.validate_bronze(df, run_id)
        elif layer == "silver":
            results = runner.validate_silver(df, run_id)
        else:
            results = runner.validate_gold(df, run_id)

        summary = runner.get_summary(results)
        logger.info(f"Validation summary for {pf.name}: {summary}")

    tracker.complete_pipeline_run(run_id, "SUCCESS")
