from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from airflow import DAG
from airflow.operators.python import PythonOperator

import polars as pl
from src.ingestion.cmapss_ingestor import CMAPSSIngestor
from src.monitoring.pipeline_logger import get_logger, update_logger_context
from src.monitoring.run_tracker import RunTracker
from src.storage.storage_manager import StorageManager
from src.transformations.bronze_to_silver import BronzeToSilverTransformer
from src.transformations.feature_engineering import FeatureEngineer
from src.transformations.health_metrics import HealthMetricsCalculator
from src.utils.config import get_project_root, load_config
from src.validation.quality_runner import QualityRunner

logger = get_logger(__name__)

DEFAULT_ARGS = {
    "owner": "machinaflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}


def on_failure_callback(context):
    task_instance = context.get("task_instance")
    logger.error(
        f"Task failed: {task_instance.task_id}",
        extra={
            "event": "task_failure",
            "task_name": task_instance.task_id,
        },
    )


def _get_components():
    config = load_config()
    project_root = get_project_root()
    db_path = project_root / config.duckdb.database_path
    tracker = RunTracker(db_path)
    storage = StorageManager(project_root)
    return config, project_root, tracker, storage


def create_pipeline_run(**context):
    config, _, tracker, _ = _get_components()
    dataset_id = config.dataset.default_id
    run_id = tracker.create_pipeline_run("machinaflow_sensor_lakehouse_pipeline", dataset_id)
    tracker.start_task(run_id, "create_pipeline_run")
    tracker.complete_task(run_id, "create_pipeline_run", "SUCCESS")
    context["ti"].xcom_push(key="pipeline_run_id", value=run_id)
    context["ti"].xcom_push(key="dataset_id", value=dataset_id)
    return run_id


def check_source_files(**context):
    config, project_root, tracker, _ = _get_components()
    run_id = context["ti"].xcom_pull(key="pipeline_run_id")
    tracker.start_task(run_id, "check_source_files")

    source_dir = project_root / config.data_paths.source_dir
    dataset_id = config.dataset.default_id
    train_file = source_dir / f"train_{dataset_id}.txt"

    if not train_file.exists():
        tracker.complete_task(run_id, "check_source_files", "FAILED")
        raise FileNotFoundError(f"Source file not found: {train_file}")

    tracker.complete_task(run_id, "check_source_files", "SUCCESS")


def ingest_bronze(**context):
    config, _, tracker, storage = _get_components()
    run_id = context["ti"].xcom_pull(key="pipeline_run_id")
    dataset_id = context["ti"].xcom_pull(key="dataset_id")
    tracker.start_task(run_id, "ingest_bronze")

    ingestor = CMAPSSIngestor(config=config, run_tracker=tracker, storage=storage)
    results = ingestor.ingest_dataset(dataset_id, run_id)
    total = sum(results.values())

    tracker.complete_task(run_id, "ingest_bronze", "SUCCESS", total)
    return total


def validate_bronze(**context):
    config, project_root, tracker, storage = _get_components()
    run_id = context["ti"].xcom_pull(key="pipeline_run_id")
    tracker.start_task(run_id, "validate_bronze")

    bronze_files = storage.list_parquet_files(config.data_paths.bronze_dir)
    runner = QualityRunner(config=config, run_tracker=tracker)

    all_results = []
    for bf in bronze_files:
        if "train" in bf.stem.lower():
            df = pl.read_parquet(bf)
            results = runner.validate_bronze(df, run_id)
            all_results.extend(results)

    passed = sum(1 for r in all_results if r.status == "PASS")
    failed = sum(1 for r in all_results if r.status == "FAIL")
    tracker.complete_task(run_id, "validate_bronze", "SUCCESS")
    return {"passed": passed, "failed": failed}


def transform_silver(**context):
    config, _, tracker, storage = _get_components()
    run_id = context["ti"].xcom_pull(key="pipeline_run_id")
    dataset_id = context["ti"].xcom_pull(key="dataset_id")
    tracker.start_task(run_id, "transform_silver")

    bronze_files = storage.list_parquet_files(config.data_paths.bronze_dir)
    train_files = [f for f in bronze_files if "train" in f.stem.lower()]

    frames = [pl.read_parquet(f) for f in train_files]
    bronze_df = pl.concat(frames) if len(frames) > 1 else frames[0]

    transformer = BronzeToSilverTransformer(config=config, storage=storage)
    silver_df = transformer.transform(bronze_df, dataset_id)

    tracker.complete_task(run_id, "transform_silver", "SUCCESS", len(silver_df))
    return len(silver_df)


def validate_silver(**context):
    config, _, tracker, storage = _get_components()
    run_id = context["ti"].xcom_pull(key="pipeline_run_id")
    tracker.start_task(run_id, "validate_silver")

    silver_files = storage.list_parquet_files(config.data_paths.silver_dir)
    runner = QualityRunner(config=config, run_tracker=tracker)

    for sf in silver_files:
        df = pl.read_parquet(sf)
        runner.validate_silver(df, run_id)

    tracker.complete_task(run_id, "validate_silver", "SUCCESS")


def generate_ml_features(**context):
    config, _, tracker, storage = _get_components()
    run_id = context["ti"].xcom_pull(key="pipeline_run_id")
    tracker.start_task(run_id, "generate_ml_features")

    silver_files = storage.list_parquet_files(config.data_paths.silver_dir)
    frames = [pl.read_parquet(f) for f in silver_files]
    silver_df = pl.concat(frames) if len(frames) > 1 else frames[0]

    engineer = FeatureEngineer(config=config, storage=storage)
    feature_df = engineer.generate_features(silver_df)

    calculator = HealthMetricsCalculator(config=config, storage=storage)
    calculator.run_all(silver_df)

    tracker.complete_task(run_id, "generate_ml_features", "SUCCESS", len(feature_df))
    return len(feature_df)


def validate_gold(**context):
    config, _, tracker, storage = _get_components()
    run_id = context["ti"].xcom_pull(key="pipeline_run_id")
    tracker.start_task(run_id, "validate_gold")

    gold_files = storage.list_parquet_files(config.data_paths.gold_dir)
    runner = QualityRunner(config=config, run_tracker=tracker)

    for gf in gold_files:
        df = pl.read_parquet(gf)
        runner.validate_gold(df, run_id)

    tracker.complete_task(run_id, "validate_gold", "SUCCESS")


def update_pipeline_metrics(**context):
    config, _, tracker, _ = _get_components()
    run_id = context["ti"].xcom_pull(key="pipeline_run_id")
    tracker.start_task(run_id, "update_pipeline_metrics")

    quality_results = tracker.get_quality_results(run_id)
    passed = sum(1 for r in quality_results if r["status"] == "PASS")
    failed = sum(1 for r in quality_results if r["status"] == "FAIL")

    tracker.complete_pipeline_run(
        run_id,
        "SUCCESS",
        quality_checks_passed=passed,
        quality_checks_failed=failed,
    )
    tracker.complete_task(run_id, "update_pipeline_metrics", "SUCCESS")


with DAG(
    dag_id="machinaflow_sensor_lakehouse_pipeline",
    default_args=DEFAULT_ARGS,
    description="Industrial Sensor Lakehouse Pipeline: Bronze → Silver → Gold",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["machinaflow", "sensor", "lakehouse"],
    on_failure_callback=on_failure_callback,
) as dag:

    t_start = PythonOperator(
        task_id="start_pipeline",
        python_callable=lambda: logger.info("Pipeline started"),
    )

    t_create_run = PythonOperator(
        task_id="create_pipeline_run",
        python_callable=create_pipeline_run,
    )

    t_check_sources = PythonOperator(
        task_id="check_source_files",
        python_callable=check_source_files,
    )

    t_ingest = PythonOperator(
        task_id="ingest_bronze",
        python_callable=ingest_bronze,
    )

    t_validate_bronze = PythonOperator(
        task_id="validate_bronze",
        python_callable=validate_bronze,
    )

    t_silver = PythonOperator(
        task_id="transform_silver",
        python_callable=transform_silver,
    )

    t_validate_silver = PythonOperator(
        task_id="validate_silver",
        python_callable=validate_silver,
    )

    t_features = PythonOperator(
        task_id="generate_ml_features",
        python_callable=generate_ml_features,
    )

    t_validate_gold = PythonOperator(
        task_id="validate_gold",
        python_callable=validate_gold,
    )

    t_metrics = PythonOperator(
        task_id="update_pipeline_metrics",
        python_callable=update_pipeline_metrics,
    )

    t_end = PythonOperator(
        task_id="end_pipeline",
        python_callable=lambda: logger.info("Pipeline complete"),
    )

    (
        t_start
        >> t_create_run
        >> t_check_sources
        >> t_ingest
        >> t_validate_bronze
        >> t_silver
        >> t_validate_silver
        >> t_features
        >> t_validate_gold
        >> t_metrics
        >> t_end
    )
