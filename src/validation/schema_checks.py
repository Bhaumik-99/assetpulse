from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime

import polars as pl

REQUIRED_SENSOR_COLUMNS = [
    "unit_id",
    "cycle",
    "operational_setting_1",
    "operational_setting_2",
    "operational_setting_3",
    *[f"sensor_{i:02d}" for i in range(1, 22)],
]

REQUIRED_METADATA_COLUMNS = [
    "source_file",
    "dataset_id",
    "ingested_at",
    "pipeline_run_id",
    "source_row_number",
]

EXPECTED_TYPES = {
    "unit_id": [pl.Int64, pl.Int32],
    "cycle": [pl.Int64, pl.Int32],
    "operational_setting_1": [pl.Float64, pl.Float32],
    "operational_setting_2": [pl.Float64, pl.Float32],
    "operational_setting_3": [pl.Float64, pl.Float32],
    **{f"sensor_{i:02d}": [pl.Float64, pl.Float32] for i in range(1, 22)},
}


@dataclass
class QualityResult:
    check_name: str
    check_type: str
    status: str
    records_checked: int = 0
    failed_records: int = 0
    failure_percentage: float = 0.0
    checked_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    pipeline_run_id: str = ""
    details: str = ""


def check_required_columns(df: pl.DataFrame, include_metadata: bool = True) -> QualityResult:
    required = list(REQUIRED_SENSOR_COLUMNS)
    if include_metadata:
        required.extend(REQUIRED_METADATA_COLUMNS)

    existing = set(df.columns)
    missing = [col for col in required if col not in existing]

    if missing:
        return QualityResult(
            check_name="required_columns",
            check_type="schema",
            status="FAIL",
            details=f"Missing columns: {missing}",
        )

    return QualityResult(
        check_name="required_columns",
        check_type="schema",
        status="PASS",
        records_checked=len(df),
    )


def check_column_types(df: pl.DataFrame) -> QualityResult:
    type_mismatches: list[str] = []

    for col_name, expected_types in EXPECTED_TYPES.items():
        if col_name not in df.columns:
            continue
        actual_type = df.schema[col_name]
        if actual_type not in expected_types:
            type_mismatches.append(f"{col_name}: expected {expected_types}, got {actual_type}")

    if type_mismatches:
        return QualityResult(
            check_name="column_types",
            check_type="schema",
            status="FAIL",
            details=f"Type mismatches: {type_mismatches}",
        )

    return QualityResult(
        check_name="column_types",
        check_type="schema",
        status="PASS",
        records_checked=len(df),
    )


def check_unexpected_columns(df: pl.DataFrame, include_metadata: bool = True) -> QualityResult:
    expected = set(REQUIRED_SENSOR_COLUMNS)
    if include_metadata:
        expected.update(REQUIRED_METADATA_COLUMNS)

    silver_extras = {
        "record_quality_status",
        "has_cycle_gap",
        "has_sensor_range_violation",
        "has_sensor_spike",
        "silver_processed_at",
    }
    expected.update(silver_extras)

    unexpected = [col for col in df.columns if col not in expected]

    if unexpected:
        return QualityResult(
            check_name="unexpected_columns",
            check_type="schema",
            status="WARN",
            details=f"Unexpected columns: {unexpected}",
        )

    return QualityResult(
        check_name="unexpected_columns",
        check_type="schema",
        status="PASS",
        records_checked=len(df),
    )


def run_schema_checks(df: pl.DataFrame, include_metadata: bool = True) -> list[QualityResult]:
    return [
        check_required_columns(df, include_metadata),
        check_column_types(df),
        check_unexpected_columns(df, include_metadata),
    ]
