from __future__ import annotations

import polars as pl

from src.validation.schema_checks import QualityResult

CRITICAL_COLUMNS = ["unit_id", "cycle"]
SENSOR_COLUMNS = [f"sensor_{i:02d}" for i in range(1, 22)]
DEFAULT_COMPLETENESS_THRESHOLD = 0.95


def check_null_column(df: pl.DataFrame, column: str) -> QualityResult:
    if column not in df.columns:
        return QualityResult(
            check_name=f"null_{column}",
            check_type="completeness",
            status="FAIL",
            details=f"Column {column} not found",
        )

    total = len(df)
    null_count = df[column].null_count()
    failure_pct = (null_count / total * 100) if total > 0 else 0.0

    status = "PASS" if null_count == 0 else "FAIL"

    return QualityResult(
        check_name=f"null_{column}",
        check_type="completeness",
        status=status,
        records_checked=total,
        failed_records=null_count,
        failure_percentage=round(failure_pct, 4),
    )


def check_sensor_completeness(
    df: pl.DataFrame,
    threshold: float = DEFAULT_COMPLETENESS_THRESHOLD,
) -> list[QualityResult]:
    results: list[QualityResult] = []
    total = len(df)

    for sensor in SENSOR_COLUMNS:
        if sensor not in df.columns:
            continue

        null_count = df[sensor].null_count()
        completeness = 1.0 - (null_count / total) if total > 0 else 0.0
        failure_pct = (null_count / total * 100) if total > 0 else 0.0

        if completeness < threshold:
            status = "FAIL"
        elif completeness < 1.0:
            status = "WARN"
        else:
            status = "PASS"

        results.append(
            QualityResult(
                check_name=f"completeness_{sensor}",
                check_type="completeness",
                status=status,
                records_checked=total,
                failed_records=null_count,
                failure_percentage=round(failure_pct, 4),
                details=f"completeness={completeness:.4f}, threshold={threshold}",
            )
        )

    return results


def run_completeness_checks(
    df: pl.DataFrame,
    threshold: float = DEFAULT_COMPLETENESS_THRESHOLD,
) -> list[QualityResult]:
    results: list[QualityResult] = []

    for col in CRITICAL_COLUMNS:
        results.append(check_null_column(df, col))

    for col in ["operational_setting_1", "operational_setting_2", "operational_setting_3"]:
        results.append(check_null_column(df, col))

    results.extend(check_sensor_completeness(df, threshold))

    return results
