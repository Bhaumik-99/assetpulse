from __future__ import annotations

from pathlib import Path

import polars as pl

from src.validation.schema_checks import QualityResult

DUPLICATE_KEY_COLUMNS = ["unit_id", "cycle", "dataset_id"]


def detect_duplicates(
    df: pl.DataFrame,
    key_columns: list[str] | None = None,
) -> tuple[QualityResult, pl.DataFrame]:
    keys = key_columns or DUPLICATE_KEY_COLUMNS
    available_keys = [k for k in keys if k in df.columns]

    if not available_keys:
        return QualityResult(
            check_name="duplicate_records",
            check_type="duplicate",
            status="WARN",
            details=f"None of the key columns {keys} found in DataFrame",
        ), pl.DataFrame()

    total = len(df)
    duplicated_mask = df.select(available_keys).is_duplicated()
    dup_count = duplicated_mask.sum()

    duplicate_df = df.filter(duplicated_mask)

    failure_pct = (dup_count / total * 100) if total > 0 else 0.0
    status = "PASS" if dup_count == 0 else "FAIL"

    result = QualityResult(
        check_name="duplicate_records",
        check_type="duplicate",
        status=status,
        records_checked=total,
        failed_records=dup_count,
        failure_percentage=round(failure_pct, 4),
        details=f"key_columns={available_keys}",
    )

    return result, duplicate_df


def quarantine_duplicates(
    df: pl.DataFrame,
    quarantine_dir: Path,
    key_columns: list[str] | None = None,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    keys = key_columns or DUPLICATE_KEY_COLUMNS
    available_keys = [k for k in keys if k in df.columns]

    if not available_keys:
        return df, pl.DataFrame()

    duplicated_mask = df.select(available_keys).is_duplicated()
    clean_df = df.filter(~duplicated_mask)
    duplicate_df = df.filter(duplicated_mask)

    if len(duplicate_df) > 0:
        quarantine_dir.mkdir(parents=True, exist_ok=True)
        duplicate_df.write_parquet(quarantine_dir / "duplicate_records.parquet")

    return clean_df, duplicate_df
