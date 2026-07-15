from __future__ import annotations

import polars as pl

from src.validation.schema_checks import QualityResult


def detect_cycle_gaps(df: pl.DataFrame) -> tuple[QualityResult, pl.DataFrame]:
    if "unit_id" not in df.columns or "cycle" not in df.columns:
        return QualityResult(
            check_name="cycle_gaps",
            check_type="cycle_gap",
            status="FAIL",
            details="Required columns unit_id and cycle not found",
        ), pl.DataFrame()

    sorted_df = df.sort(["unit_id", "cycle"])

    gap_df = (
        sorted_df.select(
            [
                pl.col("unit_id"),
                pl.col("cycle").alias("current_cycle"),
                pl.col("cycle").shift(1).over("unit_id").alias("previous_cycle"),
            ]
        )
        .with_columns(
            [
                (pl.col("current_cycle") - pl.col("previous_cycle")).alias("gap_size"),
            ]
        )
        .filter(pl.col("gap_size").is_not_null() & (pl.col("gap_size") > 1))
    )

    total_transitions = len(sorted_df)
    gap_count = len(gap_df)
    failure_pct = (gap_count / total_transitions * 100) if total_transitions > 0 else 0.0

    status = "PASS" if gap_count == 0 else "WARN"

    result = QualityResult(
        check_name="cycle_gaps",
        check_type="cycle_gap",
        status=status,
        records_checked=total_transitions,
        failed_records=gap_count,
        failure_percentage=round(failure_pct, 4),
        details=f"Found {gap_count} cycle gaps across {df['unit_id'].n_unique()} units",
    )

    return result, gap_df


def get_units_with_gaps(gap_df: pl.DataFrame) -> list[int]:
    if len(gap_df) == 0:
        return []
    return gap_df["unit_id"].unique().sort().to_list()
