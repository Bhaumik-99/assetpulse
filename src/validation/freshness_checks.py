from __future__ import annotations

from datetime import UTC, datetime

import polars as pl

from src.utils.config import FreshnessConfig
from src.validation.schema_checks import QualityResult


def check_freshness(
    df: pl.DataFrame,
    layer: str,
    freshness_config: FreshnessConfig | None = None,
    timestamp_column: str = "ingested_at",
) -> QualityResult:
    if freshness_config is None:
        freshness_config = FreshnessConfig()

    threshold_map = {
        "bronze": freshness_config.bronze_max_age_hours,
        "silver": freshness_config.silver_max_age_hours,
        "gold": freshness_config.gold_max_age_hours,
    }

    max_age_hours = threshold_map.get(layer, 24)

    if timestamp_column not in df.columns:
        return QualityResult(
            check_name=f"freshness_{layer}",
            check_type="freshness",
            status="FAIL",
            details=f"Timestamp column {timestamp_column} not found",
        )

    if len(df) == 0:
        return QualityResult(
            check_name=f"freshness_{layer}",
            check_type="freshness",
            status="FAIL",
            details="Dataset is empty",
        )

    try:
        max_ts = df[timestamp_column].max()
        if max_ts is None:
            return QualityResult(
                check_name=f"freshness_{layer}",
                check_type="freshness",
                status="FAIL",
                details="All timestamp values are null",
            )

        if isinstance(max_ts, datetime):
            if max_ts.tzinfo is None:
                max_ts = max_ts.replace(tzinfo=UTC)
        else:
            return QualityResult(
                check_name=f"freshness_{layer}",
                check_type="freshness",
                status="WARN",
                details=f"Timestamp type not datetime: {type(max_ts)}",
            )

        now = datetime.now(UTC)
        age_hours = (now - max_ts).total_seconds() / 3600

        if age_hours <= max_age_hours:
            status = "PASS"
        elif age_hours <= max_age_hours * 2:
            status = "WARN"
        else:
            status = "FAIL"

        return QualityResult(
            check_name=f"freshness_{layer}",
            check_type="freshness",
            status=status,
            records_checked=len(df),
            details=f"age_hours={age_hours:.2f}, threshold={max_age_hours}h",
        )

    except Exception as e:
        return QualityResult(
            check_name=f"freshness_{layer}",
            check_type="freshness",
            status="FAIL",
            details=f"Error computing freshness: {e}",
        )
