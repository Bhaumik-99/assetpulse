from __future__ import annotations

import polars as pl

from src.validation.schema_checks import QualityResult

SENSOR_COLUMNS = [f"sensor_{i:02d}" for i in range(1, 22)]


def detect_spikes(
    df: pl.DataFrame,
    window_size: int = 10,
    threshold_sigma: float = 3.0,
    sensors: list[str] | None = None,
) -> tuple[list[QualityResult], pl.DataFrame]:
    target_sensors = sensors or [s for s in SENSOR_COLUMNS if s in df.columns]
    results: list[QualityResult] = []
    total = len(df)

    sorted_df = df.sort(["unit_id", "cycle"])

    spike_columns: list[pl.Expr] = []
    for sensor in target_sensors:
        if sensor not in sorted_df.columns:
            continue

        rolling_mean = pl.col(sensor).rolling_mean(window_size=window_size).over("unit_id")
        rolling_std = pl.col(sensor).rolling_std(window_size=window_size).over("unit_id")

        spike_flag = (
            ((pl.col(sensor) - rolling_mean).abs() > (threshold_sigma * rolling_std))
            .fill_null(False)
            .alias(f"spike_{sensor}")
        )

        spike_columns.append(spike_flag)

    if not spike_columns:
        return results, pl.DataFrame()

    flagged_df = sorted_df.with_columns(spike_columns)

    for sensor in target_sensors:
        spike_col = f"spike_{sensor}"
        if spike_col not in flagged_df.columns:
            continue

        spike_count = flagged_df[spike_col].sum()
        failure_pct = (spike_count / total * 100) if total > 0 else 0.0

        if spike_count == 0:
            status = "PASS"
        elif failure_pct < 2.0:
            status = "WARN"
        else:
            status = "FAIL"

        results.append(
            QualityResult(
                check_name=f"spike_{sensor}",
                check_type="spike",
                status=status,
                records_checked=total,
                failed_records=spike_count,
                failure_percentage=round(failure_pct, 4),
                details=f"window={window_size}, sigma={threshold_sigma}",
            )
        )

    has_any_spike = pl.lit(False)
    for sensor in target_sensors:
        spike_col = f"spike_{sensor}"
        if spike_col in flagged_df.columns:
            has_any_spike = has_any_spike | pl.col(spike_col)

    spike_records = flagged_df.filter(has_any_spike)

    return results, spike_records
