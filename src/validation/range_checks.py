from __future__ import annotations

from pathlib import Path

import polars as pl

from src.utils.config import SensorRangeEntry, load_sensor_ranges
from src.validation.schema_checks import QualityResult


def check_sensor_ranges(
    df: pl.DataFrame,
    sensor_ranges: dict[str, SensorRangeEntry] | None = None,
) -> tuple[list[QualityResult], pl.DataFrame]:
    if sensor_ranges is None:
        sensor_ranges = load_sensor_ranges()

    results: list[QualityResult] = []
    violation_frames: list[pl.DataFrame] = []
    total = len(df)

    for sensor_name, range_entry in sensor_ranges.items():
        if sensor_name not in df.columns:
            continue

        violation_mask = (pl.col(sensor_name) < range_entry.min) | (pl.col(sensor_name) > range_entry.max)

        violations = df.filter(violation_mask)
        violation_count = len(violations)
        failure_pct = (violation_count / total * 100) if total > 0 else 0.0

        if violation_count == 0:
            status = "PASS"
        elif failure_pct < 5.0:
            status = "WARN"
        else:
            status = "FAIL"

        results.append(
            QualityResult(
                check_name=f"range_{sensor_name}",
                check_type="range",
                status=status,
                records_checked=total,
                failed_records=violation_count,
                failure_percentage=round(failure_pct, 4),
                details=f"range=[{range_entry.min}, {range_entry.max}]",
            )
        )

        if violation_count > 0:
            select_cols = ["unit_id", "cycle", sensor_name]
            if "dataset_id" in df.columns:
                select_cols.append("dataset_id")
            available = [c for c in select_cols if c in df.columns]
            violation_frames.append(
                violations.select(available).with_columns(pl.lit(sensor_name).alias("violated_sensor"))
            )

    all_violations = pl.concat(violation_frames, how="diagonal") if violation_frames else pl.DataFrame()

    return results, all_violations


def quarantine_range_violations(
    violations: pl.DataFrame,
    quarantine_dir: Path,
) -> None:
    if len(violations) == 0:
        return
    quarantine_dir.mkdir(parents=True, exist_ok=True)
    violations.write_parquet(quarantine_dir / "sensor_range_violations.parquet")
