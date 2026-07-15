from __future__ import annotations

import time
from pathlib import Path

import numpy as np


def generate_sensor_data(n_rows: int = 100_000) -> None:
    rng = np.random.default_rng(42)
    n_units = 100
    cycles_per_unit = n_rows // n_units

    unit_ids = np.repeat(np.arange(1, n_units + 1), cycles_per_unit)
    cycles = np.tile(np.arange(1, cycles_per_unit + 1), n_units)
    sensors = {f"sensor_{i:02d}": rng.normal(0, 1, n_rows) for i in range(1, 22)}

    return unit_ids, cycles, sensors


def benchmark_polars(unit_ids, cycles, sensors, n_rows):
    import polars as pl

    data = {"unit_id": unit_ids, "cycle": cycles, **sensors}
    df = pl.DataFrame(data)

    start = time.perf_counter()
    _ = pl.DataFrame(data)
    read_time = time.perf_counter() - start

    start = time.perf_counter()
    _ = df.group_by("unit_id").agg([
        pl.col("sensor_02").mean().alias("sensor_02_mean"),
        pl.col("sensor_03").std().alias("sensor_03_std"),
    ])
    group_time = time.perf_counter() - start

    start = time.perf_counter()
    _ = df.sort(["unit_id", "cycle"]).with_columns([
        pl.col("sensor_02").rolling_mean(window_size=10).over("unit_id").alias("rolling_mean"),
        pl.col("sensor_02").rolling_std(window_size=10).over("unit_id").alias("rolling_std"),
    ])
    rolling_time = time.perf_counter() - start

    return {
        "framework": "Polars",
        "read_ms": round(read_time * 1000, 2),
        "group_by_ms": round(group_time * 1000, 2),
        "rolling_stats_ms": round(rolling_time * 1000, 2),
    }


def benchmark_pandas(unit_ids, cycles, sensors, n_rows):
    import pandas as pd

    data = {"unit_id": unit_ids, "cycle": cycles, **sensors}

    start = time.perf_counter()
    df = pd.DataFrame(data)
    read_time = time.perf_counter() - start

    start = time.perf_counter()
    _ = df.groupby("unit_id").agg(
        sensor_02_mean=("sensor_02", "mean"),
        sensor_03_std=("sensor_03", "std"),
    )
    group_time = time.perf_counter() - start

    start = time.perf_counter()
    df_sorted = df.sort_values(["unit_id", "cycle"])
    df_sorted["rolling_mean"] = df_sorted.groupby("unit_id")["sensor_02"].transform(
        lambda x: x.rolling(window=10).mean()
    )
    df_sorted["rolling_std"] = df_sorted.groupby("unit_id")["sensor_02"].transform(
        lambda x: x.rolling(window=10).std()
    )
    rolling_time = time.perf_counter() - start

    return {
        "framework": "Pandas",
        "read_ms": round(read_time * 1000, 2),
        "group_by_ms": round(group_time * 1000, 2),
        "rolling_stats_ms": round(rolling_time * 1000, 2),
    }


def main():
    n_rows = 100_000
    print(f"Benchmark: Pandas vs Polars ({n_rows:,} rows, 21 sensors)")
    print("=" * 60)

    unit_ids, cycles, sensors = generate_sensor_data(n_rows)

    polars_results = benchmark_polars(unit_ids, cycles, sensors, n_rows)
    pandas_results = benchmark_pandas(unit_ids, cycles, sensors, n_rows)

    print(f"\n{'Operation':<25} {'Polars (ms)':>15} {'Pandas (ms)':>15} {'Speedup':>10}")
    print("-" * 65)

    for op in ["read_ms", "group_by_ms", "rolling_stats_ms"]:
        polars_val = polars_results[op]
        pandas_val = pandas_results[op]
        speedup = pandas_val / polars_val if polars_val > 0 else float("inf")
        op_name = op.replace("_ms", "").replace("_", " ").title()
        print(f"{op_name:<25} {polars_val:>15.2f} {pandas_val:>15.2f} {speedup:>9.1f}x")

    print("\nNote: Results vary by hardware. Run on your machine for accurate numbers.")


if __name__ == "__main__":
    main()
