from __future__ import annotations

import polars as pl

from src.validation.schema_checks import (
    check_column_types,
    check_required_columns,
    check_unexpected_columns,
    run_schema_checks,
)


def _make_valid_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "unit_id": [1, 2],
            "cycle": [1, 1],
            "operational_setting_1": [0.0, 0.0],
            "operational_setting_2": [0.0, 0.0],
            "operational_setting_3": [100.0, 100.0],
            **{f"sensor_{i:02d}": [1.0, 2.0] for i in range(1, 22)},
            "source_file": ["a.txt", "a.txt"],
            "dataset_id": ["FD001", "FD001"],
            "ingested_at": ["2024-01-01", "2024-01-01"],
            "pipeline_run_id": ["abc", "abc"],
            "source_row_number": [1, 2],
        }
    )


class TestRequiredColumns:
    def test_pass_all_present(self):
        df = _make_valid_df()
        result = check_required_columns(df)
        assert result.status == "PASS"

    def test_fail_missing_column(self):
        df = _make_valid_df().drop("sensor_01")
        result = check_required_columns(df)
        assert result.status == "FAIL"
        assert "sensor_01" in result.details

    def test_pass_without_metadata(self):
        df = _make_valid_df().drop(["source_file", "dataset_id", "ingested_at", "pipeline_run_id", "source_row_number"])
        result = check_required_columns(df, include_metadata=False)
        assert result.status == "PASS"


class TestColumnTypes:
    def test_pass_correct_types(self):
        df = _make_valid_df()
        df = df.with_columns(
            [
                pl.col("unit_id").cast(pl.Int64),
                pl.col("cycle").cast(pl.Int64),
            ]
        )
        result = check_column_types(df)
        assert result.status == "PASS"

    def test_fail_wrong_type(self):
        df = _make_valid_df().with_columns(pl.col("unit_id").cast(pl.Utf8))
        result = check_column_types(df)
        assert result.status == "FAIL"


class TestUnexpectedColumns:
    def test_pass_no_extras(self):
        df = _make_valid_df()
        result = check_unexpected_columns(df)
        assert result.status == "PASS"

    def test_warn_extra_columns(self):
        df = _make_valid_df().with_columns(pl.lit("x").alias("random_col"))
        result = check_unexpected_columns(df)
        assert result.status == "WARN"


class TestRunSchemaChecks:
    def test_returns_multiple_results(self):
        df = _make_valid_df()
        results = run_schema_checks(df)
        assert len(results) == 3
