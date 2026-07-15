from __future__ import annotations

import polars as pl

from src.validation.duplicate_checks import detect_duplicates, quarantine_duplicates


def _make_df_with_duplicates() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "unit_id": [1, 1, 2, 2, 3],
            "cycle": [1, 1, 1, 2, 1],
            "dataset_id": ["FD001", "FD001", "FD001", "FD001", "FD001"],
            "sensor_01": [1.0, 1.0, 2.0, 2.5, 3.0],
        }
    )


def _make_df_no_duplicates() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "unit_id": [1, 1, 2, 3],
            "cycle": [1, 2, 1, 1],
            "dataset_id": ["FD001", "FD001", "FD001", "FD001"],
            "sensor_01": [1.0, 1.5, 2.0, 3.0],
        }
    )


class TestDetectDuplicates:
    def test_detects_duplicates(self):
        df = _make_df_with_duplicates()
        result, dup_df = detect_duplicates(df)
        assert result.status == "FAIL"
        assert result.failed_records == 2
        assert len(dup_df) == 2

    def test_no_duplicates(self):
        df = _make_df_no_duplicates()
        result, dup_df = detect_duplicates(df)
        assert result.status == "PASS"
        assert result.failed_records == 0
        assert len(dup_df) == 0


class TestQuarantineDuplicates:
    def test_quarantine_creates_file(self, tmp_path):
        df = _make_df_with_duplicates()
        clean, dups = quarantine_duplicates(df, tmp_path / "quarantine")
        assert len(clean) == 3
        assert len(dups) == 2
        assert (tmp_path / "quarantine" / "duplicate_records.parquet").exists()

    def test_no_quarantine_when_clean(self, tmp_path):
        df = _make_df_no_duplicates()
        clean, dups = quarantine_duplicates(df, tmp_path / "quarantine")
        assert len(clean) == 4
        assert len(dups) == 0
