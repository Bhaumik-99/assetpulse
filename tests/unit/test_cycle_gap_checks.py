from __future__ import annotations

import polars as pl

from src.validation.cycle_gap_checks import detect_cycle_gaps, get_units_with_gaps


class TestCycleGapDetection:
    def test_no_gaps(self):
        df = pl.DataFrame(
            {
                "unit_id": [1, 1, 1, 2, 2],
                "cycle": [1, 2, 3, 1, 2],
            }
        )
        result, gap_df = detect_cycle_gaps(df)
        assert result.status == "PASS"
        assert len(gap_df) == 0

    def test_with_gaps(self):
        df = pl.DataFrame(
            {
                "unit_id": [1, 1, 1, 2, 2],
                "cycle": [1, 2, 5, 1, 4],
            }
        )
        result, gap_df = detect_cycle_gaps(df)
        assert result.status == "WARN"
        assert len(gap_df) == 2

    def test_gap_df_has_correct_columns(self):
        df = pl.DataFrame(
            {
                "unit_id": [1, 1, 1],
                "cycle": [1, 2, 10],
            }
        )
        _, gap_df = detect_cycle_gaps(df)
        assert "unit_id" in gap_df.columns
        assert "current_cycle" in gap_df.columns
        assert "previous_cycle" in gap_df.columns
        assert "gap_size" in gap_df.columns

    def test_gap_size_correct(self):
        df = pl.DataFrame(
            {
                "unit_id": [1, 1, 1],
                "cycle": [1, 2, 10],
            }
        )
        _, gap_df = detect_cycle_gaps(df)
        assert gap_df["gap_size"].to_list() == [8]

    def test_get_units_with_gaps(self):
        df = pl.DataFrame(
            {
                "unit_id": [1, 1, 2, 2],
                "cycle": [1, 5, 1, 2],
            }
        )
        _, gap_df = detect_cycle_gaps(df)
        units = get_units_with_gaps(gap_df)
        assert units == [1]
