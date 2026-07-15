from __future__ import annotations

from datetime import UTC, datetime, timedelta

import polars as pl

from src.utils.config import FreshnessConfig
from src.validation.freshness_checks import check_freshness


class TestFreshnessChecks:
    def test_fresh_data(self):
        now = datetime.now(UTC)
        df = pl.DataFrame(
            {
                "ingested_at": [now, now - timedelta(hours=1)],
            }
        )
        result = check_freshness(df, "bronze", FreshnessConfig(bronze_max_age_hours=24))
        assert result.status == "PASS"

    def test_stale_data(self):
        old = datetime.now(UTC) - timedelta(hours=72)
        df = pl.DataFrame(
            {
                "ingested_at": [old],
            }
        )
        result = check_freshness(df, "bronze", FreshnessConfig(bronze_max_age_hours=24))
        assert result.status == "FAIL"

    def test_warning_data(self):
        age = datetime.now(UTC) - timedelta(hours=30)
        df = pl.DataFrame(
            {
                "ingested_at": [age],
            }
        )
        result = check_freshness(df, "bronze", FreshnessConfig(bronze_max_age_hours=24))
        assert result.status == "WARN"

    def test_empty_dataset(self):
        df = pl.DataFrame({"ingested_at": []}).cast({"ingested_at": pl.Datetime})
        result = check_freshness(df, "bronze")
        assert result.status == "FAIL"

    def test_missing_column(self):
        df = pl.DataFrame({"other_col": [1]})
        result = check_freshness(df, "bronze")
        assert result.status == "FAIL"
