from __future__ import annotations

import polars as pl
import pytest

from src.utils.config import SensorRangeEntry
from src.validation.range_checks import check_sensor_ranges, quarantine_range_violations


@pytest.fixture
def sample_ranges():
    return {
        "sensor_02": SensorRangeEntry(min=630.0, max=650.0),
        "sensor_03": SensorRangeEntry(min=1560.0, max=1620.0),
    }


class TestSensorRangeChecks:
    def test_all_in_range(self, sample_ranges):
        df = pl.DataFrame(
            {
                "unit_id": [1, 2],
                "cycle": [1, 1],
                "sensor_02": [640.0, 645.0],
                "sensor_03": [1580.0, 1600.0],
            }
        )
        results, violations = check_sensor_ranges(df, sample_ranges)
        assert all(r.status == "PASS" for r in results)
        assert len(violations) == 0

    def test_out_of_range(self, sample_ranges):
        df = pl.DataFrame(
            {
                "unit_id": [1, 2, 3],
                "cycle": [1, 1, 1],
                "sensor_02": [640.0, 700.0, 645.0],
                "sensor_03": [1580.0, 1600.0, 1600.0],
            }
        )
        results, violations = check_sensor_ranges(df, sample_ranges)
        sensor_02_result = next(r for r in results if r.check_name == "range_sensor_02")
        assert sensor_02_result.failed_records == 1
        assert len(violations) > 0

    def test_quarantine_writes_file(self, tmp_path, sample_ranges):
        df = pl.DataFrame(
            {
                "unit_id": [1],
                "cycle": [1],
                "sensor_02": [999.0],
                "sensor_03": [1580.0],
            }
        )
        _, violations = check_sensor_ranges(df, sample_ranges)
        quarantine_range_violations(violations, tmp_path / "quarantine")
        assert (tmp_path / "quarantine" / "sensor_range_violations.parquet").exists()
