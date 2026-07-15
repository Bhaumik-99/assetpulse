from __future__ import annotations

import polars as pl
import pytest

from src.transformations.health_metrics import HealthMetricsCalculator
from src.utils.config import load_config, reset_config_cache


@pytest.fixture(autouse=True)
def _clear_config():
    reset_config_cache()
    yield
    reset_config_cache()


class TestRULTarget:
    def test_rul_run_to_failure(self):
        df = pl.DataFrame(
            {
                "unit_id": [1, 1, 1, 1, 1],
                "cycle": [1, 2, 3, 4, 5],
            }
        )
        calc = HealthMetricsCalculator(config=load_config())
        result = calc.calculate_rul(df)
        rul = result["remaining_useful_life"].to_list()
        assert rul == [4, 3, 2, 1, 0]

    def test_rul_multiple_units(self):
        df = pl.DataFrame(
            {
                "unit_id": [1, 1, 1, 2, 2],
                "cycle": [1, 2, 3, 1, 2],
            }
        )
        calc = HealthMetricsCalculator(config=load_config())
        result = calc.calculate_rul(df)

        u1 = result.filter(pl.col("unit_id") == 1)["remaining_useful_life"].to_list()
        u2 = result.filter(pl.col("unit_id") == 2)["remaining_useful_life"].to_list()
        assert u1 == [2, 1, 0]
        assert u2 == [1, 0]

    def test_rul_last_cycle_is_zero(self):
        df = pl.DataFrame(
            {
                "unit_id": [1, 1, 1],
                "cycle": [10, 20, 30],
            }
        )
        calc = HealthMetricsCalculator(config=load_config())
        result = calc.calculate_rul(df)
        last_rul = result.filter(pl.col("cycle") == 30)["remaining_useful_life"].to_list()[0]
        assert last_rul == 0

    def test_rul_first_cycle_is_max_minus_one(self):
        df = pl.DataFrame(
            {
                "unit_id": [1, 1, 1],
                "cycle": [1, 2, 3],
            }
        )
        calc = HealthMetricsCalculator(config=load_config())
        result = calc.calculate_rul(df)
        first_rul = result.filter(pl.col("cycle") == 1)["remaining_useful_life"].to_list()[0]
        assert first_rul == 2
