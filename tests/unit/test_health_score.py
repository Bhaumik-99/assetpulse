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


@pytest.fixture
def sample_df():
    return pl.DataFrame(
        {
            "unit_id": [1, 1, 1, 2, 2, 2],
            "cycle": [1, 2, 3, 1, 2, 3],
            "dataset_id": ["FD001"] * 6,
            **{f"sensor_{i:02d}": [float(v) for v in range(6)] for i in range(1, 22)},
        }
    )


class TestRULCalculation:
    def test_rul_correct(self):
        df = pl.DataFrame(
            {
                "unit_id": [1, 1, 1, 2, 2],
                "cycle": [1, 2, 3, 1, 2],
            }
        )
        calc = HealthMetricsCalculator(config=load_config())
        result = calc.calculate_rul(df)

        unit1_rul = result.filter(pl.col("unit_id") == 1)["remaining_useful_life"].to_list()
        assert unit1_rul == [2, 1, 0]

        unit2_rul = result.filter(pl.col("unit_id") == 2)["remaining_useful_life"].to_list()
        assert unit2_rul == [1, 0]

    def test_rul_single_cycle(self):
        df = pl.DataFrame(
            {
                "unit_id": [1],
                "cycle": [5],
            }
        )
        calc = HealthMetricsCalculator(config=load_config())
        result = calc.calculate_rul(df)
        assert result["remaining_useful_life"].to_list() == [0]


class TestHealthScore:
    def test_health_score_range(self, sample_df):
        calc = HealthMetricsCalculator(config=load_config())
        result = calc.calculate_health_score(sample_df)
        scores = result["health_score"].to_list()
        assert all(0 <= s <= 100 for s in scores)

    def test_health_score_column_exists(self, sample_df):
        calc = HealthMetricsCalculator(config=load_config())
        result = calc.calculate_health_score(sample_df)
        assert "health_score" in result.columns


class TestRiskLevel:
    def test_risk_level_assignment(self):
        df = pl.DataFrame(
            {
                "health_score": [10.0, 40.0, 60.0, 90.0],
            }
        )
        calc = HealthMetricsCalculator(config=load_config())
        result = calc.assign_risk_level(df)
        levels = result["risk_level"].to_list()
        assert levels == ["CRITICAL", "HIGH", "MEDIUM", "LOW"]

    def test_boundary_values(self):
        config = load_config()
        thresholds = config.health_score.risk_thresholds
        df = pl.DataFrame(
            {
                "health_score": [
                    float(thresholds.critical),
                    float(thresholds.high),
                    float(thresholds.medium),
                    float(thresholds.medium + 1),
                ],
            }
        )
        calc = HealthMetricsCalculator(config=config)
        result = calc.assign_risk_level(df)
        levels = result["risk_level"].to_list()
        assert levels[0] == "CRITICAL"
        assert levels[1] == "HIGH"
        assert levels[2] == "MEDIUM"
        assert levels[3] == "LOW"
