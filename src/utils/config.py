from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, field_validator

from src.utils.exceptions import ConfigurationError


class DataPathsConfig(BaseModel):
    base_dir: str
    source_dir: str
    bronze_dir: str
    silver_dir: str
    gold_dir: str
    quarantine_dir: str
    metadata_dir: str
    logs_dir: str

    def resolve(self, project_root: Path) -> dict[str, Path]:
        return {field_name: project_root / getattr(self, field_name) for field_name in self.model_fields}


class FreshnessConfig(BaseModel):
    bronze_max_age_hours: int = 24
    silver_max_age_hours: int = 24
    gold_max_age_hours: int = 48


class RiskThresholds(BaseModel):
    critical: int = 25
    high: int = 50
    medium: int = 75


class HealthScoreConfig(BaseModel):
    sensor_deviation_weight: float = 0.4
    spike_penalty_weight: float = 0.2
    degradation_penalty_weight: float = 0.3
    quality_penalty_weight: float = 0.1
    risk_thresholds: RiskThresholds = RiskThresholds()


class PipelineConfig(BaseModel):
    retries: int = 2
    retry_delay_seconds: int = 300
    task_timeout_seconds: int = 1800
    schedule_interval: str = "@daily"
    spike_detection_window: int = 10
    spike_threshold_sigma: float = 3.0


class DatasetConfig(BaseModel):
    default_id: str = "FD001"
    supported_ids: list[str] = ["FD001", "FD002", "FD003", "FD004"]
    source_url: str = ""


class DuckDBConfig(BaseModel):
    database_path: str = "data/metadata/machinaflow.duckdb"


class SensorRangeEntry(BaseModel):
    min: float
    max: float
    description: str = ""


class MachinaFlowConfig(BaseModel):
    environment: str = "development"
    data_paths: DataPathsConfig
    dataset: DatasetConfig = DatasetConfig()
    column_names: list[str] = []
    freshness: FreshnessConfig = FreshnessConfig()
    pipeline: PipelineConfig = PipelineConfig()
    health_score: HealthScoreConfig = HealthScoreConfig()
    duckdb: DuckDBConfig = DuckDBConfig()

    @field_validator("column_names")
    @classmethod
    def validate_column_names(cls, v: list[str]) -> list[str]:
        if not v:
            raise ConfigurationError("column_names must not be empty")
        expected_count = 26
        if len(v) != expected_count:
            raise ConfigurationError(f"Expected {expected_count} column names, got {len(v)}")
        return v


_config_cache: MachinaFlowConfig | None = None
_sensor_ranges_cache: dict[str, SensorRangeEntry] | None = None


def _find_project_root() -> Path:
    current = Path(__file__).resolve()
    for parent in [current, *list(current.parents)]:
        if (parent / "pyproject.toml").exists():
            return parent
    raise ConfigurationError("Could not find project root (no pyproject.toml found)")


def get_project_root() -> Path:
    return _find_project_root()


def load_config(config_path: Path | None = None) -> MachinaFlowConfig:
    global _config_cache
    if _config_cache is not None:
        return _config_cache

    if config_path is None:
        env_path = os.environ.get("MACHINAFLOW_CONFIG_PATH")
        config_path = get_project_root() / env_path if env_path else get_project_root() / "config" / "development.yaml"

    if not config_path.exists():
        raise ConfigurationError(f"Configuration file not found: {config_path}")

    with open(config_path) as f:
        raw: dict[str, Any] = yaml.safe_load(f)

    _config_cache = MachinaFlowConfig(**raw)
    return _config_cache


def load_sensor_ranges(config_path: Path | None = None) -> dict[str, SensorRangeEntry]:
    global _sensor_ranges_cache
    if _sensor_ranges_cache is not None:
        return _sensor_ranges_cache

    if config_path is None:
        config_path = get_project_root() / "config" / "sensor_ranges.yaml"

    if not config_path.exists():
        raise ConfigurationError(f"Sensor ranges file not found: {config_path}")

    with open(config_path) as f:
        raw: dict[str, Any] = yaml.safe_load(f)

    _sensor_ranges_cache = {sensor_name: SensorRangeEntry(**sensor_data) for sensor_name, sensor_data in raw.items()}
    return _sensor_ranges_cache


def get_resolved_paths(config: MachinaFlowConfig | None = None) -> dict[str, Path]:
    if config is None:
        config = load_config()
    return config.data_paths.resolve(get_project_root())


def reset_config_cache() -> None:
    global _config_cache, _sensor_ranges_cache
    _config_cache = None
    _sensor_ranges_cache = None
