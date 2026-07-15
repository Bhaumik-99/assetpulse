from __future__ import annotations

import json
import logging
import sys
from datetime import UTC, datetime
from pathlib import Path


class StructuredFormatter(logging.Formatter):
    def __init__(self, pipeline_run_id: str = "", task_name: str = "", dataset_id: str = "") -> None:
        super().__init__()
        self.pipeline_run_id = pipeline_run_id
        self.task_name = task_name
        self.dataset_id = dataset_id

    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": datetime.now(UTC).isoformat(),
            "log_level": record.levelname,
            "pipeline_run_id": getattr(record, "pipeline_run_id", self.pipeline_run_id),
            "task_name": getattr(record, "task_name", self.task_name),
            "dataset_id": getattr(record, "dataset_id", self.dataset_id),
            "event": getattr(record, "event", ""),
            "message": record.getMessage(),
        }
        if record.exc_info and record.exc_info[1]:
            log_entry["exception"] = str(record.exc_info[1])
        return json.dumps(log_entry)


def get_logger(
    name: str,
    log_dir: Path | None = None,
    pipeline_run_id: str = "",
    task_name: str = "",
    dataset_id: str = "",
    level: int = logging.INFO,
) -> logging.Logger:
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    logger.setLevel(level)
    logger.propagate = False

    formatter = StructuredFormatter(
        pipeline_run_id=pipeline_run_id,
        task_name=task_name,
        dataset_id=dataset_id,
    )

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    if log_dir:
        log_dir.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_dir / f"machinaflow_{datetime.now(UTC).strftime('%Y%m%d')}.log")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


def update_logger_context(
    logger: logging.Logger,
    pipeline_run_id: str = "",
    task_name: str = "",
    dataset_id: str = "",
) -> None:
    for handler in logger.handlers:
        if isinstance(handler.formatter, StructuredFormatter):
            if pipeline_run_id:
                handler.formatter.pipeline_run_id = pipeline_run_id
            if task_name:
                handler.formatter.task_name = task_name
            if dataset_id:
                handler.formatter.dataset_id = dataset_id
