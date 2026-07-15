from __future__ import annotations

from pathlib import Path

import polars as pl
import pyarrow.parquet as pq

from src.utils.exceptions import StorageError


class StorageManager:
    def __init__(self, base_path: Path) -> None:
        self._base_path = base_path

    @property
    def base_path(self) -> Path:
        return self._base_path

    def ensure_directory(self, relative_path: str | Path) -> Path:
        full_path = self._base_path / relative_path
        try:
            full_path.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            raise StorageError(f"Failed to create directory {full_path}: {e}") from e
        return full_path

    def write_parquet(
        self,
        df: pl.DataFrame,
        relative_path: str | Path,
        *,
        partition_by: list[str] | None = None,
    ) -> Path:
        full_path = self._base_path / relative_path
        full_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            if partition_by:
                table = df.to_arrow()
                pq.write_to_dataset(
                    table,
                    root_path=str(full_path.parent),
                    partition_cols=partition_by,
                )
            else:
                df.write_parquet(full_path)
        except Exception as e:
            raise StorageError(f"Failed to write parquet to {full_path}: {e}") from e

        return full_path

    def read_parquet(
        self,
        relative_path: str | Path,
        columns: list[str] | None = None,
    ) -> pl.DataFrame:
        full_path = self._base_path / relative_path
        if not full_path.exists():
            raise StorageError(f"Parquet file not found: {full_path}")

        try:
            if columns:
                return pl.read_parquet(full_path, columns=columns)
            return pl.read_parquet(full_path)
        except Exception as e:
            raise StorageError(f"Failed to read parquet from {full_path}: {e}") from e

    def scan_parquet(
        self,
        relative_path: str | Path,
    ) -> pl.LazyFrame:
        full_path = self._base_path / relative_path
        if not full_path.exists():
            raise StorageError(f"Parquet file not found: {full_path}")

        try:
            return pl.scan_parquet(full_path)
        except Exception as e:
            raise StorageError(f"Failed to scan parquet from {full_path}: {e}") from e

    def list_parquet_files(self, relative_path: str | Path) -> list[Path]:
        full_path = self._base_path / relative_path
        if not full_path.exists():
            return []
        return sorted(full_path.rglob("*.parquet"))

    def file_exists(self, relative_path: str | Path) -> bool:
        return (self._base_path / relative_path).exists()

    def get_full_path(self, relative_path: str | Path) -> Path:
        return self._base_path / relative_path
