from __future__ import annotations

import io
import zipfile
from pathlib import Path

import requests

from src.utils.config import get_project_root, load_config


def download_cmapss_dataset(output_dir: Path | None = None) -> None:
    config = load_config()
    project_root = get_project_root()

    if output_dir is None:
        output_dir = project_root / config.data_paths.source_dir

    output_dir.mkdir(parents=True, exist_ok=True)

    url = config.dataset.source_url
    print(f"Downloading C-MAPSS dataset from {url}")

    response = requests.get(url, timeout=120)
    response.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
        for member in zf.namelist():
            filename = Path(member).name
            if filename and (filename.endswith(".txt") or filename.endswith(".csv")):
                target = output_dir / filename
                with zf.open(member) as src, open(target, "wb") as dst:
                    dst.write(src.read())
                print(f"  Extracted: {filename}")

    print(f"Dataset downloaded to {output_dir}")


if __name__ == "__main__":
    download_cmapss_dataset()
