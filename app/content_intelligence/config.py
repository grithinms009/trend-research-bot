import os
from pathlib import Path
from typing import Any, Dict

import yaml


DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[2] / "app" / "config" / "content_intel.yaml"


def load_config(path: str | os.PathLike[str] | None = None) -> Dict[str, Any]:
    config_path = Path(path) if path else DEFAULT_CONFIG_PATH
    if not config_path.exists():
        raise FileNotFoundError(f"Content intelligence config not found: {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    if not isinstance(data, dict):
        raise ValueError("Content intelligence config must be a mapping")

    return data
