from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import yaml

DEFAULT_ROOT = Path(__file__).resolve().parents[2] / "app" / "config"


def load_yaml(name: str) -> Dict[str, Any]:
    path = DEFAULT_ROOT / name
    if not path.exists():
        raise FileNotFoundError(f"Config file missing: {path}")
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ValueError(f"{name} must contain a mapping at root level")
    return data


def load_all_configs() -> Dict[str, Dict[str, Any]]:
    return {
        "channels": load_yaml("channels.yaml").get("channels", {}),
        "model_routing": load_yaml("model_routing.yaml"),
        "quota": load_yaml("quota_targets.yaml"),
        "quality": load_yaml("quality_thresholds.yaml"),
    }
