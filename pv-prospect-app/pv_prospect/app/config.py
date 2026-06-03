"""Configuration for the Prediction API."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict


@dataclass
class AppConfig:
    store_dir: Path

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'AppConfig':
        return cls(store_dir=Path(data['store_dir']))
