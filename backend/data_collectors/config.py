"""Настройки оркестратора: каталог выгрузки и таймаут HTTP для источников."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class CollectorSettings:
    http_timeout_sec: float = 30.0
    raw_output_dir: str = "data/raw"
    snapshot_item_limit: int = 100


def load_collector_settings() -> CollectorSettings:
    raw = os.environ.get("RWA_RAW_DATA_DIR")
    return CollectorSettings(raw_output_dir=raw or "data/raw")
