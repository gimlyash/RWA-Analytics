"""Настройки оркестратора: каталог выгрузки и таймаут HTTP для источников."""

from __future__ import annotations

import os
from dataclasses import dataclass

from backend.core.config import load_core_settings


@dataclass(frozen=True, slots=True)
class CollectorSettings:
    http_timeout_sec: float = 30.0
    raw_output_dir: str = "data/raw"
    # 0 means "no limit" (collect all items).
    snapshot_item_limit: int = 0
    database_url: str | None = None


def load_collector_settings() -> CollectorSettings:
    raw = os.environ.get("RWA_RAW_DATA_DIR")
    lim_raw = os.environ.get("RWA_SNAPSHOT_ITEM_LIMIT")
    core = load_core_settings()
    return CollectorSettings(
        raw_output_dir=raw or "data/raw",
        snapshot_item_limit=int(lim_raw) if lim_raw and lim_raw.strip() else 0,
        database_url=core.database_url,
    )
