"""
Параметры сборщика (таймауты, каталог выгрузки). Расширяйте при добавлении API-ключей.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class CollectorSettings:
    http_timeout_sec: float = 30.0
    http_max_retries: int = 3
    http_retry_backoff_sec: float = 1.5
    raw_output_dir: str = "data/raw"


def load_collector_settings() -> CollectorSettings:
    raw = os.environ.get("RWA_RAW_DATA_DIR")
    return CollectorSettings(raw_output_dir=raw or "data/raw")
