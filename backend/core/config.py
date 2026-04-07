"""Глобальные настройки приложения (переменные окружения)."""

from __future__ import annotations

import os
from dataclasses import dataclass

@dataclass(frozen=True, slots=True)
class CoreSettings:

    database_url: str | None

def load_core_settings() -> CoreSettings:
    url = os.environ.get("DATABASE_URL")
    return CoreSettings(database_url=url.strip() if url else None)