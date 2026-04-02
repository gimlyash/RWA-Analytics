"""
Снимок сбора: несколько источников в одном JSON для последующей загрузки в БД Django.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(slots=True)
class SourceSnapshot:
    source: str
    fetched_at_utc: str
    ok: bool
    error: str | None = None
    data: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class CollectionBundle:
    collected_at_utc: str
    sources: list[SourceSnapshot] = field(default_factory=list)
    meta: dict[str, Any] = field(default_factory=dict)

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "collected_at_utc": self.collected_at_utc,
            "meta": self.meta,
            "sources": [
                {
                    "source": s.source,
                    "fetched_at_utc": s.fetched_at_utc,
                    "ok": s.ok,
                    "error": s.error,
                    "data": s.data,
                }
                for s in self.sources
            ],
        }
