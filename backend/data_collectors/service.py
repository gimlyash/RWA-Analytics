"""
Оркестрация: список источников → ``CollectionBundle``, опционально JSON в ``data/raw``.

Новые источники: добавьте функцию ``(http) -> dict`` и строку в ``runners``.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from pathlib import Path
from typing import Any

from backend.data_collectors.config import CollectorSettings, load_collector_settings
from backend.data_collectors.http import HttpFetcher
from backend.data_collectors.models import CollectionBundle, SourceSnapshot, utc_now_iso
from backend.data_collectors.sources.defillama import fetch_defillama_snapshot

logger = logging.getLogger(__name__)


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent.parent


class DataCollectorService:
    """Один прогон всех зарегистрированных сборщиков; ошибка одного не отменяет остальные."""

    def __init__(self, settings: CollectorSettings | None = None) -> None:
        self.settings = settings or load_collector_settings()

    def collect(self) -> CollectionBundle:
        collected_at = utc_now_iso()
        bundle = CollectionBundle(
            collected_at_utc=collected_at,
            meta={"raw_output_dir": self.settings.raw_output_dir},
        )

        with HttpFetcher(self.settings) as http:
            runners: list[tuple[str, Callable[[], dict[str, Any]]]] = [
                ("defillama", lambda: fetch_defillama_snapshot(http)),
            ]

            for name, fn in runners:
                bundle.sources.append(self._run_source(name, fn))

        return bundle

    def _run_source(self, name: str, fn: Callable[[], dict[str, Any]]) -> SourceSnapshot:
        t = utc_now_iso()
        try:
            data = fn()
            return SourceSnapshot(
                source=name,
                fetched_at_utc=t,
                ok=True,
                error=None,
                data=data if isinstance(data, dict) else {"value": data},
            )
        except Exception as e:
            logger.exception("Source %s failed", name)
            return SourceSnapshot(
                source=name,
                fetched_at_utc=t,
                ok=False,
                error=f"{type(e).__name__}: {e}",
                data={},
            )

    def collect_and_save_json(
        self,
        *,
        output_dir: Path | None = None,
        filename_prefix: str = "snapshot",
    ) -> tuple[CollectionBundle, Path]:
        bundle = self.collect()
        root = _repo_root()
        out_dir = output_dir or (root / self.settings.raw_output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)

        safe_ts = bundle.collected_at_utc.replace(":", "-").replace("+00:00", "Z")
        path = out_dir / f"{filename_prefix}_{safe_ts}.json"
        path.write_text(
            json.dumps(bundle.to_json_dict(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        logger.info("Wrote collection bundle to %s", path)
        return bundle, path


def collect_all(
    *,
    save_json: bool = True,
    settings: CollectorSettings | None = None,
) -> CollectionBundle:
    service = DataCollectorService(settings=settings)
    if save_json:
        bundle, _ = service.collect_and_save_json()
        return bundle
    return service.collect()
