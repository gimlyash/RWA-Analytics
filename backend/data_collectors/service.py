from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any
from typing import Callable

from backend.core.db import persist_collection_bundle
from backend.data_collectors.config import CollectorSettings, load_collector_settings
from backend.data_collectors.models import CollectionBundle, SourceSnapshot, utc_now_iso
from backend.data_collectors.sources.defillama import (
    fetch_defillama_snapshot,
    fetch_defillama_yields_snapshot,
)

logger = logging.getLogger(__name__)


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent.parent


def _snapshot_from_fetch_result(result: dict[str, Any]) -> SourceSnapshot:
    return SourceSnapshot(
        source=str(result.get("source", "unknown")),
        fetched_at_utc=utc_now_iso(),
        ok=bool(result.get("ok")),
        error=(
            None
            if result.get("ok")
            else (
                str(result["error"])
                if result.get("error") is not None
                else "unknown_error"
            )
        ),
        data={k: v for k, v in result.items() if k not in ("source", "ok", "error")},
    )


class DataCollectorService:
    def __init__(self, settings: CollectorSettings | None = None) -> None:
        self.settings = settings or load_collector_settings()

    def collect(self) -> CollectionBundle:
        collected_at = utc_now_iso()
        bundle = CollectionBundle(
            collected_at_utc=collected_at,
            meta={
                "raw_output_dir": self.settings.raw_output_dir,
                "http_timeout_sec": self.settings.http_timeout_sec,
                "snapshot_item_limit": self.settings.snapshot_item_limit,
            },
        )

        timeout = int(self.settings.http_timeout_sec)
        lim = self.settings.snapshot_item_limit

        steps: list[tuple[str, Callable[[], dict[str, Any]]]] = [
            ("defillama_protocols", lambda: fetch_defillama_snapshot(timeout_sec=timeout, limit=lim)),
            ("defillama_yields", lambda: fetch_defillama_yields_snapshot(timeout_sec=timeout, limit=lim)),
        ]

        for _label, fn in steps:
            try:
                bundle.sources.append(_snapshot_from_fetch_result(fn()))
            except Exception as e:
                logger.exception("Collector step %s failed", _label)
                bundle.sources.append(
                    SourceSnapshot(
                        source=_label,
                        fetched_at_utc=utc_now_iso(),
                        ok=False,
                        error=f"{type(e).__name__}: {e}",
                        data={},
                    )
                )

        return bundle

    def collect_and_save_json(
        self,
        *,
        output_dir: Path | None = None,
        filename_prefix: str = "snapshot",
        save_database: bool = True,
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
        logger.info("Wrote %s", path)

        if save_database and self.settings.database_url:
            run_id = persist_collection_bundle(self.settings.database_url, bundle)
            logger.info("PostgreSQL collection_runs.id=%s", run_id)

        return bundle, path

    def collect_and_save_database_only(self) -> CollectionBundle:
        bundle = self.collect()
        if self.settings.database_url:
            run_id = persist_collection_bundle(self.settings.database_url, bundle)
            logger.info("PostgreSQL collection_runs.id=%s", run_id)
        return bundle


def collect_all(
    *,
    save_json: bool = True,
    save_database: bool = True,
    settings: CollectorSettings | None = None,
) -> CollectionBundle:
    service = DataCollectorService(settings=settings)
    if save_json:
        bundle, _ = service.collect_and_save_json(save_database=save_database)
        return bundle
    if save_database and service.settings.database_url:
        return service.collect_and_save_database_only()
    return service.collect()
