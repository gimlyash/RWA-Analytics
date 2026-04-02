"""Пример источника: DeFiLlama (протоколы + yields). Документация: https://defillama.com/docs/api"""

from __future__ import annotations

import logging
from typing import Any

from backend.data_collectors.http import HttpFetcher
from backend.data_collectors.watchlist import RWA_NAME_KEYWORDS, RWA_PROTOCOL_SLUGS

logger = logging.getLogger(__name__)

DEFILLAMA_PROTOCOLS_URL = "https://api.llama.fi/protocols"
DEFILLAMA_YIELDS_POOLS_URL = "https://yields.llama.fi/pools"


def _coerce_pool_list(payload: object) -> list[dict[str, Any]]:
    """DeFiLlama может отдавать список пулов или объект с ключом ``data`` / ``pools``."""

    if isinstance(payload, list):
        return [p for p in payload if isinstance(p, dict)]
    if isinstance(payload, dict):
        for key in ("data", "pools", "pool"):
            inner = payload.get(key)
            if isinstance(inner, list):
                return [p for p in inner if isinstance(p, dict)]
    return []


def _matches_rwa(text: str | None) -> bool:
    if not text:
        return False
    lower = text.lower()
    return any(k in lower for k in RWA_NAME_KEYWORDS)


def _filter_protocols(protocols: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for p in protocols:
        slug = (p.get("slug") or "").lower()
        name = p.get("name") or ""
        if slug in RWA_PROTOCOL_SLUGS or _matches_rwa(name) or _matches_rwa(slug):
            out.append(p)
    return out


def _filter_pools(pools: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for pool in pools:
        project = pool.get("project") or ""
        symbol = pool.get("symbol") or ""
        chain = pool.get("chain") or ""
        if (
            project.lower() in RWA_PROTOCOL_SLUGS
            or _matches_rwa(project)
            or _matches_rwa(symbol)
            or _matches_rwa(chain)
        ):
            out.append(pool)
    return out


def fetch_defillama_snapshot(http: HttpFetcher) -> dict[str, Any]:
    """
    Загружает списки протоколов и пулов доходности, оставляя записи,
    релевантные RWA по ключевым словам и известным slug.
    """

    protocols_raw = http.get_json(DEFILLAMA_PROTOCOLS_URL)
    pools_payload = http.get_json(DEFILLAMA_YIELDS_POOLS_URL)

    if not isinstance(protocols_raw, list):
        logger.error("Unexpected protocols payload type: %s", type(protocols_raw))
        protocols_raw = []
    pools_list = _coerce_pool_list(pools_payload)
    if isinstance(pools_payload, dict) and not pools_list:
        logger.warning(
            "Could not find pool list in yields payload; keys: %s",
            list(pools_payload.keys())[:20],
        )

    protocols_filtered = _filter_protocols(protocols_raw)
    pools_filtered = _filter_pools(pools_list)

    return {
        "protocols_total": len(protocols_raw),
        "pools_total": len(pools_list),
        "protocols_rwa_matched": len(protocols_filtered),
        "pools_rwa_matched": len(pools_filtered),
        "protocols": protocols_filtered,
        "yield_pools": pools_filtered,
    }
