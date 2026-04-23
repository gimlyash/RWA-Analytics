from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from django.db import transaction

from backend.core.models import (
    CollectionRun,
    Protocol,
    ProtocolMetricPoint,
    SourceSnapshotRow,
    YieldPool,
    YieldPoolMetricPoint,
)


def _safe_str(value: Any) -> str | None:
    if value is None:
        return None
    try:
        s = str(value).strip()
        return s or None
    except Exception:
        return None


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        prepared = value.strip().replace(",", "")
        if not prepared:
            return None
        try:
            return float(prepared)
        except ValueError:
            return None
    return None


def _slugify_defillama(value: Any) -> str | None:
    raw = _safe_str(value)
    if not raw:
        return None
    slug = raw.lower().strip()
    slug = slug.replace("'", "")
    for ch in (" ", "/", "\\", ":", ";", ",", ".", "(", ")", "[", "]", "{", "}", "|"):
        slug = slug.replace(ch, "-")
    while "--" in slug:
        slug = slug.replace("--", "-")
    slug = slug.strip("-")
    return slug or None


@dataclass(frozen=True)
class DefiLlamaNormalizeResult:
    run_id: int
    protocols_upserted: int
    protocol_points_upserted: int
    pools_upserted: int
    pool_points_upserted: int


def normalize_defillama_run(*, run_id: int) -> DefiLlamaNormalizeResult:
    """
    Нормализует данные DeFiLlama из `source_snapshots` в канонические таблицы.

    Источники (как в коде сборщика):
    - source="defillama_protocols": data={"protocols_total":..., "protocols":[...]}
    - source="defillama_yields": data={"pools_total":..., "pools":[...]}

    Записываем метрики на timestamp `CollectionRun.collected_at_utc`.
    """
    run = CollectionRun.objects.get(id=run_id)
    ts = run.collected_at_utc

    protocols_snapshot = (
        SourceSnapshotRow.objects.filter(run_id=run_id, source="defillama_protocols", ok=True)
        .order_by("-id")
        .first()
    )
    yields_snapshot = (
        SourceSnapshotRow.objects.filter(run_id=run_id, source="defillama_yields", ok=True)
        .order_by("-id")
        .first()
    )

    protocols_upserted = 0
    protocol_points_upserted = 0
    pools_upserted = 0
    pool_points_upserted = 0

    with transaction.atomic():
        if protocols_snapshot:
            payload = protocols_snapshot.data
            items = payload.get("protocols")
            if isinstance(items, list):
                for p in items:
                    if not isinstance(p, dict):
                        continue
                    name = _safe_str(p.get("name")) or _safe_str(p.get("slug")) or _safe_str(p.get("id"))
                    slug = _slugify_defillama(p.get("slug")) or _slugify_defillama(name)
                    if not name or not slug:
                        continue

                    obj, created = Protocol.objects.update_or_create(
                        slug=slug,
                        defaults={
                            "name": name,
                            "defillama_id": _safe_str(p.get("id")) or _safe_str(p.get("slug")),
                            "meta": p,
                        },
                    )
                    protocols_upserted += 1 if created else 0

                    tvl = _safe_float(p.get("tvl"))
                    _, created_point = ProtocolMetricPoint.objects.update_or_create(
                        protocol=obj,
                        ts_utc=ts,
                        defaults={
                            "tvl_usd": tvl,
                            "extra": {
                                "source_snapshot_id": int(protocols_snapshot.id),
                                "source": "defillama_protocols",
                            },
                        },
                    )
                    protocol_points_upserted += 1 if created_point else 0

        if yields_snapshot:
            payload = yields_snapshot.data
            items = payload.get("pools")
            if isinstance(items, list):
                for pool in items:
                    if not isinstance(pool, dict):
                        continue

                    pool_id = _safe_str(pool.get("pool")) or _safe_str(pool.get("id"))
                    project = _safe_str(pool.get("project"))
                    chain = _safe_str(pool.get("chain"))
                    if not pool_id or not project or not chain:
                        continue

                    symbol = _safe_str(pool.get("symbol"))

                    protocol_slug = _slugify_defillama(project)
                    protocol_obj = None
                    if protocol_slug:
                        protocol_obj, _ = Protocol.objects.get_or_create(
                            slug=protocol_slug,
                            defaults={"name": project, "meta": {"source": "defillama_yields"}},
                        )

                    obj, created = YieldPool.objects.update_or_create(
                        pool_id=pool_id,
                        defaults={
                            "project": project,
                            "chain": chain,
                            "symbol": symbol,
                            "protocol": protocol_obj,
                            "meta": pool,
                        },
                    )
                    pools_upserted += 1 if created else 0

                    apy = _safe_float(pool.get("apy") or pool.get("apyBase"))
                    tvl_usd = _safe_float(pool.get("tvlUsd"))

                    _, created_point = YieldPoolMetricPoint.objects.update_or_create(
                        pool=obj,
                        ts_utc=ts,
                        defaults={
                            "apy": apy,
                            "tvl_usd": tvl_usd,
                            "extra": {
                                "source_snapshot_id": int(yields_snapshot.id),
                                "source": "defillama_yields",
                            },
                        },
                    )
                    pool_points_upserted += 1 if created_point else 0

    return DefiLlamaNormalizeResult(
        run_id=int(run_id),
        protocols_upserted=int(protocols_upserted),
        protocol_points_upserted=int(protocol_points_upserted),
        pools_upserted=int(pools_upserted),
        pool_points_upserted=int(pool_points_upserted),
    )

