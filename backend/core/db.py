"""Подключение к PostgreSQL и запись бандлов сборщика через Django ORM."""

from __future__ import annotations

from datetime import datetime
import os

import django
from django.conf import settings
from django.db import transaction

from backend.data_collectors.models import CollectionBundle


def _parse_iso_utc(value: str) -> datetime:
    prepared = value.strip()
    if prepared.endswith("Z"):
        prepared = prepared[:-1] + "+00:00"
    return datetime.fromisoformat(prepared)


def _ensure_django(*, database_url: str | None) -> None:
    """
    Позволяет использовать Django ORM из обычного python-кода (без manage.py).

    Если передан `database_url`, он будет использован как `DATABASE_URL`.
    """
    if database_url:
        os.environ.setdefault("DATABASE_URL", database_url)
    os.environ.setdefault(
        "DJANGO_SETTINGS_MODULE", "backend.rwa_analytics_config.settings"
    )

    if not settings.configured:
        django.setup()


def persist_collection_bundle(database_url: str, bundle: CollectionBundle) -> int:
    _ensure_django(database_url=database_url)

    # Import models only after django.setup(), иначе settings не сконфигурированы.
    from backend.core.models import CollectionRun, SourceSnapshotRow

    with transaction.atomic():
        run = CollectionRun.objects.create(
            collected_at_utc=_parse_iso_utc(bundle.collected_at_utc),
            meta=bundle.meta,
        )

        SourceSnapshotRow.objects.bulk_create(
            [
                SourceSnapshotRow(
                    run=run,
                    source=snap.source,
                    fetched_at_utc=_parse_iso_utc(snap.fetched_at_utc),
                    ok=bool(snap.ok),
                    error=snap.error,
                    data=snap.data,
                )
                for snap in bundle.sources
            ]
        )

    return int(run.id)
