"""Подключение к PostgreSQL и запись бандлов сборщика."""

from __future__ import annotations

from datetime import datetime
from functools import lru_cache

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

from backend.core.db_models import Base, CollectionRun, SourceSnapshotRow
from backend.data_collectors.models import CollectionBundle


def _parse_iso_utc(value: str) -> datetime:
    prepared = value.strip()
    if prepared.endswith("Z"):
        prepared = prepared[:-1] + "+00:00"
    return datetime.fromisoformat(prepared)


@lru_cache(maxsize=8)
def get_engine(database_url: str) -> Engine:
    return create_engine(database_url, pool_pre_ping=True, future=True)


def init_db(database_url: str) -> None:
    engine = get_engine(database_url)
    Base.metadata.create_all(engine)


def persist_collection_bundle(database_url: str, bundle: CollectionBundle) -> int:
    init_db(database_url)
    engine = get_engine(database_url)
    session_local = sessionmaker(engine, expire_on_commit=False, future=True)

    with session_local() as session:
        run = CollectionRun(
            collected_at_utc=_parse_iso_utc(bundle.collected_at_utc),
            meta=bundle.meta,
        )
        session.add(run)
        session.flush()

        for snap in bundle.sources:
            session.add(
                SourceSnapshotRow(
                    collection_run_id=run.id,
                    source=snap.source,
                    fetched_at_utc=_parse_iso_utc(snap.fetched_at_utc),
                    ok=snap.ok,
                    error=snap.error,
                    data=snap.data,
                )
            )

        session.commit()
        return int(run.id)
