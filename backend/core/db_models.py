"""ORM-модели PostgreSQL для снимков сборщика данных."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import Boolean, DateTime, ForeignKey, String, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class CollectionRun(Base):
    __tablename__ = "collection_runs"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    collected_at_utc: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    meta: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)

    snapshots: Mapped[list["SourceSnapshotRow"]] = relationship(
        back_populates="run",
        cascade="all, delete-orphan",
    )


class SourceSnapshotRow(Base):
    __tablename__ = "source_snapshots"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    collection_run_id: Mapped[int] = mapped_column(
        ForeignKey("collection_runs.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    source: Mapped[str] = mapped_column(String(255), nullable=False)
    fetched_at_utc: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    ok: Mapped[bool] = mapped_column(Boolean, nullable=False)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    data: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)

    run: Mapped["CollectionRun"] = relationship(back_populates="snapshots")
