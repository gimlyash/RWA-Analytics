from __future__ import annotations

from django.db import models


class CollectionRun(models.Model):
    collected_at_utc = models.DateTimeField()
    meta = models.JSONField(default=dict)

    class Meta:
        db_table = "collection_runs"


class SourceSnapshotRow(models.Model):
    run = models.ForeignKey(
        CollectionRun,
        on_delete=models.CASCADE,
        related_name="snapshots",
        db_index=True,
    )
    source = models.CharField(max_length=255)
    fetched_at_utc = models.DateTimeField()
    ok = models.BooleanField()
    error = models.TextField(null=True, blank=True)
    data = models.JSONField(default=dict)

    class Meta:
        db_table = "source_snapshots"


class Protocol(models.Model):
    """
    Канонический протокол/проект (на старте — нормализуем только DeFiLlama).
    """

    slug = models.SlugField(max_length=255, unique=True)
    name = models.CharField(max_length=255)

    # DeFiLlama identity (если известно)
    defillama_id = models.CharField(max_length=255, null=True, blank=True, db_index=True)

    # Удобно хранить оригинальные поля (category, chains, etc.) без потери.
    meta = models.JSONField(default=dict)

    created_at_utc = models.DateTimeField(auto_now_add=True)
    updated_at_utc = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "protocols"


class ProtocolMetricPoint(models.Model):
    protocol = models.ForeignKey(
        Protocol,
        on_delete=models.CASCADE,
        related_name="metric_points",
        db_index=True,
    )
    ts_utc = models.DateTimeField(db_index=True)

    tvl_usd = models.FloatField(null=True, blank=True)
    extra = models.JSONField(default=dict)

    class Meta:
        db_table = "protocol_metric_points"
        constraints = [
            models.UniqueConstraint(
                fields=["protocol", "ts_utc"], name="uniq_protocol_metric_point_ts"
            )
        ]


class YieldPool(models.Model):
    """
    DeFiLlama yields.llama.fi pool (уникальный pool id).
    """

    pool_id = models.CharField(max_length=255, unique=True)
    project = models.CharField(max_length=255, db_index=True)
    chain = models.CharField(max_length=255, db_index=True)
    symbol = models.CharField(max_length=255, null=True, blank=True)

    protocol = models.ForeignKey(
        Protocol,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="yield_pools",
    )

    meta = models.JSONField(default=dict)

    created_at_utc = models.DateTimeField(auto_now_add=True)
    updated_at_utc = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "yield_pools"


class YieldPoolMetricPoint(models.Model):
    pool = models.ForeignKey(
        YieldPool,
        on_delete=models.CASCADE,
        related_name="metric_points",
        db_index=True,
    )
    ts_utc = models.DateTimeField(db_index=True)

    apy = models.FloatField(null=True, blank=True)
    tvl_usd = models.FloatField(null=True, blank=True)

    extra = models.JSONField(default=dict)

    class Meta:
        db_table = "yield_pool_metric_points"
        constraints = [
            models.UniqueConstraint(
                fields=["pool", "ts_utc"], name="uniq_yield_pool_metric_point_ts"
            )
        ]
