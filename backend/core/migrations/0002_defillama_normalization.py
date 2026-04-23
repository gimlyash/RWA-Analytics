from __future__ import annotations

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    dependencies = [
        ("core", "0001_initial"),
    ]

    operations = [
        migrations.CreateModel(
            name="Protocol",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("slug", models.SlugField(max_length=255, unique=True)),
                ("name", models.CharField(max_length=255)),
                ("defillama_id", models.CharField(blank=True, db_index=True, max_length=255, null=True)),
                ("meta", models.JSONField(default=dict)),
                ("created_at_utc", models.DateTimeField(auto_now_add=True)),
                ("updated_at_utc", models.DateTimeField(auto_now=True)),
            ],
            options={
                "db_table": "protocols",
            },
        ),
        migrations.CreateModel(
            name="YieldPool",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("pool_id", models.CharField(max_length=255, unique=True)),
                ("project", models.CharField(db_index=True, max_length=255)),
                ("chain", models.CharField(db_index=True, max_length=255)),
                ("symbol", models.CharField(blank=True, max_length=255, null=True)),
                ("meta", models.JSONField(default=dict)),
                ("created_at_utc", models.DateTimeField(auto_now_add=True)),
                ("updated_at_utc", models.DateTimeField(auto_now=True)),
                (
                    "protocol",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="yield_pools",
                        to="core.protocol",
                    ),
                ),
            ],
            options={
                "db_table": "yield_pools",
            },
        ),
        migrations.CreateModel(
            name="ProtocolMetricPoint",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("ts_utc", models.DateTimeField(db_index=True)),
                ("tvl_usd", models.FloatField(blank=True, null=True)),
                ("extra", models.JSONField(default=dict)),
                (
                    "protocol",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="metric_points",
                        to="core.protocol",
                    ),
                ),
            ],
            options={
                "db_table": "protocol_metric_points",
            },
        ),
        migrations.CreateModel(
            name="YieldPoolMetricPoint",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("ts_utc", models.DateTimeField(db_index=True)),
                ("apy", models.FloatField(blank=True, null=True)),
                ("tvl_usd", models.FloatField(blank=True, null=True)),
                ("extra", models.JSONField(default=dict)),
                (
                    "pool",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="metric_points",
                        to="core.yieldpool",
                    ),
                ),
            ],
            options={
                "db_table": "yield_pool_metric_points",
            },
        ),
        migrations.AddConstraint(
            model_name="protocolmetricpoint",
            constraint=models.UniqueConstraint(
                fields=("protocol", "ts_utc"), name="uniq_protocol_metric_point_ts"
            ),
        ),
        migrations.AddConstraint(
            model_name="yieldpoolmetricpoint",
            constraint=models.UniqueConstraint(
                fields=("pool", "ts_utc"), name="uniq_yield_pool_metric_point_ts"
            ),
        ),
    ]

