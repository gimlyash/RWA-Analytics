from __future__ import annotations

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    initial = True

    dependencies: list[tuple[str, str]] = []

    operations = [
        migrations.CreateModel(
            name="CollectionRun",
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
                ("collected_at_utc", models.DateTimeField()),
                ("meta", models.JSONField(default=dict)),
            ],
            options={
                "db_table": "collection_runs",
            },
        ),
        migrations.CreateModel(
            name="SourceSnapshotRow",
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
                ("source", models.CharField(max_length=255)),
                ("fetched_at_utc", models.DateTimeField()),
                ("ok", models.BooleanField()),
                ("error", models.TextField(blank=True, null=True)),
                ("data", models.JSONField(default=dict)),
                (
                    "run",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="snapshots",
                        to="core.collectionrun",
                    ),
                ),
            ],
            options={
                "db_table": "source_snapshots",
            },
        ),
    ]
