from __future__ import annotations

from django.core.management.base import BaseCommand, CommandError

from backend.core.models import CollectionRun
from backend.core.normalization.defillama import normalize_defillama_run


class Command(BaseCommand):
    help = "Normalize DeFiLlama raw snapshots into canonical tables."

    def add_arguments(self, parser) -> None:
        parser.add_argument(
            "--run-id",
            type=int,
            default=None,
            help="CollectionRun id (default: latest).",
        )

    def handle(self, *args, **options):
        run_id = options.get("run_id")
        if run_id is None:
            latest = CollectionRun.objects.order_by("-id").first()
            if not latest:
                raise CommandError("No CollectionRun rows found.")
            run_id = int(latest.id)

        try:
            result = normalize_defillama_run(run_id=int(run_id))
        except CollectionRun.DoesNotExist as exc:
            raise CommandError(f"CollectionRun id={run_id} not found") from exc

        self.stdout.write(
            self.style.SUCCESS(
                "ok "
                f"run_id={result.run_id} "
                f"protocols_upserted={result.protocols_upserted} "
                f"protocol_points_upserted={result.protocol_points_upserted} "
                f"pools_upserted={result.pools_upserted} "
                f"pool_points_upserted={result.pool_points_upserted}"
            )
        )

