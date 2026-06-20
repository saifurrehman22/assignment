"""Run one incremental batch: Postgres -> S3 Bronze (Parquet) -> ClickHouse Silver."""
from django.core.management.base import BaseCommand

from analytics.pipeline.runner import run_batch


class Command(BaseCommand):
    help = "Run a single incremental batch over all sources."

    def handle(self, *args, **opts):
        run_batch(stdout=self.stdout)
        self.stdout.write(self.style.SUCCESS("batch complete."))
