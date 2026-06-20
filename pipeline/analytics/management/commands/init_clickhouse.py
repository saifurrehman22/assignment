"""Create the ClickHouse databases, tables and reporting views (idempotent)."""
from django.core.management.base import BaseCommand

from analytics.clickhouse.ddl import init_schema


class Command(BaseCommand):
    help = "Create/refresh all ClickHouse tables and reporting views."

    def handle(self, *args, **opts):
        created = init_schema()
        for line in created:
            self.stdout.write(f"  {line}")
        self.stdout.write(self.style.SUCCESS(f"ClickHouse schema ready ({len(created)} objects)."))
