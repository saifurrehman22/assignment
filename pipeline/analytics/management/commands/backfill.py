"""
Backfill command: reset watermarks and reprocess the whole history.

Because every layer is idempotent (ORM upserts in Postgres, ReplacingMergeTree in
ClickHouse), a backfill is safe to run at any time and converges to the same metrics
as the incremental path. Optional ``--from``/``--to`` restrict which event dates are
truncated from silver before reprocessing (handy for correcting a single window).
"""
import datetime as dt

from django.conf import settings
from django.core.management.base import BaseCommand

from analytics.clickhouse.client import get_client
from analytics.clickhouse.ddl import init_schema
from analytics.pipeline.aws import ensure_bucket, ensure_firehose_stream
from analytics.pipeline.runner import reset_checkpoints, run_batch
from analytics.pipeline.sources import SOURCES


class Command(BaseCommand):
    help = "Reset watermarks and reprocess all (or a date range of) source data."

    def add_arguments(self, parser):
        parser.add_argument("--from", dest="date_from", default=None,
                            help="Inclusive start event date YYYY-MM-DD (optional).")
        parser.add_argument("--to", dest="date_to", default=None,
                            help="Inclusive end event date YYYY-MM-DD (optional).")
        parser.add_argument("--truncate", action="store_true",
                            help="Drop existing bronze/silver rows before reprocessing.")

    def handle(self, *args, **opts):
        init_schema()
        ensure_bucket()
        ensure_firehose_stream()

        db = settings.CLICKHOUSE["database"]
        client = get_client()

        if opts["truncate"]:
            self._truncate(client, db, opts["date_from"], opts["date_to"])

        reset_checkpoints()
        self.stdout.write("watermarks reset; reprocessing...")
        run_batch(stdout=self.stdout)
        self.stdout.write(self.style.SUCCESS("backfill complete."))

    def _truncate(self, client, db, date_from, date_to):
        tables = (
            [s.bronze_table for s in SOURCES.values()]
            + [s.silver_table for s in SOURCES.values()]
            + ["silver_rejects"]
        )
        if date_from and date_to:
            for t in tables:
                # fx/dim tables have no event_date partition; guard with a column check
                cols = client.query(
                    f"SELECT name FROM system.columns "
                    f"WHERE database='{db}' AND table='{t}'"
                ).result_rows
                colnames = {c[0] for c in cols}
                datecol = "event_date" if "event_date" in colnames else (
                    "date" if "date" in colnames else None)
                if datecol:
                    client.command(
                        f"ALTER TABLE {db}.{t} DELETE "
                        f"WHERE {datecol} BETWEEN %(f)s AND %(t)s",
                        parameters={"f": date_from, "t": date_to},
                    )
            self.stdout.write(f"  truncated rows in [{date_from}, {date_to}]")
        else:
            for t in set(tables):
                client.command(f"TRUNCATE TABLE IF EXISTS {db}.{t}")
            self.stdout.write("  truncated all bronze/silver tables")
