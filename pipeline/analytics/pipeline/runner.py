"""Orchestrates one incremental batch across all sources (and the backfill reset)."""
import datetime as dt

from django.utils import timezone

from ..models import BatchCheckpoint, BatchRun
from . import extract, load
from .sources import SOURCE_ORDER, SOURCES

EPOCH = dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)


def _watermark(source_name: str) -> dt.datetime:
    cp, _ = BatchCheckpoint.objects.get_or_create(source=source_name)
    return cp.last_ingested_at or EPOCH


def reset_checkpoints(sources: list[str] | None = None):
    """Backfill helper: clear watermarks so the next batch reprocesses everything."""
    names = sources or list(SOURCES.keys())
    for name in names:
        BatchCheckpoint.objects.update_or_create(
            source=name, defaults={"last_ingested_at": None}
        )


def run_batch(stdout=None) -> dict:
    """
    Run one incremental batch over every source in dependency order.

    Returns a per-source summary dict. Safe to run repeatedly (idempotent).
    """
    def log(msg):
        if stdout:
            stdout.write(msg)

    batch_id = extract.new_batch_id()
    log(f"== batch {batch_id} ==")
    summary = {}

    for name in SOURCE_ORDER:
        source = SOURCES[name]
        run = BatchRun.objects.create(batch_id=batch_id, source=name, status="running")
        try:
            watermark = _watermark(name)
            manifest = extract.extract_source(source, batch_id, watermark)
            rows = manifest["rows"]

            loaded = 0
            if rows:
                loaded = load.load_bronze(source, manifest["keys"])
                load.transform_silver(source, batch_id)
                # Advance the watermark only after a successful load+transform.
                BatchCheckpoint.objects.update_or_create(
                    source=name,
                    defaults={"last_ingested_at": manifest["new_watermark"]},
                )

            run.rows_exported = rows
            run.s3_objects = len(manifest["keys"])
            run.rows_loaded = loaded
            run.status = "ok"
            run.finished_at = timezone.now()
            run.save()

            summary[name] = {"exported": rows, "objects": len(manifest["keys"]),
                             "loaded": loaded}
            log(f"  {name:22s} exported={rows:5d} objects={len(manifest['keys']):3d} "
                f"loaded={loaded:5d}")
        except Exception as exc:  # noqa: BLE001 - record & re-raise for visibility
            run.status = "error"
            run.detail = str(exc)
            run.finished_at = timezone.now()
            run.save()
            log(f"  {name:22s} ERROR: {exc}")
            raise

    return summary
