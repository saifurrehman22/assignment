"""
Incremental extraction: Postgres -> S3 Bronze (Parquet, partitioned by date & batch).

Watermark strategy
------------------
Each source has a ``BatchCheckpoint`` row holding ``last_ingested_at``. A batch reads
only rows with ``ingested_at > last_ingested_at``, writes them to Parquet, and then
advances the watermark to the max ``ingested_at`` it saw.

We watermark on ``ingested_at`` (the row's write time in Postgres) rather than the
business ``created_at`` on purpose: a late-arriving or corrected row carries an *old*
event time but a *fresh* ingest time, so it is still captured by the next batch. Event
time is used only for *partitioning* the lake (and for metric attribution downstream).

Idempotency: re-running a batch re-exports the same rows with the same ids; ClickHouse
ReplacingMergeTree collapses them, so metrics are unchanged.
"""
import datetime as dt
import io
import uuid

from django.utils import timezone

from .aws import ensure_bucket, s3_client
from .sources import SOURCES, Source

EPOCH = dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)


def _rows_since(source: Source, watermark: dt.datetime) -> list[dict]:
    qs = source.model.objects.filter(ingested_at__gt=watermark).order_by("ingested_at")
    out = []
    for obj in qs.iterator():
        row = {c: getattr(obj, c) for c in source.columns}
        row["ingested_at"] = obj.ingested_at
        out.append(row)
    return out


def _to_parquet_bytes(rows: list[dict], source: Source, batch_id: str) -> bytes:
    import pandas as pd

    records = []
    for r in rows:
        rec = dict(r)
        # Normalise types for Parquet/ClickHouse.
        if source.name == "fx_rates":
            rec["date"] = r["date"]
            rec["rate_to_usd"] = float(r["rate_to_usd"])
            rec["event_date"] = r["date"]
        else:
            rec["created_at"] = r[source.created_field]
            rec["event_date"] = r[source.created_field].date()
        if "amount" in rec and rec["amount"] is not None:
            rec["amount"] = float(rec["amount"])
        rec["batch_id"] = batch_id
        rec["ingested_at"] = r["ingested_at"]
        records.append(rec)

    df = pd.DataFrame.from_records(records)
    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", index=False)
    return buf.getvalue()


def extract_source(source: Source, batch_id: str, watermark: dt.datetime) -> dict:
    """
    Export one source's new rows to S3 Bronze, partitioned by event date & batch.

    Returns a manifest dict: {keys: [...], rows: N, new_watermark: dt|None}.
    """
    rows = _rows_since(source, watermark)
    if not rows:
        return {"keys": [], "rows": 0, "new_watermark": None}

    bucket = ensure_bucket()
    s3 = s3_client()

    # Partition rows by event date so each S3 object lives under date=.../batch=...
    by_date: dict[dt.date, list[dict]] = {}
    for r in rows:
        d = source.event_date_expr(r)
        by_date.setdefault(d, []).append(r)

    keys = []
    for event_date, drows in sorted(by_date.items()):
        body = _to_parquet_bytes(drows, source, batch_id)
        key = (
            f"bronze/{source.name}/date={event_date.isoformat()}"
            f"/batch={batch_id}/part-00000.parquet"
        )
        s3.put_object(Bucket=bucket, Key=key, Body=body)
        keys.append(key)

    new_watermark = max(r["ingested_at"] for r in rows)
    return {"keys": keys, "rows": len(rows), "new_watermark": new_watermark}


def new_batch_id() -> str:
    return timezone.now().strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:8]
