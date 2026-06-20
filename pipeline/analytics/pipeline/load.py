"""
Load + transform: S3 Bronze (Parquet) -> ClickHouse bronze tables -> silver.

* Bronze load: read each Parquet object for the batch and insert into the matching
  ``bronze_*`` ReplacingMergeTree (idempotent on the business id).
* Silver transform: ``INSERT ... SELECT`` the *current batch's* bronze rows into the
  cleaned ``silver_*`` table, splitting out data-quality rejects into ``silver_rejects``.
  Re-running is safe because silver is also a ReplacingMergeTree keyed on the id.
"""
import datetime as dt
import io
from decimal import Decimal

from django.conf import settings

from ..clickhouse.client import get_client
from .aws import s3_client
from .sources import Source
from .validation import SQL_INVALID_REASON, SQL_VALID_PREDICATE

# Column order inserted into each bronze table (must match ddl.py).
BRONZE_COLUMNS = {
    "payments": [
        "payment_attempt_id", "customer_id", "amount", "currency", "status",
        "created_at", "event_date", "batch_id", "ingested_at",
    ],
    "refunds": [
        "refund_id", "payment_id", "amount", "currency",
        "created_at", "event_date", "batch_id", "ingested_at",
    ],
    "subscription_events": [
        "subscription_event_id", "customer_id", "subscription_id", "event_type",
        "plan", "created_at", "event_date", "batch_id", "ingested_at",
    ],
    "fx_rates": ["date", "currency", "rate_to_usd", "batch_id", "ingested_at"],
}

DECIMAL_COLS = {"amount", "rate_to_usd"}
DATETIME_COLS = {"created_at", "ingested_at"}
DATE_COLS = {"event_date", "date"}


def _coerce(col: str, value):
    if value is None:
        return value
    if col in DECIMAL_COLS:
        return Decimal(str(value))
    if col in DATETIME_COLS:
        ts = value
        if hasattr(ts, "to_pydatetime"):
            ts = ts.to_pydatetime()
        if isinstance(ts, dt.datetime) and ts.tzinfo is not None:
            ts = ts.astimezone(dt.timezone.utc).replace(tzinfo=None)
        return ts
    if col in DATE_COLS:
        if hasattr(value, "date") and isinstance(value, dt.datetime):
            return value.date()
        if hasattr(value, "to_pydatetime"):
            return value.to_pydatetime().date()
        return value
    return value


def load_bronze(source: Source, keys: list[str]) -> int:
    """Insert every Parquet object for this batch into the bronze table."""
    import pandas as pd

    if not keys:
        return 0
    bucket = settings.AWS["bronze_bucket"]
    s3 = s3_client()
    client = get_client()
    cols = BRONZE_COLUMNS[source.name]

    total = 0
    for key in keys:
        obj = s3.get_object(Bucket=bucket, Key=key)
        df = pd.read_parquet(io.BytesIO(obj["Body"].read()), engine="pyarrow")
        data = [[_coerce(c, row[c]) for c in cols] for row in df.to_dict("records")]
        if data:
            client.insert(source.bronze_table, data, column_names=cols,
                          database=settings.CLICKHOUSE["database"])
            total += len(data)
    return total


# --- Per-source bronze->silver transforms (run for the current batch only) ---------

def _transform_payments(client, db, batch_id):
    client.command(f"""
        INSERT INTO {db}.silver_payments
        SELECT payment_attempt_id, customer_id, amount, currency, status,
               created_at, event_date, batch_id, ingested_at
        FROM {db}.bronze_payments
        WHERE batch_id = %(b)s AND {SQL_VALID_PREDICATE}
    """, parameters={"b": batch_id})

    client.command(f"""
        INSERT INTO {db}.silver_rejects
        SELECT 'payments' AS source, payment_attempt_id AS record_id,
               {SQL_INVALID_REASON} AS reason,
               currency, amount, created_at, batch_id, ingested_at
        FROM {db}.bronze_payments
        WHERE batch_id = %(b)s AND NOT ({SQL_VALID_PREDICATE})
    """, parameters={"b": batch_id})


def _transform_refunds(client, db, batch_id):
    # Refunds inherit validity from their own amount/currency too.
    client.command(f"""
        INSERT INTO {db}.silver_refunds
        SELECT refund_id, payment_id, amount, currency,
               created_at, event_date, batch_id, ingested_at
        FROM {db}.bronze_refunds
        WHERE batch_id = %(b)s AND {SQL_VALID_PREDICATE}
    """, parameters={"b": batch_id})

    client.command(f"""
        INSERT INTO {db}.silver_rejects
        SELECT 'refunds' AS source, refund_id AS record_id,
               {SQL_INVALID_REASON} AS reason,
               currency, amount, created_at, batch_id, ingested_at
        FROM {db}.bronze_refunds
        WHERE batch_id = %(b)s AND NOT ({SQL_VALID_PREDICATE})
    """, parameters={"b": batch_id})


def _transform_subscription_events(client, db, batch_id):
    client.command(f"""
        INSERT INTO {db}.silver_subscription_events
        SELECT subscription_event_id, customer_id, subscription_id, event_type,
               plan, created_at, event_date, batch_id, ingested_at
        FROM {db}.bronze_subscription_events
        WHERE batch_id = %(b)s
    """, parameters={"b": batch_id})


def _transform_fx_rates(client, db, batch_id):
    client.command(f"""
        INSERT INTO {db}.dim_fx_rates
        SELECT date, currency, rate_to_usd, ingested_at
        FROM {db}.bronze_fx_rates
        WHERE batch_id = %(b)s
    """, parameters={"b": batch_id})


_TRANSFORMS = {
    "payments": _transform_payments,
    "refunds": _transform_refunds,
    "subscription_events": _transform_subscription_events,
    "fx_rates": _transform_fx_rates,
}


def transform_silver(source: Source, batch_id: str):
    client = get_client()
    db = settings.CLICKHOUSE["database"]
    _TRANSFORMS[source.name](client, db, batch_id)
