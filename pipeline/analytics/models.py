"""
PostgreSQL models — the operational *system of record*.

Design notes
------------
* Every business table is keyed on its **external id** (the natural key from the
  CSV feed). That is what makes the seed idempotent: re-loading the same row is an
  upsert on the external id, never a duplicate insert.
* Each table carries an ``ingested_at`` column. The incremental batch job
  watermarks on this column (not on the business ``created_at``) so that
  late-arriving or corrected rows — which may have an *old* event time — are still
  picked up the next time they are written. See ``analytics.pipeline.extract``.
* Raw values are stored faithfully (including invalid amounts/currencies and
  duplicates that share an id). Cleaning & rejection happen downstream in
  ClickHouse so the lake keeps an auditable copy of exactly what arrived.
"""
from django.db import models


class IngestTimestamped(models.Model):
    """Mixin adding the monotonic watermark column used by the extractor."""

    ingested_at = models.DateTimeField(db_index=True)

    class Meta:
        abstract = True


class Customer(IngestTimestamped):
    customer_id = models.CharField(primary_key=True, max_length=64)
    name = models.CharField(max_length=255)
    email = models.EmailField(max_length=255)
    country = models.CharField(max_length=8)
    created_at = models.DateTimeField()

    class Meta:
        db_table = "src_customers"


class Payment(IngestTimestamped):
    """One payment *attempt*. A retry is a separate attempt with its own id."""

    payment_attempt_id = models.CharField(primary_key=True, max_length=64)
    customer_id = models.CharField(max_length=64, db_index=True)
    amount = models.DecimalField(max_digits=14, decimal_places=2)
    currency = models.CharField(max_length=8)
    status = models.CharField(max_length=16)  # succeeded | failed | pending
    created_at = models.DateTimeField(db_index=True)

    class Meta:
        db_table = "src_payments"


class Refund(IngestTimestamped):
    """A refund against a payment attempt. ``created_at`` can be much later."""

    refund_id = models.CharField(primary_key=True, max_length=64)
    payment_id = models.CharField(max_length=64, db_index=True)
    amount = models.DecimalField(max_digits=14, decimal_places=2)
    currency = models.CharField(max_length=8)
    created_at = models.DateTimeField(db_index=True)

    class Meta:
        db_table = "src_refunds"


class SubscriptionEvent(IngestTimestamped):
    subscription_event_id = models.CharField(primary_key=True, max_length=64)
    customer_id = models.CharField(max_length=64, db_index=True)
    subscription_id = models.CharField(max_length=64, db_index=True)
    event_type = models.CharField(max_length=16)  # start | renew | cancel
    plan = models.CharField(max_length=32)
    created_at = models.DateTimeField(db_index=True)

    class Meta:
        db_table = "src_subscription_events"


class FxRate(IngestTimestamped):
    """Daily rate to convert ``currency`` -> USD on a given UTC ``date``."""

    date = models.DateField()
    currency = models.CharField(max_length=8)
    rate_to_usd = models.DecimalField(max_digits=14, decimal_places=6)

    class Meta:
        db_table = "src_fx_rates"
        constraints = [
            models.UniqueConstraint(
                fields=["date", "currency"], name="uniq_fx_date_currency"
            )
        ]


class BatchCheckpoint(models.Model):
    """
    Watermark store for the incremental extractor. One row per source table.

    ``last_ingested_at`` is the high-water mark: the next batch only reads rows
    with ``ingested_at > last_ingested_at``. A backfill resets these rows.
    """

    source = models.CharField(primary_key=True, max_length=64)
    last_ingested_at = models.DateTimeField(null=True, blank=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "batch_checkpoint"


class BatchRun(models.Model):
    """Audit log of every batch execution (for observability / debugging)."""

    batch_id = models.CharField(max_length=64, db_index=True)
    source = models.CharField(max_length=64)
    rows_exported = models.IntegerField(default=0)
    s3_objects = models.IntegerField(default=0)
    rows_loaded = models.IntegerField(default=0)
    started_at = models.DateTimeField(auto_now_add=True)
    finished_at = models.DateTimeField(null=True, blank=True)
    status = models.CharField(max_length=16, default="running")  # running|ok|error
    detail = models.TextField(blank=True, default="")

    class Meta:
        db_table = "batch_run"
