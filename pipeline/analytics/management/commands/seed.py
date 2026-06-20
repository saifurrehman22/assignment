"""
Idempotent seed of the CSV dataset into Postgres (the system of record).

Idempotency
-----------
Every row is upserted on its external id via ``bulk_create(..., update_conflicts=True)``
so running ``seed`` any number of times yields exactly the same table contents — never
duplicates. Records are loaded *verbatim* (including the feed's exact-duplicate rows and
invalid amounts/currencies); cleaning happens downstream in ClickHouse so Postgres keeps
an auditable copy of what actually arrived.

``ingested_at`` is stamped with this run's start time so the incremental extractor's
watermark advances on every (re)load.
"""
import csv
import datetime as dt
import os
from decimal import Decimal, InvalidOperation
from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone

from analytics import models


def _parse_dt(value: str) -> dt.datetime:
    # CSV timestamps look like 2026-01-18T11:48:24Z
    return dt.datetime.fromisoformat(value.replace("Z", "+00:00"))


def _dec(value: str):
    try:
        return Decimal(value)
    except (InvalidOperation, TypeError):
        return None


class Command(BaseCommand):
    help = "Idempotently load the CSV dataset into Postgres."

    def add_arguments(self, parser):
        parser.add_argument(
            "--dataset-dir", default=settings.DATASET_DIR,
            help="Directory containing the CSV files.",
        )

    def handle(self, *args, **opts):
        d = Path(opts["dataset_dir"])
        if not d.exists():
            self.stderr.write(self.style.ERROR(f"dataset dir not found: {d}"))
            return
        now = timezone.now()
        self.stdout.write(f"Seeding from {d} (ingested_at={now.isoformat()})")

        loaders = [
            ("customers.csv", self._customers),
            ("payments.csv", self._payments),
            ("refunds.csv", self._refunds),
            ("subscription_events.csv", self._sub_events),
            ("fx_rates.csv", self._fx_rates),
        ]
        for filename, fn in loaders:
            path = d / filename
            with open(path, newline="", encoding="utf-8") as fh:
                rows = list(csv.DictReader(fh))
            count = fn(rows, now)
            self.stdout.write(self.style.SUCCESS(f"  {filename:28s} upserted {count}"))

    # --- per-table upserts -----------------------------------------------------

    @transaction.atomic
    def _customers(self, rows, now):
        objs, seen = [], set()
        for r in rows:
            cid = r["customer_id"]
            if cid in seen:
                continue
            seen.add(cid)
            objs.append(models.Customer(
                customer_id=cid, name=r["name"], email=r["email"],
                country=r["country"], created_at=_parse_dt(r["created_at"]),
                ingested_at=now,
            ))
        models.Customer.objects.bulk_create(
            objs, update_conflicts=True, unique_fields=["customer_id"],
            update_fields=["name", "email", "country", "created_at", "ingested_at"],
        )
        return len(objs)

    @transaction.atomic
    def _payments(self, rows, now):
        objs, seen = [], set()
        for r in rows:
            pid = r["payment_attempt_id"]
            if pid in seen:  # collapse exact-duplicate rows by id within the file
                continue
            seen.add(pid)
            objs.append(models.Payment(
                payment_attempt_id=pid, customer_id=r["customer_id"],
                amount=_dec(r["amount"]) or Decimal("0"), currency=r["currency"],
                status=r["status"], created_at=_parse_dt(r["created_at"]),
                ingested_at=now,
            ))
        models.Payment.objects.bulk_create(
            objs, update_conflicts=True, unique_fields=["payment_attempt_id"],
            update_fields=["customer_id", "amount", "currency", "status",
                           "created_at", "ingested_at"],
        )
        return len(objs)

    @transaction.atomic
    def _refunds(self, rows, now):
        objs, seen = [], set()
        for r in rows:
            rid = r["refund_id"]
            if rid in seen:
                continue
            seen.add(rid)
            objs.append(models.Refund(
                refund_id=rid, payment_id=r["payment_id"],
                amount=_dec(r["amount"]) or Decimal("0"), currency=r["currency"],
                created_at=_parse_dt(r["created_at"]), ingested_at=now,
            ))
        models.Refund.objects.bulk_create(
            objs, update_conflicts=True, unique_fields=["refund_id"],
            update_fields=["payment_id", "amount", "currency", "created_at",
                           "ingested_at"],
        )
        return len(objs)

    @transaction.atomic
    def _sub_events(self, rows, now):
        objs, seen = [], set()
        for r in rows:
            sid = r["subscription_event_id"]
            if sid in seen:
                continue
            seen.add(sid)
            objs.append(models.SubscriptionEvent(
                subscription_event_id=sid, customer_id=r["customer_id"],
                subscription_id=r["subscription_id"], event_type=r["event_type"],
                plan=r["plan"], created_at=_parse_dt(r["created_at"]), ingested_at=now,
            ))
        models.SubscriptionEvent.objects.bulk_create(
            objs, update_conflicts=True, unique_fields=["subscription_event_id"],
            update_fields=["customer_id", "subscription_id", "event_type", "plan",
                           "created_at", "ingested_at"],
        )
        return len(objs)

    @transaction.atomic
    def _fx_rates(self, rows, now):
        objs, seen = [], set()
        for r in rows:
            key = (r["date"], r["currency"])
            if key in seen:
                continue
            seen.add(key)
            objs.append(models.FxRate(
                date=dt.date.fromisoformat(r["date"]), currency=r["currency"],
                rate_to_usd=_dec(r["rate_to_usd"]), ingested_at=now,
            ))
        models.FxRate.objects.bulk_create(
            objs, update_conflicts=True, unique_fields=["date", "currency"],
            update_fields=["rate_to_usd", "ingested_at"],
        )
        return len(objs)
