"""
Pure-Python reference implementation of the metric definitions, computed straight
from the CSV feed. It mirrors the ClickHouse reporting SQL one-for-one and is used
two ways:

1. Unit tests assert the reference against the dataset's published sanity values.
2. The ClickHouse integration test asserts the warehouse output == this reference,
   guaranteeing the SQL and the spec cannot silently diverge.
"""
import csv
import datetime as dt
from collections import defaultdict
from decimal import Decimal
from pathlib import Path

SUPPORTED = {"USD", "EUR", "GBP"}


def _d(s):
    return dt.date.fromisoformat(s[:10])


def load(dataset_dir):
    p = Path(dataset_dir)

    fx = {}
    for r in csv.DictReader(open(p / "fx_rates.csv")):
        fx[(r["date"], r["currency"])] = Decimal(r["rate_to_usd"])

    payments = {}  # dedup by id
    for r in csv.DictReader(open(p / "payments.csv")):
        payments[r["payment_attempt_id"]] = r

    refunds = {}
    for r in csv.DictReader(open(p / "refunds.csv")):
        refunds[r["refund_id"]] = r

    subs = list(csv.DictReader(open(p / "subscription_events.csv")))
    return fx, payments, refunds, subs


def is_valid(amount, currency):
    return Decimal(str(amount)) > 0 and currency in SUPPORTED


def daily_revenue(dataset_dir):
    """Return {date_str: {...}} mirroring v_daily_revenue, summed over currencies."""
    fx, payments, refunds, _ = load(dataset_dir)

    rev = defaultdict(lambda: {"attempts": 0, "succeeded": 0, "failed": 0,
                               "pending": 0, "gross_usd": Decimal(0),
                               "refunds_usd": Decimal(0)})
    valid_pay = {}
    for pid, r in payments.items():
        if not is_valid(r["amount"], r["currency"]):
            continue
        valid_pay[pid] = r
        d = r["created_at"][:10]
        rec = rev[d]
        rec["attempts"] += 1
        rec[r["status"]] = rec.get(r["status"], 0) + 1
        if r["status"] == "succeeded":
            rec["gross_usd"] += Decimal(r["amount"]) * fx[(d, r["currency"])]

    # Refunds attributed to the ORIGINAL payment's day at the payment's rate.
    for r in refunds.values():
        if not is_valid(r["amount"], r["currency"]):
            continue
        p = valid_pay.get(r["payment_id"])
        if not p:
            continue
        pday = p["created_at"][:10]
        rev[pday]["refunds_usd"] += Decimal(r["amount"]) * fx[(pday, p["currency"])]

    out = {}
    for d, rec in rev.items():
        gross = rec["gross_usd"]
        ref = rec["refunds_usd"]
        out[d] = {
            "attempts": rec["attempts"],
            "succeeded": rec["succeeded"],
            "failed": rec["failed"],
            "pending": rec["pending"],
            "gross_usd": round(float(gross), 2),
            "refunds_usd": round(float(ref), 2),
            "net_usd": round(float(gross - ref), 2),
            "success_rate": (rec["succeeded"] / rec["attempts"]) if rec["attempts"] else 0,
            "refund_rate": (float(ref / gross)) if gross else 0,
        }
    return out


def active_subscriptions_asof(dataset_dir, date_str, plan=None):
    _, _, _, subs = load(dataset_dir)
    by_sub = defaultdict(lambda: {"start": [], "cancel": [], "plan": None})
    # plan = earliest event's plan (argMin)
    for r in sorted(subs, key=lambda x: x["created_at"]):
        s = by_sub[r["subscription_id"]]
        if s["plan"] is None:
            s["plan"] = r["plan"]
        if r["event_type"] == "start":
            s["start"].append(_d(r["created_at"]))
        elif r["event_type"] == "cancel":
            s["cancel"].append(_d(r["created_at"]))
    D = _d(date_str)
    n = 0
    for s in by_sub.values():
        if plan and s["plan"] != plan:
            continue
        started = any(sd <= D for sd in s["start"])
        canceled = any(cd <= D for cd in s["cancel"])
        if started and not canceled:
            n += 1
    return n


def reject_count(dataset_dir):
    _, payments, refunds, _ = load(dataset_dir)
    n = 0
    for r in payments.values():
        if not is_valid(r["amount"], r["currency"]):
            n += 1
    for r in refunds.values():
        if not is_valid(r["amount"], r["currency"]):
            n += 1
    return n
