"""
Metric-calculation tests.

Two layers:
* against the real dataset, asserting the published sanity values;
* against tiny synthetic fixtures, asserting the tricky Core Problems in isolation
  (late refunds attributed to the original day; failed payments excluded from
  revenue but counted in the success-rate denominator).
"""
import os

import pytest

from analytics.tests import reference_metrics as ref

DATASET = os.environ.get("DATASET_DIR", "/data/dataset")
need_dataset = pytest.mark.skipif(not os.path.exists(DATASET), reason="dataset not mounted")


# --------------------------- real dataset sanity values ---------------------------

@need_dataset
def test_daily_gross_revenue_2026_01_15():
    daily = ref.daily_revenue(DATASET)
    assert daily["2026-01-15"]["gross_usd"] == 40461.59


@need_dataset
def test_total_succeeded_payments():
    daily = ref.daily_revenue(DATASET)
    assert sum(d["succeeded"] for d in daily.values()) == 4295


@need_dataset
def test_total_valid_attempts():
    daily = ref.daily_revenue(DATASET)
    assert sum(d["attempts"] for d in daily.values()) == 6000


@need_dataset
def test_active_subscriptions_end_of_month():
    assert ref.active_subscriptions_asof(DATASET, "2026-01-31") == 755


# ------------------------------- synthetic fixtures -------------------------------

def _write_fixture(tmp_path):
    (tmp_path / "fx_rates.csv").write_text(
        "date,currency,rate_to_usd\n"
        "2026-01-01,USD,1.0\n2026-01-01,EUR,1.10\n"
        "2026-01-20,USD,1.0\n2026-01-20,EUR,1.20\n"
    )
    # day 1: one succeeded EUR (100 -> 110 USD), one failed (excluded from gross)
    (tmp_path / "payments.csv").write_text(
        "payment_attempt_id,customer_id,amount,currency,status,created_at\n"
        "pay_1,c1,100.00,EUR,succeeded,2026-01-01T10:00:00Z\n"
        "pay_2,c1,50.00,USD,failed,2026-01-01T11:00:00Z\n"
        "pay_3,c2,200.00,USD,succeeded,2026-01-20T09:00:00Z\n"
    )
    # A LATE refund processed on 2026-01-20 but against pay_1 (event day 2026-01-01).
    (tmp_path / "refunds.csv").write_text(
        "refund_id,payment_id,amount,currency,created_at\n"
        "ref_1,pay_1,100.00,EUR,2026-01-20T12:00:00Z\n"
    )
    (tmp_path / "subscription_events.csv").write_text(
        "subscription_event_id,customer_id,subscription_id,event_type,plan,created_at\n"
        "se_1,c1,sub_1,start,pro,2026-01-01T00:00:00Z\n"
        "se_2,c2,sub_2,start,basic,2026-01-02T00:00:00Z\n"
        "se_3,c2,sub_2,cancel,basic,2026-01-02T06:00:00Z\n"  # same-day start+cancel
    )
    return str(tmp_path)


def test_late_refund_attributed_to_original_payment_day(tmp_path):
    ds = _write_fixture(tmp_path)
    daily = ref.daily_revenue(ds)
    # Refund converts at the PAYMENT's rate (EUR @1.10 on Jan 1 -> 110 USD),
    # and lands on the payment's day, not the refund's day.
    assert daily["2026-01-01"]["refunds_usd"] == 110.0
    assert daily["2026-01-01"]["net_usd"] == 0.0  # 110 gross - 110 refund
    # The refund must NOT appear on its own processing day (Jan 20).
    assert daily["2026-01-20"]["refunds_usd"] == 0.0


def test_failed_payment_excluded_from_gross_but_in_denominator(tmp_path):
    ds = _write_fixture(tmp_path)
    daily = ref.daily_revenue(ds)
    day = daily["2026-01-01"]
    assert day["gross_usd"] == 110.0       # failed pay_2 excluded from revenue
    assert day["attempts"] == 2            # but counted as an attempt
    assert day["succeeded"] == 1
    assert day["success_rate"] == 0.5      # 1 / 2


def test_same_day_start_and_cancel_is_not_active(tmp_path):
    ds = _write_fixture(tmp_path)
    # sub_2 starts and cancels on Jan 2 -> not active at end of Jan 2.
    assert ref.active_subscriptions_asof(ds, "2026-01-02") == 1   # only sub_1
    assert ref.active_subscriptions_asof(ds, "2026-01-01") == 1   # sub_1 only
