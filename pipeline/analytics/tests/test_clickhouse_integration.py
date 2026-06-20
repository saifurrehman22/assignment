"""
Full-stack integration test (Postgres -> S3 -> ClickHouse).

Auto-skips unless ClickHouse and LocalStack are reachable, so it runs under
`make test` (everything is up) but is skipped during local sqlite-only unit runs.

Covers:
* end-to-end correctness vs the published sanity values and the Python reference;
* Core Problem #1 (reruns): re-extracting the same rows must NOT double any figure;
* Core Problem #5 (data quality): exactly 60 rejected records, 6000 valid.
"""
import importlib.util
import os
import socket

import pytest
from django.conf import settings
from django.core.management import call_command

from analytics.tests import reference_metrics as ref

DATASET = os.environ.get("DATASET_DIR", "/data/dataset")

# Skip cleanly when the warehouse client libs aren't installed (e.g. a bare local
# venv running only the unit tests).
_libs_present = all(
    importlib.util.find_spec(m) for m in ("clickhouse_connect", "boto3", "pandas")
)


def _reachable(host, port):
    s = socket.socket()
    s.settimeout(1)
    try:
        s.connect((host, int(port)))
        return True
    except Exception:
        return False
    finally:
        s.close()


_ch_up = _reachable(settings.CLICKHOUSE["host"], settings.CLICKHOUSE["port"])
_ls_up = _reachable(
    settings.AWS["endpoint_url"].split("//")[-1].split(":")[0],
    settings.AWS["endpoint_url"].rsplit(":", 1)[-1],
)

pytestmark = [
    pytest.mark.django_db,
    pytest.mark.skipif(
        not (_ch_up and _ls_up and _libs_present and os.path.exists(DATASET)),
        reason="ClickHouse/LocalStack/dataset/libs not available"),
]


def _ch():
    from analytics.clickhouse.client import get_client
    return get_client()


def _scalar(sql):
    return _ch().query(sql).result_rows[0][0]


@pytest.fixture
def fresh_pipeline():
    """Seed Postgres + run one batch into a clean ClickHouse."""
    from analytics.pipeline.runner import reset_checkpoints, run_batch

    call_command("init_clickhouse")
    call_command("bootstrap_s3")
    db = settings.CLICKHOUSE["database"]
    client = _ch()
    for t in ["bronze_payments", "bronze_refunds", "bronze_subscription_events",
              "bronze_fx_rates", "silver_payments", "silver_refunds",
              "silver_subscription_events", "dim_fx_rates", "silver_rejects"]:
        client.command(f"TRUNCATE TABLE IF EXISTS {db}.{t}")
    call_command("seed", dataset_dir=DATASET)
    reset_checkpoints()
    run_batch()
    return db


def test_silver_counts_and_rejects(fresh_pipeline):
    assert _scalar("SELECT count() FROM analytics.silver_payments FINAL") == 6000
    assert _scalar("SELECT countIf(status='succeeded') "
                   "FROM analytics.silver_payments FINAL") == 4295
    assert _scalar("SELECT count() FROM analytics.silver_rejects FINAL") == 60


def test_daily_gross_matches_reference(fresh_pipeline):
    gross = _scalar(
        "SELECT round(sum(gross_usd),2) FROM analytics.v_daily_revenue "
        "WHERE date='2026-01-15'")
    assert float(gross) == 40461.59


def test_active_subscriptions_end_of_month(fresh_pipeline):
    active = _scalar(
        "SELECT sum(active_subscriptions) FROM analytics.v_active_subscriptions_daily "
        "WHERE date='2026-01-31'")
    assert int(active) == 755


def test_clickhouse_matches_python_reference(fresh_pipeline):
    expected = ref.daily_revenue(DATASET)
    rows = _ch().query(
        "SELECT toString(date), sum(gross_usd), sum(refunds_usd), sum(net_usd) "
        "FROM analytics.v_daily_revenue GROUP BY date").result_rows
    got = {r[0]: (round(float(r[1]), 2), round(float(r[2]), 2), round(float(r[3]), 2))
           for r in rows}
    # Compare in integer cents with a 1-cent tolerance: both sides are correct to the
    # cent and differ only by half-cent rounding (Decimal sum vs float round).
    def cents(x):
        return round(x * 100)

    for d, exp in expected.items():
        g, rf, n = got[d]
        assert abs(cents(g) - cents(exp["gross_usd"])) <= 1, f"{d} gross {g} vs {exp['gross_usd']}"
        assert abs(cents(rf) - cents(exp["refunds_usd"])) <= 1, f"{d} refunds {rf} vs {exp['refunds_usd']}"
        assert abs(cents(n) - cents(exp["net_usd"])) <= 1, f"{d} net {n} vs {exp['net_usd']}"


def test_rerun_does_not_double(fresh_pipeline):
    """Re-extract & reload the exact same source rows; metrics must not change."""
    from analytics.pipeline.runner import reset_checkpoints, run_batch

    before = _scalar("SELECT round(sum(gross_usd),2) FROM analytics.v_daily_revenue")
    before_rows = _scalar("SELECT count() FROM analytics.silver_payments FINAL")

    reset_checkpoints()      # simulate the "nightly job runs twice" scenario
    run_batch()

    after = _scalar("SELECT round(sum(gross_usd),2) FROM analytics.v_daily_revenue")
    after_rows = _scalar("SELECT count() FROM analytics.silver_payments FINAL")
    assert before == after
    assert before_rows == after_rows == 6000
