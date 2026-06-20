"""
Read-only metric queries against ClickHouse.

Separation of concerns: the API talks **only** to ClickHouse here — never to
Postgres. Filters (date range, currency, plan) are passed as bound parameters.

Note on the ``plan`` filter: payments in the source feed are not linked to a
subscription plan, so ``plan`` filters the *subscription* metrics (active subs,
cohorts). Revenue is filtered by ``currency`` (the original transaction currency).
"""
from django.conf import settings

from ..clickhouse.client import query_rows

DB = settings.CLICKHOUSE["database"]

DEFAULT_START = "2026-01-01"
DEFAULT_END = "2026-01-31"


def _revenue_daily(start, end, currency):
    where = ["date BETWEEN %(start)s AND %(end)s"]
    params = {"start": start, "end": end}
    if currency:
        where.append("currency = %(currency)s")
        params["currency"] = currency
    # Aggregate in a subquery, then derive the rates outside it. (Aliasing a column
    # to sum(<sameName>) and re-referencing it confuses ClickHouse's analyzer into
    # nesting aggregates, so we keep the two steps separate.)
    sql = f"""
        SELECT
            date, attempts, succeeded, failed, pending,
            gross_usd, refunds_usd, net_usd,
            if(attempts = 0, 0, succeeded / attempts)        AS success_rate,
            if(gross_usd = 0, 0, refunds_usd / gross_usd)    AS refund_rate
        FROM (
            SELECT
                toString(date)             AS date,
                sum(attempts)              AS attempts,
                sum(succeeded)             AS succeeded,
                sum(failed)                AS failed,
                sum(pending)               AS pending,
                round(sum(gross_usd), 2)   AS gross_usd,
                round(sum(refunds_usd), 2) AS refunds_usd,
                round(sum(net_usd), 2)     AS net_usd
            FROM {DB}.v_daily_revenue
            WHERE {' AND '.join(where)}
            GROUP BY date
        )
        ORDER BY date
    """
    return {r["date"]: r for r in query_rows(sql, params)}


def _active_subs_daily(start, end, plan):
    where = ["date BETWEEN %(start)s AND %(end)s"]
    params = {"start": start, "end": end}
    if plan:
        where.append("plan = %(plan)s")
        params["plan"] = plan
    sql = f"""
        SELECT toString(date) AS date, sum(active_subscriptions) AS active_subscriptions
        FROM {DB}.v_active_subscriptions_daily
        WHERE {' AND '.join(where)}
        GROUP BY date
        ORDER BY date
    """
    return {r["date"]: r["active_subscriptions"] for r in query_rows(sql, params)}


def daily_metrics(start=None, end=None, currency=None, plan=None):
    start = start or DEFAULT_START
    end = end or DEFAULT_END
    rev = _revenue_daily(start, end, currency)
    subs = _active_subs_daily(start, end, plan)
    dates = sorted(set(rev) | set(subs))
    out = []
    for d in dates:
        row = rev.get(d, {
            "date": d, "attempts": 0, "succeeded": 0, "failed": 0, "pending": 0,
            "gross_usd": 0, "refunds_usd": 0, "net_usd": 0,
            "success_rate": 0, "refund_rate": 0,
        })
        row = dict(row)
        row["active_subscriptions"] = int(subs.get(d, 0))
        out.append(row)
    return out


def summary_metrics(start=None, end=None, currency=None, plan=None):
    start = start or DEFAULT_START
    end = end or DEFAULT_END

    where = ["date BETWEEN %(start)s AND %(end)s"]
    params = {"start": start, "end": end}
    if currency:
        where.append("currency = %(currency)s")
        params["currency"] = currency
    totals_sql = f"""
        SELECT
            gross_usd, refunds_usd, net_usd, attempts, succeeded, failed, pending,
            if(attempts = 0, 0, succeeded / attempts)     AS success_rate,
            if(gross_usd = 0, 0, refunds_usd / gross_usd) AS refund_rate
        FROM (
            SELECT
                round(sum(gross_usd), 2)   AS gross_usd,
                round(sum(refunds_usd), 2) AS refunds_usd,
                round(sum(net_usd), 2)     AS net_usd,
                sum(attempts)              AS attempts,
                sum(succeeded)             AS succeeded,
                sum(failed)                AS failed,
                sum(pending)               AS pending
            FROM {DB}.v_daily_revenue
            WHERE {' AND '.join(where)}
        )
    """
    totals = query_rows(totals_sql, params)[0]

    # Active subscriptions are a point-in-time metric: as of the end of the range.
    subs_where = ["date = %(end)s"]
    subs_params = {"end": end}
    if plan:
        subs_where.append("plan = %(plan)s")
        subs_params["plan"] = plan
    subs_sql = f"""
        SELECT sum(active_subscriptions) AS active_subscriptions
        FROM {DB}.v_active_subscriptions_daily
        WHERE {' AND '.join(subs_where)}
    """
    subs_rows = query_rows(subs_sql, subs_params)
    active = int(subs_rows[0]["active_subscriptions"] or 0) if subs_rows else 0

    rejects = query_rows(
        f"SELECT count() AS n FROM {DB}.silver_rejects FINAL"
    )[0]["n"]

    totals = dict(totals)
    totals.update({
        "start": start, "end": end,
        "active_subscriptions_at_end": active,
        "rejected_records": int(rejects),
    })
    return totals


def cohort_retention(plan=None):
    where = []
    params = {}
    if plan:
        where.append("plan = %(plan)s")
        params["plan"] = plan
    clause = f"WHERE {' AND '.join(where)}" if where else ""
    sql = f"""
        SELECT
            toString(cohort_month) AS cohort_month,
            plan,
            toString(active_month) AS active_month,
            month_offset,
            cohort_size_seen,
            active,
            retention
        FROM {DB}.v_cohort_retention
        {clause}
        ORDER BY cohort_month, plan, month_offset
    """
    return query_rows(sql, params)
