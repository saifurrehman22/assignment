"""
ClickHouse schema for the analytics store (medallion architecture).

Layers
------
Bronze  : raw rows loaded verbatim from the S3 Parquet lake. ``ReplacingMergeTree``
          keyed on the business id with ``ingested_at`` as the version column —
          this is what makes **reruns idempotent**: re-loading a payment attempt
          (or the exact-duplicate rows in the feed) collapses to one row on merge,
          and reporting reads with ``FINAL`` so results never depend on whether a
          background merge has happened yet.
Silver  : cleaned + validated. The bronze->silver transform *rejects* rows with a
          non-positive amount or an unsupported currency (routing them to
          ``silver_rejects`` for audit) and keeps the rest. Also ``ReplacingMergeTree``
          keyed on the business id.
Gold    : reporting **views** that compute the business metrics in USD with UTC day
          boundaries. Views (not materialised tables) keep the metrics always
          consistent with silver and re-derivable; they read ``FINAL`` so dedup is
          guaranteed regardless of merge state.

Engine rationale
----------------
* ``ReplacingMergeTree(ingested_at)`` — natural fit for an idempotent upsert keyed
  on an external id; the latest ingest of a given id wins.
* ``FINAL`` in every gold view — forces dedup at query time so metrics are exact
  even immediately after a load, at the cost of a merge-on-read (fine at this scale).
* FX is a ``dim_fx_rates`` dimension joined at the rate of the *event date*; refunds
  join back to their original payment so they convert at the **payment's** rate and
  are attributed to the **payment's day**.
"""
from django.conf import settings

from .client import get_client


def _ddl_statements(db: str) -> list[str]:
    return [
        f"CREATE DATABASE IF NOT EXISTS {db}",

        # ---------------------------------------------------------------- BRONZE
        f"""
        CREATE TABLE IF NOT EXISTS {db}.bronze_payments
        (
            payment_attempt_id String,
            customer_id        String,
            amount             Decimal(14, 2),
            currency           String,
            status             LowCardinality(String),
            created_at         DateTime('UTC'),
            event_date         Date,
            batch_id           String,
            ingested_at        DateTime64(3, 'UTC')
        )
        ENGINE = ReplacingMergeTree(ingested_at)
        PARTITION BY toYYYYMM(event_date)
        ORDER BY payment_attempt_id
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {db}.bronze_refunds
        (
            refund_id   String,
            payment_id  String,
            amount      Decimal(14, 2),
            currency    String,
            created_at  DateTime('UTC'),
            event_date  Date,
            batch_id    String,
            ingested_at DateTime64(3, 'UTC')
        )
        ENGINE = ReplacingMergeTree(ingested_at)
        PARTITION BY toYYYYMM(event_date)
        ORDER BY refund_id
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {db}.bronze_subscription_events
        (
            subscription_event_id String,
            customer_id           String,
            subscription_id       String,
            event_type            LowCardinality(String),
            plan                  LowCardinality(String),
            created_at            DateTime('UTC'),
            event_date            Date,
            batch_id              String,
            ingested_at           DateTime64(3, 'UTC')
        )
        ENGINE = ReplacingMergeTree(ingested_at)
        PARTITION BY toYYYYMM(event_date)
        ORDER BY subscription_event_id
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {db}.bronze_fx_rates
        (
            date        Date,
            currency    String,
            rate_to_usd Decimal(14, 6),
            batch_id    String,
            ingested_at DateTime64(3, 'UTC')
        )
        ENGINE = ReplacingMergeTree(ingested_at)
        ORDER BY (date, currency)
        """,

        # ---------------------------------------------------------------- SILVER
        f"""
        CREATE TABLE IF NOT EXISTS {db}.silver_payments
        (
            payment_attempt_id String,
            customer_id        String,
            amount             Decimal(14, 2),
            currency           LowCardinality(String),
            status             LowCardinality(String),
            created_at         DateTime('UTC'),
            event_date         Date,
            batch_id           String,
            ingested_at        DateTime64(3, 'UTC')
        )
        ENGINE = ReplacingMergeTree(ingested_at)
        PARTITION BY toYYYYMM(event_date)
        ORDER BY payment_attempt_id
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {db}.silver_refunds
        (
            refund_id   String,
            payment_id  String,
            amount      Decimal(14, 2),
            currency    LowCardinality(String),
            created_at  DateTime('UTC'),
            event_date  Date,
            batch_id    String,
            ingested_at DateTime64(3, 'UTC')
        )
        ENGINE = ReplacingMergeTree(ingested_at)
        PARTITION BY toYYYYMM(event_date)
        ORDER BY refund_id
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {db}.silver_subscription_events
        (
            subscription_event_id String,
            customer_id           String,
            subscription_id       String,
            event_type            LowCardinality(String),
            plan                  LowCardinality(String),
            created_at            DateTime('UTC'),
            event_date            Date,
            batch_id              String,
            ingested_at           DateTime64(3, 'UTC')
        )
        ENGINE = ReplacingMergeTree(ingested_at)
        PARTITION BY toYYYYMM(event_date)
        ORDER BY subscription_event_id
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {db}.dim_fx_rates
        (
            date        Date,
            currency    LowCardinality(String),
            rate_to_usd Decimal(14, 6),
            ingested_at DateTime64(3, 'UTC')
        )
        ENGINE = ReplacingMergeTree(ingested_at)
        ORDER BY (date, currency)
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {db}.silver_rejects
        (
            source      LowCardinality(String),
            record_id   String,
            reason      String,
            currency    String,
            amount      Decimal(14, 2),
            created_at  DateTime('UTC'),
            batch_id    String,
            ingested_at DateTime64(3, 'UTC')
        )
        ENGINE = ReplacingMergeTree(ingested_at)
        ORDER BY (source, record_id)
        """,

        # ------------------------------------------------------ GOLD / REPORTING
        # Per-payment USD value, converted at the event-date rate.
        f"""
        CREATE OR REPLACE VIEW {db}.v_payments_usd AS
        SELECT
            p.payment_attempt_id              AS payment_attempt_id,
            p.customer_id                     AS customer_id,
            p.status                          AS status,
            p.currency                        AS currency,
            p.event_date                      AS event_date,
            p.amount                          AS amount,
            p.amount * fx.rate_to_usd         AS usd_amount
        FROM {db}.silver_payments AS p FINAL
        LEFT JOIN {db}.dim_fx_rates AS fx FINAL
            ON fx.currency = p.currency AND fx.date = p.event_date
        """,

        # Per-refund USD value, converted at the ORIGINAL PAYMENT's rate and
        # attributed to the ORIGINAL PAYMENT's day & currency (late-refund rule).
        f"""
        CREATE OR REPLACE VIEW {db}.v_refunds_usd AS
        SELECT
            r.refund_id                       AS refund_id,
            r.payment_id                      AS payment_id,
            p.event_date                      AS attribution_date,
            p.currency                        AS attribution_currency,
            r.amount * fx.rate_to_usd         AS usd_amount
        FROM {db}.silver_refunds AS r FINAL
        INNER JOIN {db}.silver_payments AS p FINAL
            ON p.payment_attempt_id = r.payment_id
        LEFT JOIN {db}.dim_fx_rates AS fx FINAL
            ON fx.currency = p.currency AND fx.date = p.event_date
        """,

        # Daily payment aggregates per (event_date, currency).
        f"""
        CREATE OR REPLACE VIEW {db}.v_daily_payments AS
        SELECT
            event_date,
            currency,
            count()                                   AS attempts,
            countIf(status = 'succeeded')             AS succeeded,
            countIf(status = 'failed')                AS failed,
            countIf(status = 'pending')               AS pending,
            sumIf(usd_amount, status = 'succeeded')   AS gross_usd
        FROM {db}.v_payments_usd
        GROUP BY event_date, currency
        """,

        # Daily refund aggregates per (attribution_date, attribution_currency).
        f"""
        CREATE OR REPLACE VIEW {db}.v_daily_refunds AS
        SELECT
            attribution_date     AS event_date,
            attribution_currency AS currency,
            sum(usd_amount)      AS refunds_usd
        FROM {db}.v_refunds_usd
        GROUP BY event_date, currency
        """,

        # The headline daily revenue view (gross, refunds, net, rates) by currency.
        f"""
        CREATE OR REPLACE VIEW {db}.v_daily_revenue AS
        SELECT
            p.event_date                                          AS date,
            p.currency                                            AS currency,
            p.attempts                                            AS attempts,
            p.succeeded                                           AS succeeded,
            p.failed                                              AS failed,
            p.pending                                             AS pending,
            round(p.gross_usd, 2)                                 AS gross_usd,
            round(ifNull(r.refunds_usd, 0), 2)                    AS refunds_usd,
            round(p.gross_usd - ifNull(r.refunds_usd, 0), 2)      AS net_usd,
            if(p.attempts = 0, 0, p.succeeded / p.attempts)       AS success_rate,
            if(p.gross_usd = 0, 0, ifNull(r.refunds_usd, 0) / p.gross_usd) AS refund_rate
        FROM {db}.v_daily_payments AS p
        LEFT JOIN {db}.v_daily_refunds AS r
            ON r.event_date = p.event_date AND r.currency = p.currency
        """,

        # Per-subscription lifecycle (start / first-cancel / plan).
        f"""
        CREATE OR REPLACE VIEW {db}.v_subscription_lifecycle AS
        SELECT
            subscription_id,
            argMin(plan, created_at) AS plan,
            if(countIf(event_type = 'start')  = 0, toDate('2099-12-31'),
               minIf(event_date, event_type = 'start'))  AS start_date,
            if(countIf(event_type = 'cancel') = 0, toDate('2099-12-31'),
               minIf(event_date, event_type = 'cancel')) AS cancel_date
        FROM {db}.silver_subscription_events FINAL
        GROUP BY subscription_id
        """,

        # Active subscriptions for every day in the data window, by plan.
        # Active on day D  <=>  start_date <= D AND cancel_date > D  (end-of-day, UTC).
        f"""
        CREATE OR REPLACE VIEW {db}.v_active_subscriptions_daily AS
        WITH
            bounds AS (
                SELECT min(event_date) AS mn, max(event_date) AS mx
                FROM {db}.silver_subscription_events FINAL
            ),
            calendar AS (
                SELECT assumeNotNull((SELECT mn FROM bounds)) + number AS d
                FROM numbers(toUInt32(
                    assumeNotNull((SELECT mx FROM bounds))
                    - assumeNotNull((SELECT mn FROM bounds))) + 1)
            )
        SELECT
            c.d                AS date,
            l.plan             AS plan,
            count()            AS active_subscriptions
        FROM calendar AS c
        CROSS JOIN {db}.v_subscription_lifecycle AS l
        WHERE l.start_date <= c.d AND l.cancel_date > c.d
        GROUP BY date, plan
        """,

        # Cohort retention: share of subs started in month X still active at the
        # end of month X+N (by start-cohort month and plan).
        f"""
        CREATE OR REPLACE VIEW {db}.v_cohort_retention AS
        WITH
            cohorts AS (
                SELECT subscription_id, plan, start_date, cancel_date,
                       toStartOfMonth(start_date) AS cohort_month
                FROM {db}.v_subscription_lifecycle
                WHERE start_date != toDate('2099-12-31')
            ),
            month_bounds AS (
                SELECT toStartOfMonth(min(start_date)) AS mn,
                       toStartOfMonth(max(start_date)) AS mx
                FROM cohorts
            ),
            months AS (
                SELECT addMonths(assumeNotNull((SELECT mn FROM month_bounds)), number)
                       AS month_start
                FROM numbers(
                    dateDiff('month',
                             assumeNotNull((SELECT mn FROM month_bounds)),
                             assumeNotNull((SELECT mx FROM month_bounds))) + 1)
            )
        SELECT
            c.cohort_month                                         AS cohort_month,
            c.plan                                                 AS plan,
            m.month_start                                          AS active_month,
            dateDiff('month', c.cohort_month, m.month_start)       AS month_offset,
            count()                                                AS cohort_size_seen,
            countIf(c.start_date <= toLastDayOfMonth(m.month_start)
                    AND c.cancel_date > toLastDayOfMonth(m.month_start)) AS active,
            round(
                countIf(c.start_date <= toLastDayOfMonth(m.month_start)
                        AND c.cancel_date > toLastDayOfMonth(m.month_start))
                / count(), 4)                                      AS retention
        FROM cohorts AS c
        CROSS JOIN months AS m
        WHERE m.month_start >= c.cohort_month
        GROUP BY cohort_month, plan, active_month, month_offset
        ORDER BY cohort_month, plan, month_offset
        """,
    ]


def init_schema() -> list[str]:
    """Create every database object idempotently. Returns the object names touched."""
    db = settings.CLICKHOUSE["database"]
    client = get_client(database="default")
    created = []
    for stmt in _ddl_statements(db):
        client.command(stmt)
        created.append(stmt.strip().split("\n")[0].strip())
    return created
