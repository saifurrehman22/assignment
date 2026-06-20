# Batch Analytics Pipeline

A batch pipeline that produces **trustworthy daily revenue, refund and subscription
metrics** for a subscriptions business — and stays correct when the source feed is
messy (duplicates, bad records, multiple currencies, late refunds) or when jobs are
re-run.

```
CSV feed ─▶ Django seed ─▶ PostgreSQL ─▶ incremental batch ─▶ S3 Bronze (Parquet)
                          (system of record)  (watermark)        │
                                                                  ▼
                                              ClickHouse Bronze ─▶ Silver (clean) ─▶ Gold views
                                                                                       │
                                                            DRF API + dashboard ◀──────┘
```

All reporting reads **only from ClickHouse**. Money is reported in **USD**, days use
**UTC** boundaries, transactions convert at the FX rate of their **event date**, and
refunds convert at — and are attributed to — their **original payment's** day & rate.

---

## 1. Quick start

Prerequisites: Docker + Docker Compose. (A `make` is handy but every command has a
raw `docker compose` equivalent below.)

```bash
make up            # build images, start postgres + clickhouse + localstack + web
make pipeline      # seed Postgres, then run one incremental batch end-to-end
make dashboard     # prints the dashboard + API URLs
make test          # run the test suite (idempotency, reruns, metric calculations)
```

Then open:

* **Dashboard:**  http://localhost:8000/
* **Daily API:**  http://localhost:8000/api/metrics/daily?start=2026-01-01&end=2026-01-31
* **Summary API:** http://localhost:8000/api/metrics/summary

`make pipeline` is just `make seed` + `make run-batch`. Individual steps:

```bash
make seed          # idempotent CSV -> Postgres load
make run-batch     # Postgres -> S3 Bronze (Parquet) -> ClickHouse Silver (+ views)
make backfill      # reset watermarks and reprocess everything (idempotent)
make backfill ARGS="--truncate --from 2026-01-10 --to 2026-01-12"   # window backfill
```

### Without `make`

```bash
cp .env.example .env
docker compose up -d --build
docker compose exec web python manage.py migrate
docker compose exec web python manage.py seed
docker compose exec web python manage.py run_batch     # runs init_clickhouse + bootstrap_s3 internally via make; standalone needs them first:
docker compose exec web python manage.py init_clickhouse
docker compose exec web python manage.py bootstrap_s3
docker compose exec web python manage.py run_batch
docker compose run --rm web pytest -q
```

> The `web` container auto-runs `migrate` on start (see `docker/entrypoint.sh`), so the
> API is serving as soon as the stack is healthy. `make run-batch`/`make backfill`
> create the ClickHouse schema and S3 bucket for you first.

### Expected numbers (dataset sanity values)

After `make pipeline` the metrics match the dataset's published checks exactly:

| Check | Value |
|---|---|
| Distinct valid payment attempts | 6000 |
| Distinct succeeded payments | 4295 |
| Rejected (invalid) records | 60 |
| Daily gross revenue 2026-01-15 (USD) | 40,461.59 |
| Active subscriptions as of 2026-01-31 | 755 |

---

## 2. API

| Endpoint | Filters | Description |
|---|---|---|
| `GET /api/metrics/daily` | `start, end, currency, plan` | Per-day gross/net/refunds, success & refund rates, active subs |
| `GET /api/metrics/summary` | `start, end, currency, plan` | Totals over the range + active subs at end-of-range + reject count |
| `GET /api/metrics/cohorts` | `plan` | Monthly start-cohort retention |

`currency` filters revenue (the original transaction currency); `plan` filters the
subscription metrics. Payments in the feed are not linked to a plan, so `plan` does not
sub-divide revenue — see *Design notes* below.

Example:

```bash
curl 'http://localhost:8000/api/metrics/summary?start=2026-01-01&end=2026-01-31&currency=EUR'
curl 'http://localhost:8000/api/metrics/daily?start=2026-01-15&end=2026-01-15'
```

---

## 3. Architecture & key decisions

### Layers

| Layer | Store | Engine / form | Why |
|---|---|---|---|
| Source of record | PostgreSQL | Django models keyed on external ids | Operational truth; faithful (raw) copy of the feed for audit |
| Bronze (lake) | S3 (LocalStack) | Parquet, partitioned `date=/batch=` | Immutable, replayable landing zone |
| Bronze (warehouse) | ClickHouse | `ReplacingMergeTree(ingested_at)` | Loadable copy; dedup on id |
| Silver | ClickHouse | `ReplacingMergeTree(ingested_at)` | **Cleaned & validated** rows; rejects split out |
| Gold | ClickHouse | **Views** with `FINAL` | Metrics, always consistent with Silver |

### Idempotency strategy (Core Problem #1 & #2)

Three independent guards mean a re-run — or a nightly job that fires twice — can never
double a figure:

1. **Postgres seed** upserts on the external id (`bulk_create(update_conflicts=True)`).
   Re-seeding the feed (including its 40 exact-duplicate payment rows) converges to the
   same rows.
2. **Incremental export** is watermarked. Each source has a `BatchCheckpoint`; a batch
   reads only rows with `ingested_at > last_ingested_at`. We watermark on the *ingest*
   time, not the business event time, so **late-arriving / corrected rows** (old event
   time, fresh ingest time) are still picked up.
3. **ClickHouse `ReplacingMergeTree`** is keyed on the business id (`payment_attempt_id`,
   `refund_id`, `subscription_event_id`). Re-loading the same id collapses to one row,
   and every Gold view reads with **`FINAL`** so results are exact regardless of whether
   a background merge has happened yet. The integration test `test_rerun_does_not_double`
   resets the watermark, re-runs the whole batch, and asserts revenue and row counts are
   unchanged.

### Late refunds (Core Problem #2)

A refund carries its own (later) processing time, but Net Revenue must correct the
**original payment's** day. `v_refunds_usd` joins each refund back to its payment, takes
the **payment's** event date & currency, converts the refund at the **payment's** FX
rate, and aggregates into `v_daily_revenue` on the payment's day. So a refund that
arrives on Jan 20 for a Jan 1 payment reduces Jan 1's net — never Jan 20's.

### Failed payments (Core Problem #3)

`v_daily_payments` computes `attempts = count()` (every attempt, incl. retries & failures)
but `gross_usd = sumIf(usd_amount, status='succeeded')`. So failed/pending attempts are
**excluded from revenue** yet **counted in the success-rate denominator**
(`success_rate = succeeded / attempts`).

### Separation of concerns (Core Problem #4)

The DRF layer (`analytics/api/queries.py`) issues SQL exclusively against ClickHouse.
Postgres is never queried for reporting. The only Postgres↔ClickHouse link is the batch
job moving data forward through the lake.

### Data quality / rejection (Core Problem #5)

The rule "non-positive amount **or** unsupported currency → reject" lives once, in
`analytics/pipeline/validation.py`, expressed both in Python (used by tests) and as the
SQL predicate used by the bronze→silver transform. Invalid rows are routed to
`silver_rejects` (with a reason) instead of silently dropped, so they're auditable. The
feed's 60 invalid records are rejected; 6000 valid attempts remain.

### FX conversion

`dim_fx_rates` is a small dimension. Payments convert at `rate(currency, event_date)`;
refunds convert at `rate(payment.currency, payment.event_date)`. Missing-rate rows would
surface as NULL (none occur in this dataset; USD is always 1.0).

### ClickHouse engine choices

* **`ReplacingMergeTree(ingested_at)`** for every bronze/silver table — the natural fit
  for "idempotent upsert keyed on an external id, latest ingest wins."
* **`FINAL` in Gold views** — forces query-time dedup so metrics are correct *immediately*
  after load, not only after a background merge. Acceptable cost at this data scale; at
  larger scale you'd move hot aggregates into `AggregatingMergeTree`/`SummingMergeTree`
  materialized views.
* **`PARTITION BY toYYYYMM(event_date)`** — aligns partitions with the reporting grain
  and makes windowed backfills (`ALTER TABLE … DELETE WHERE event_date BETWEEN …`) cheap.
* **Views, not materialized tables, for Gold** — keeps metrics always consistent with
  Silver and trivially re-derivable; the dataset is small enough that recomputation is
  instant.

### Active subscriptions & cohorts

`v_subscription_lifecycle` reduces each `subscription_id` to `start_date` / first
`cancel_date` / `plan`. A subscription is **active at end of day D** when
`start_date ≤ D AND cancel_date > D`, so a **same-day start-and-cancel** is correctly
*not* active. `v_active_subscriptions_daily` cross-joins the lifecycle with a generated
calendar to give a per-day, per-plan active series; `v_cohort_retention` groups by start
month and measures the share still active at the end of each later month.

### Firehose note

The brief specifies delivery to S3 Bronze via LocalStack Firehose as Parquet. Firehose's
native Parquet conversion requires Glue (a LocalStack-Pro feature), so the runnable path
writes Parquet **directly** to S3 — the exact format/partitioning requested — while
`bootstrap_s3` still provisions the Firehose delivery stream as the conceptual mechanism.
See `analytics/pipeline/aws.py`.

---

## 4. Repository layout

```
docker-compose.yml          Postgres, ClickHouse, LocalStack, web (Django)
Dockerfile / docker/        web image + entrypoint (waits for PG, migrates)
Makefile                    up / migrate / seed / run-batch / backfill / test / dashboard
.env.example                all configuration
dataset/                    the provided CSVs (+ README_DATASET.md)
pipeline/
  config/                   Django settings / urls / wsgi
  analytics/
    models.py               Postgres system-of-record + BatchCheckpoint / BatchRun
    management/commands/     seed, init_clickhouse, bootstrap_s3, run_batch, backfill
    pipeline/                extract (watermark→Parquet→S3), load (S3→CH→silver),
                             runner, sources, validation, aws
    clickhouse/              client + ddl.py (all tables & reporting views)
    api/                     DRF views, urls, ClickHouse-only queries
    tests/                   idempotency, reruns, metric calculations, reference impl
  templates/dashboard.html   verification frontend
```

---

## 5. Tests

`make test` runs everything inside the stack:

* `test_seed_idempotency.py` — seeding twice is identical; an upsert overwrites, never
  duplicates.
* `test_validation.py` — the reject rules; the dataset yields exactly 60 rejects.
* `test_metrics_reference.py` — sanity values on the real data **plus** synthetic
  fixtures isolating late-refund attribution, failed-payment handling, and same-day
  start/cancel.
* `test_clickhouse_integration.py` — full Postgres→S3→ClickHouse run: silver counts,
  gross/active sanity values, ClickHouse output == Python reference, and the
  "nightly job runs twice doesn't double" guarantee.

The unit tests also run locally without Docker:

```bash
cd pipeline
USE_SQLITE_FOR_TESTS=1 DATASET_DIR=../dataset pytest -q   # CH integration tests auto-skip
```
