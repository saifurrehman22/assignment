# Dataset — Batch Analytics Pipeline Take-Home

Deterministic seed data for the assignment. Regenerate identically with
`python generate_dataset.py` (fixed random seed).

Load each CSV into the matching PostgreSQL table via your `make seed` command.
Reporting must be computed in **USD** with **UTC** day boundaries, converting each
transaction at the FX rate for its own event date, and converting a refund at the
**original payment's** rate (see the assignment brief for exact metric definitions).

## Files & schema

**customers.csv** — `customer_id, name, email, country, created_at`

**payments.csv** — `payment_attempt_id, customer_id, amount, currency, status, created_at`
- `status` is one of `succeeded`, `failed`, `pending`.
- `amount` is in the row's `currency` (major units). `created_at` is the attempt's event time (UTC).
- Each row is one payment *attempt*; a retry is a separate attempt with its own id.

**refunds.csv** — `refund_id, payment_id, amount, currency, created_at`
- `payment_id` references a `payment_attempt_id` in payments.csv.
- `created_at` is when the refund was processed (this can be much later than the original payment).

**subscription_events.csv** — `subscription_event_id, customer_id, subscription_id, event_type, plan, created_at`
- `event_type` is one of `start`, `renew`, `cancel`.

**fx_rates.csv** — `date, currency, rate_to_usd`
- Daily rate to convert `currency` -> USD on a given UTC `date`. `USD` is always `1.0`.

## What's in the data

This dataset intentionally contains the messy cases described in the assignment brief:
duplicate payment events, multiple currencies, full and partial refunds, **late-arriving
refunds** whose original payment is in an earlier period, failed and retried payments,
**invalid records** (non-positive amount or unknown currency) that must be rejected, and
subscription lifecycles including **same-day start-and-cancel**. Currencies present:
USD, EUR, GBP. Date range: 2026-01-01 to 2026-01-31 (UTC).

## Sanity-check values

Use these to verify your basic numbers as you build (after deduplication and after
rejecting invalid records):

- Distinct valid payment attempts: **6000**
- Distinct succeeded payments: **4295**
- Records rejected as invalid: **60**
- Daily gross revenue on 2026-01-15 (USD): **40,461.59**
- Active subscriptions as of 2026-01-31 (end of day, UTC): **755**
