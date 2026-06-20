"""Registry describing each source table: how to read it from Postgres, what the
Parquet/bronze columns are, and the id / event-date used for partitioning."""
from dataclasses import dataclass, field

from analytics import models


@dataclass(frozen=True)
class Source:
    name: str
    model: type
    id_field: str
    created_field: str | None  # business event time (None for fx_rates -> use `date`)
    columns: list[str]         # columns to export to Parquet / bronze
    bronze_table: str
    silver_table: str

    def event_date_expr(self, row: dict):
        """Return the UTC event date (date object) used for partitioning."""
        if self.name == "fx_rates":
            return row["date"]
        return row[self.created_field].date()


SOURCES: dict[str, Source] = {
    "payments": Source(
        name="payments",
        model=models.Payment,
        id_field="payment_attempt_id",
        created_field="created_at",
        columns=[
            "payment_attempt_id", "customer_id", "amount", "currency",
            "status", "created_at",
        ],
        bronze_table="bronze_payments",
        silver_table="silver_payments",
    ),
    "refunds": Source(
        name="refunds",
        model=models.Refund,
        id_field="refund_id",
        created_field="created_at",
        columns=["refund_id", "payment_id", "amount", "currency", "created_at"],
        bronze_table="bronze_refunds",
        silver_table="silver_refunds",
    ),
    "subscription_events": Source(
        name="subscription_events",
        model=models.SubscriptionEvent,
        id_field="subscription_event_id",
        created_field="created_at",
        columns=[
            "subscription_event_id", "customer_id", "subscription_id",
            "event_type", "plan", "created_at",
        ],
        bronze_table="bronze_subscription_events",
        silver_table="silver_subscription_events",
    ),
    "fx_rates": Source(
        name="fx_rates",
        model=models.FxRate,
        id_field="id",
        created_field=None,
        columns=["date", "currency", "rate_to_usd"],
        bronze_table="bronze_fx_rates",
        silver_table="dim_fx_rates",
    ),
}

# Order matters for the transform: fx + payments must be in silver before refunds
# can be converted/attributed against them.
SOURCE_ORDER = ["fx_rates", "payments", "subscription_events", "refunds"]
