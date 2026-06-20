"""
Canonical data-quality rules.

This is the single source of truth for what makes a record *invalid*. The ClickHouse
bronze->silver transform mirrors these exact predicates in SQL; the unit tests assert
against the Python versions here so the two cannot silently drift.

Rules (from the brief): a record is rejected when it has a non-positive amount or an
unsupported currency. Duplicates (same id) are NOT rejected — they are de-duplicated
by the ClickHouse ReplacingMergeTree keyed on the id.
"""
from decimal import Decimal

from django.conf import settings

SUPPORTED_CURRENCIES = set(settings.SUPPORTED_CURRENCIES)


def reject_reason(amount, currency: str) -> str | None:
    """Return a human-readable rejection reason, or None if the record is valid."""
    amt = Decimal(str(amount))
    if amt <= 0:
        return "non_positive_amount"
    if currency not in SUPPORTED_CURRENCIES:
        return "unsupported_currency"
    return None


def is_valid(amount, currency: str) -> bool:
    return reject_reason(amount, currency) is None


# The same predicate, as a ClickHouse SQL boolean expression. Kept here next to the
# Python rule so reviewers can see they are identical.
SQL_VALID_PREDICATE = "amount > 0 AND currency IN ('USD','EUR','GBP')"
SQL_INVALID_REASON = (
    "if(amount <= 0, 'non_positive_amount', 'unsupported_currency')"
)
