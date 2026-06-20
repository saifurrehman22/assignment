"""Data-quality rule tests (Core Problem #5: invalid records are rejected)."""
import os

import pytest

from analytics.pipeline import validation
from analytics.tests import reference_metrics as ref

DATASET = os.environ.get("DATASET_DIR", "/data/dataset")


def test_reject_reason_rules():
    assert validation.reject_reason("100.00", "USD") is None
    assert validation.reject_reason("0", "USD") == "non_positive_amount"
    assert validation.reject_reason("-5", "EUR") == "non_positive_amount"
    assert validation.reject_reason("10", "ZZZ") == "unsupported_currency"
    # non-positive takes precedence over a bad currency (matches the SQL CASE order)
    assert validation.reject_reason("-1", "ZZZ") == "non_positive_amount"


def test_supported_currencies():
    assert validation.SUPPORTED_CURRENCIES == {"USD", "EUR", "GBP"}


@pytest.mark.skipif(not os.path.exists(DATASET), reason="dataset not mounted")
def test_dataset_reject_count_matches_sanity_value():
    # Published sanity value: exactly 60 records rejected as invalid.
    assert ref.reject_count(DATASET) == 60
