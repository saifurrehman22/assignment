"""
Seed idempotency tests (Core Problem #1: reruns produce identical state).

Runs the real `seed` management command against a throwaway test database twice and
asserts row counts are stable and that a changed row is *updated*, not duplicated.
"""
import os

import pytest
from django.core.management import call_command

from analytics import models

DATASET = os.environ.get("DATASET_DIR", "/data/dataset")
pytestmark = [
    pytest.mark.django_db,
    pytest.mark.skipif(not os.path.exists(DATASET), reason="dataset not mounted"),
]


def _counts():
    return {
        "customers": models.Customer.objects.count(),
        "payments": models.Payment.objects.count(),
        "refunds": models.Refund.objects.count(),
        "subs": models.SubscriptionEvent.objects.count(),
        "fx": models.FxRate.objects.count(),
    }


def test_seed_twice_is_idempotent():
    call_command("seed", dataset_dir=DATASET)
    first = _counts()
    call_command("seed", dataset_dir=DATASET)
    second = _counts()
    assert first == second
    # The feed has 6060 distinct payment ids (40 exact-duplicate rows collapsed).
    assert first["payments"] == 6060
    assert first["customers"] == 400


def test_seed_upserts_changed_row_without_duplicating():
    call_command("seed", dataset_dir=DATASET)
    pid = models.Payment.objects.first().pk
    models.Payment.objects.filter(pk=pid).update(status="MUTATED")
    before = models.Payment.objects.count()

    call_command("seed", dataset_dir=DATASET)  # should overwrite the mutation

    assert models.Payment.objects.count() == before          # no new row
    assert models.Payment.objects.get(pk=pid).status != "MUTATED"  # value restored
