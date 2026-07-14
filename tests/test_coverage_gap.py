"""
Tests for the scoring coverage-gap fix in batch/scorer.py.

Root cause: run_batch_scoring's input (`_fetch_active_hubspot_leads`) filters on
`lead_tier HAS_PROPERTY`, so only already-scored contacts are ever re-scored —
freshly ACTIVE-but-unscored leads (e.g. offer-page visitors) never enter the
batch. The fix pulls recently-active emails from Supabase and merges the ones
HubSpot knows but hasn't tiered yet.

Guarantees:
  - SCORE_ACTIVE_UNSCORED=OFF (default) → SHADOW: the active-unscored contact is
    NOT scored (absent from HubSpot updates); stats.active_unscored_shadow counts it.
  - SCORE_ACTIVE_UNSCORED=ON → the active-unscored contact IS scored (present in
    HubSpot updates); stats.active_unscored_added counts it.
  - already-active emails already in the main scan are not double-counted.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import asyncio

import pytest

import batch.scorer as scorer
from tests.test_scoring_mode import _patch_batch, _CONTACT


# A recently-active contact HubSpot knows but has NOT tiered yet (no lead_tier).
_ACTIVE_UNSCORED = {
    "id": "active1",
    "properties": {
        "email": "fresh@example.de",
        "firstname": "Fresh",
        "lastname": "Lead",
        "phone": "+41791110000",
        # no lead_tier -> exactly the gap we want to close
    },
}


def _patch_coverage(monkeypatch, *, active_emails, hs_by_email):
    """Layer the coverage-gap mocks on top of the standard batch harness."""

    async def _active(days=14):
        return set(active_emails)

    async def _by_emails(emails):
        return [c for c in hs_by_email if c["properties"]["email"] in emails]

    monkeypatch.setattr(scorer, "fetch_recently_active_emails", _active)
    monkeypatch.setattr(scorer, "_fetch_hubspot_contacts_by_emails", _by_emails)


def _updated_ids(captured):
    return {u["id"] for u in captured["hubspot_updates"]}


class TestCoverageGapShadow:
    def test_shadow_does_not_score_active_unscored(self, monkeypatch):
        captured = {}
        _patch_batch(monkeypatch, captured)
        _patch_coverage(
            monkeypatch,
            active_emails={"fresh@example.de"},
            hs_by_email=[_ACTIVE_UNSCORED],
        )
        monkeypatch.setattr(scorer, "SCORING_MODE", "engagement")
        monkeypatch.setattr(scorer, "SCORE_ACTIVE_UNSCORED", False)

        asyncio.run(scorer.run_batch_scoring())

        # Flag OFF: the fresh contact is counted but NOT scored.
        assert "active1" not in _updated_ids(captured)
        assert captured["stats"].active_unscored_shadow == 1
        assert captured["stats"].active_unscored_added == 0


class TestCoverageGapLive:
    def test_flag_on_scores_active_unscored(self, monkeypatch):
        captured = {}
        _patch_batch(monkeypatch, captured)
        _patch_coverage(
            monkeypatch,
            active_emails={"fresh@example.de"},
            hs_by_email=[_ACTIVE_UNSCORED],
        )
        monkeypatch.setattr(scorer, "SCORING_MODE", "engagement")
        monkeypatch.setattr(scorer, "SCORE_ACTIVE_UNSCORED", True)

        asyncio.run(scorer.run_batch_scoring())

        # Flag ON: the fresh contact is now scored (present in HubSpot updates).
        assert "active1" in _updated_ids(captured)
        assert captured["stats"].active_unscored_added == 1
        assert captured["stats"].active_unscored_shadow == 0


class TestCoverageGapDedup:
    def test_already_scored_active_email_not_recounted(self, monkeypatch):
        # The active email equals the one the main scan already returned
        # (lead@example.de from _CONTACT) -> nothing to add.
        captured = {}
        _patch_batch(monkeypatch, captured)
        main_email = _CONTACT["properties"]["email"]
        _patch_coverage(
            monkeypatch,
            active_emails={main_email},
            hs_by_email=[],  # _by_emails would get an empty new_active list anyway
        )
        monkeypatch.setattr(scorer, "SCORING_MODE", "engagement")
        monkeypatch.setattr(scorer, "SCORE_ACTIVE_UNSCORED", False)

        asyncio.run(scorer.run_batch_scoring())

        assert captured["stats"].active_unscored_shadow == 0
        assert captured["stats"].active_unscored_added == 0

    def test_coverage_step_failure_is_non_fatal(self, monkeypatch):
        captured = {}
        _patch_batch(monkeypatch, captured)

        async def _boom(days=14):
            raise RuntimeError("supabase down")

        monkeypatch.setattr(scorer, "fetch_recently_active_emails", _boom)
        monkeypatch.setattr(scorer, "SCORING_MODE", "engagement")
        monkeypatch.setattr(scorer, "SCORE_ACTIVE_UNSCORED", True)

        # Batch still completes and scores the main contact.
        asyncio.run(scorer.run_batch_scoring())
        assert "c1" in _updated_ids(captured)
