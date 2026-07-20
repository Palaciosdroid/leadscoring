"""
Tests for the flag-gated PostHog intent signals (Spec 2026-07-09, Sync 20.07).

Guarantees:
  - POSTHOG_SIGNAL_POINTS_ENABLED off (default) → the signal dict is
    BYTE-IDENTICAL to today's (no new keys), so compute_points output is
    unchanged even when the HubSpot props are populated.
  - Flag on → spec weights per bucket (+40 payment / +25 dwell>=5 / +25 vsl>=90
    / +15 dwell>=2 / +15 vsl>=50), payment date-decay (>14d half, >30d zero),
    edge cases (empty/junk props, epoch-millis dates) never crash.
  - intent_funnel is NEVER a score input (routing only).
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from datetime import datetime, timedelta, timezone

import pytest

import batch.scorer as scorer
from scoring.points import (
    compute_points,
    payment_page_points_for_age,
    posthog_signals_enabled,
    POSTHOG_SIGNAL_FLAG_ENV,
    PAYMENT_PAGE_POINTS,
    OFFER_DWELL_HOT_POINTS,
    OFFER_DWELL_WARM_POINTS,
    VSL_HOT_POINTS,
    VSL_WARM_POINTS,
)


def _days_ago_iso(days: float) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=days)).date().isoformat()


_POSTHOG_PROPS = {
    "offer_dwell_minutes": "5.5",
    "offer_dwell_last_at": _days_ago_iso(1),
    "payment_page_visited": _days_ago_iso(1),
    "vsl_watched_percent": "95",
    "vsl_watched_last_at": _days_ago_iso(1),
    "intent_funnel": "MC (Meditationscoach)",
}


def _dwell(minutes: str, anchor_days: float | None):
    props = {"offer_dwell_minutes": minutes}
    if anchor_days is not None:
        props["offer_dwell_last_at"] = _days_ago_iso(anchor_days)
    return props


def _vsl(percent: str, anchor_days: float | None):
    props = {"vsl_watched_percent": percent}
    if anchor_days is not None:
        props["vsl_watched_last_at"] = _days_ago_iso(anchor_days)
    return props


@pytest.fixture
def flag_off(monkeypatch):
    monkeypatch.delenv(POSTHOG_SIGNAL_FLAG_ENV, raising=False)


@pytest.fixture
def flag_on(monkeypatch):
    monkeypatch.setenv(POSTHOG_SIGNAL_FLAG_ENV, "1")


# ---------------------------------------------------------------------------
# Flag OFF — byte-identical behavior
# ---------------------------------------------------------------------------

class TestFlagOff:
    def test_flag_default_is_off(self, flag_off):
        assert posthog_signals_enabled() is False

    def test_signal_dict_identical_with_and_without_posthog_props(self, flag_off):
        base_props = {"lead_eig_budget": "4000_6000", "lead_eig_interest": "naechster_schritt"}
        with_posthog = {**base_props, **_POSTHOG_PROPS}
        sig_plain = scorer._assemble_point_signals([], base_props, "meditation", False)
        sig_posthog = scorer._assemble_point_signals([], with_posthog, "meditation", False)
        assert sig_plain == sig_posthog  # byte-identical: no new keys at all
        assert "payment_page_age_days" not in sig_posthog
        assert "offer_dwell_minutes" not in sig_posthog
        assert "vsl_watched_percent" not in sig_posthog

    def test_score_unchanged_with_populated_props(self, flag_off):
        sig = scorer._assemble_point_signals([], _POSTHOG_PROPS, None, False)
        result = compute_points(sig)
        assert result.points == 0
        assert result.reasons == []

    def test_flag_off_values(self, monkeypatch):
        for raw in ("", "0", "false", "no", "off"):
            monkeypatch.setenv(POSTHOG_SIGNAL_FLAG_ENV, raw)
            assert posthog_signals_enabled() is False


# ---------------------------------------------------------------------------
# Flag ON — spec weights per bucket
# ---------------------------------------------------------------------------

class TestFlagOnBuckets:
    def test_payment_fresh_plus_40(self, flag_on):
        sig = scorer._assemble_point_signals(
            [], {"payment_page_visited": _days_ago_iso(0)}, None, False
        )
        result = compute_points(sig)
        assert result.points == PAYMENT_PAGE_POINTS == 40
        assert any("Payment-Page" in r for r in result.reasons)

    def test_payment_15_to_30_days_halved(self, flag_on):
        sig = scorer._assemble_point_signals(
            [], {"payment_page_visited": _days_ago_iso(20)}, None, False
        )
        assert compute_points(sig).points == PAYMENT_PAGE_POINTS // 2 == 20

    def test_payment_older_30_days_zero(self, flag_on):
        sig = scorer._assemble_point_signals(
            [], {"payment_page_visited": _days_ago_iso(40)}, None, False
        )
        result = compute_points(sig)
        assert result.points == 0
        assert result.reasons == []

    def test_dwell_hot_plus_25(self, flag_on):
        sig = scorer._assemble_point_signals([], _dwell("5", 0), None, False)
        assert compute_points(sig).points == OFFER_DWELL_HOT_POINTS == 25

    def test_dwell_warm_plus_15(self, flag_on):
        sig = scorer._assemble_point_signals([], _dwell("2.0", 0), None, False)
        assert compute_points(sig).points == OFFER_DWELL_WARM_POINTS == 15

    def test_dwell_below_warm_zero(self, flag_on):
        sig = scorer._assemble_point_signals([], _dwell("1.9", 0), None, False)
        assert compute_points(sig).points == 0

    def test_vsl_hot_plus_25(self, flag_on):
        sig = scorer._assemble_point_signals([], _vsl("90", 0), None, False)
        assert compute_points(sig).points == VSL_HOT_POINTS == 25

    def test_vsl_warm_plus_15(self, flag_on):
        sig = scorer._assemble_point_signals([], _vsl("50", 0), None, False)
        assert compute_points(sig).points == VSL_WARM_POINTS == 15

    def test_vsl_below_warm_zero(self, flag_on):
        sig = scorer._assemble_point_signals([], _vsl("49.9", 0), None, False)
        assert compute_points(sig).points == 0

    def test_all_three_signals_stack_to_90(self, flag_on):
        # Spec example: VSL 100 + Offer 5min + Payment = 90 points = queue top.
        sig = scorer._assemble_point_signals([], _POSTHOG_PROPS, None, False)
        result = compute_points(sig)
        assert result.points == 40 + 25 + 25 == 90
        assert result.tier == "1_hot"

    def test_stacks_on_top_of_existing_signals(self, flag_on):
        props = {**_POSTHOG_PROPS, "lead_eig_budget": "4000_6000"}
        sig = scorer._assemble_point_signals([], props, None, False)
        # + budget 30, + neutral-fill "Interesse unbekannt" 10 (budget prop
        # marks the lead as a quiz-taker — existing behavior, unchanged).
        assert compute_points(sig).points == 90 + 30 + 10

    def test_hot_via_posthog_only_dwell_and_vsl(self, flag_on):
        # dwell hot (25) + vsl hot (25) + payment 20d (20) = 70 >= 65 hot
        props = {
            "offer_dwell_minutes": "7",
            "offer_dwell_last_at": _days_ago_iso(3),
            "vsl_watched_percent": "100",
            "vsl_watched_last_at": _days_ago_iso(3),
            "payment_page_visited": _days_ago_iso(20),
        }
        result = compute_points(scorer._assemble_point_signals([], props, None, False))
        assert result.points == 70
        assert result.tier == "1_hot"


# ---------------------------------------------------------------------------
# Dwell/VSL decay via per-signal anchors (offer_dwell_last_at /
# vsl_watched_last_at — REPLY-posthog-CC-Decay-Anchors-2026-07-20.md)
# ---------------------------------------------------------------------------

class TestDwellVslAnchorDecay:
    def test_dwell_fresh_anchor_full(self, flag_on):
        sig = scorer._assemble_point_signals([], _dwell("6", 10), None, False)
        assert compute_points(sig).points == 25

    def test_dwell_anchor_15_to_30_days_halved(self, flag_on):
        sig = scorer._assemble_point_signals([], _dwell("6", 20), None, False)
        assert compute_points(sig).points == 25 // 2
        sig = scorer._assemble_point_signals([], _dwell("2.5", 20), None, False)
        assert compute_points(sig).points == 15 // 2

    def test_dwell_anchor_older_30_days_zero(self, flag_on):
        sig = scorer._assemble_point_signals([], _dwell("6", 40), None, False)
        result = compute_points(sig)
        assert result.points == 0
        assert result.reasons == []

    def test_dwell_value_without_anchor_scores_zero(self, flag_on):
        # Conservative: legacy value with no anchor must not look fresh forever.
        sig = scorer._assemble_point_signals([], _dwell("6", None), None, False)
        assert "offer_dwell_minutes" in sig
        assert "offer_dwell_age_days" not in sig
        assert compute_points(sig).points == 0

    def test_vsl_fresh_anchor_full(self, flag_on):
        sig = scorer._assemble_point_signals([], _vsl("95", 10), None, False)
        assert compute_points(sig).points == 25

    def test_vsl_anchor_15_to_30_days_halved(self, flag_on):
        sig = scorer._assemble_point_signals([], _vsl("95", 20), None, False)
        assert compute_points(sig).points == 25 // 2
        sig = scorer._assemble_point_signals([], _vsl("60", 20), None, False)
        assert compute_points(sig).points == 15 // 2

    def test_vsl_anchor_older_30_days_zero(self, flag_on):
        sig = scorer._assemble_point_signals([], _vsl("95", 40), None, False)
        assert compute_points(sig).points == 0

    def test_vsl_value_without_anchor_scores_zero(self, flag_on):
        sig = scorer._assemble_point_signals([], _vsl("95", None), None, False)
        assert "vsl_watched_percent" in sig
        assert "vsl_watched_age_days" not in sig
        assert compute_points(sig).points == 0

    def test_junk_anchor_treated_as_missing(self, flag_on):
        props = {"offer_dwell_minutes": "6", "offer_dwell_last_at": "not-a-date"}
        sig = scorer._assemble_point_signals([], props, None, False)
        assert compute_points(sig).points == 0

    def test_anchors_decay_independently(self, flag_on):
        # dwell fresh (25) + vsl half (12) + payment expired (0) = 37
        props = {
            **_dwell("6", 5),
            **_vsl("95", 20),
            "payment_page_visited": _days_ago_iso(45),
        }
        sig = scorer._assemble_point_signals([], props, None, False)
        assert compute_points(sig).points == 25 + 12


# ---------------------------------------------------------------------------
# Edge cases — junk props, wire formats, routing field
# ---------------------------------------------------------------------------

class TestEdgeCases:
    def test_empty_props_no_keys_no_points(self, flag_on):
        sig = scorer._assemble_point_signals([], {}, None, False)
        assert "payment_page_age_days" not in sig
        assert "offer_dwell_minutes" not in sig
        assert "vsl_watched_percent" not in sig
        assert compute_points(sig).points == 0

    def test_junk_values_never_crash(self, flag_on):
        props = {
            "offer_dwell_minutes": "abc",
            "payment_page_visited": "not-a-date",
            "vsl_watched_percent": None,
        }
        sig = scorer._assemble_point_signals([], props, None, False)
        assert compute_points(sig).points == 0

    def test_epoch_millis_date_parsed(self, flag_on):
        ms = str(int((datetime.now(timezone.utc) - timedelta(days=2)).timestamp() * 1000))
        sig = scorer._assemble_point_signals([], {"payment_page_visited": ms}, None, False)
        assert compute_points(sig).points == PAYMENT_PAGE_POINTS

    def test_future_date_clamped_to_full_points(self, flag_on):
        # Clock skew / timezone edge: future date → age 0 → full weight, no crash.
        sig = scorer._assemble_point_signals(
            [], {"payment_page_visited": _days_ago_iso(-1)}, None, False
        )
        assert compute_points(sig).points == PAYMENT_PAGE_POINTS

    def test_intent_funnel_is_never_scored(self, flag_on):
        sig = scorer._assemble_point_signals(
            [], {"intent_funnel": "AL (Ausbildung deines Lebens)"}, None, False
        )
        assert "intent_funnel" not in sig
        assert compute_points(sig).points == 0

    def test_disqualify_still_wins_over_posthog_signals(self, flag_on):
        sig = scorer._assemble_point_signals([], _POSTHOG_PROPS, None, True)
        assert compute_points(sig).tier == "4_disqualified"


# ---------------------------------------------------------------------------
# payment_page_points_for_age boundaries (pure unit)
# ---------------------------------------------------------------------------

class TestPaymentDecayBoundaries:
    @pytest.mark.parametrize("age,expected", [
        (0, 40), (14, 40), (14.5, 20), (30, 20), (30.5, 0), (365, 0),
    ])
    def test_boundaries(self, age, expected):
        assert payment_page_points_for_age(age) == expected

    def test_none_and_negative(self):
        assert payment_page_points_for_age(None) == 0
        assert payment_page_points_for_age(-1) == 0
