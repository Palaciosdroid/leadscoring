"""Test the stale-score warning on the Aircall card."""
from batch.scorer import _build_aircall_card

BASE = dict(
    tier_label="WARM", funnel="hypnose", score=55, last_call_date=None,
    email_summary={"opens": 0, "clicks": 0}, first_touch=None, last_touch=None,
    hook="Test hook",
)


def test_stale_score_warning_shown_over_90d():
    card = _build_aircall_card(**BASE, score_age_days=120)
    assert "⚠️ Score 120d alt" in card


def test_no_warning_when_fresh():
    card = _build_aircall_card(**BASE, score_age_days=5)
    assert "veraltet" not in card


def test_no_warning_when_age_unknown():
    card = _build_aircall_card(**BASE, score_age_days=None)
    assert "veraltet" not in card


def test_boundary_90d_no_warning():
    # exactly 90 days is not yet stale (> 90 required)
    card = _build_aircall_card(**BASE, score_age_days=90)
    assert "veraltet" not in card
