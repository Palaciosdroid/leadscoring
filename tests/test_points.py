"""Tests for the transparent point-system scorer (scoring/points.py)."""

from scoring.points import (
    BUDGET_POINTS,
    CHECKOUT_POINTS,
    CONSULT_POINTS,
    DISQUALIFIED_TIER,
    FORM_SUBMIT_POINTS,
    HYPNOSE_CATEGORY_POINTS,
    INTEREST_POINTS,
    LAUNCHCALL_POINTS,
    PRICE_POINTS,
    REPLAY_POINTS,
    VIDEO_COMPLETE_POINTS,
    compute_points,
)


def test_launchcall():
    res = compute_points({"launchcall": True})
    assert res.points == LAUNCHCALL_POINTS
    assert any("Launchcall" in r for r in res.reasons)
    assert compute_points({"launchcall": False}).points == 0


def test_launchcall_stacks_and_can_reach_hot():
    # launchcall (25) + checkout (25) + strong interest (25) + budget (30) = 105 -> hot
    res = compute_points({
        "launchcall": True, "checkout": True,
        "interest": "naechster_schritt", "budget": "4000_6000",
    })
    assert res.points == LAUNCHCALL_POINTS + CHECKOUT_POINTS + 25 + 30
    assert res.tier == "1_hot"


def test_launchcall_does_not_override_disqualify():
    res = compute_points({"launchcall": True, "unsubscribed": True})
    assert res.tier == DISQUALIFIED_TIER


# ---------------------------------------------------------------------------
# Per-signal point values
# ---------------------------------------------------------------------------
def test_budget_4000_plus():
    assert compute_points({"budget": "4000_6000"}).points == 30
    assert compute_points({"budget": "6000_8000"}).points == 30


def test_budget_2000_4000():
    assert compute_points({"budget": "2000_4000"}).points == 15


def test_budget_unter_2000_zero():
    res = compute_points({"budget": "unter_2000"})
    assert res.points == 0
    # 0-point budget contributes no reason line.
    assert res.reasons == []


def test_interest_naechster_schritt():
    assert compute_points({"interest": "naechster_schritt"}).points == 25


def test_interest_grundsaetzlich():
    assert compute_points({"interest": "grundsaetzlich"}).points == 10


def test_consult():
    assert compute_points({"consult": True}).points == CONSULT_POINTS
    assert compute_points({"consult": False}).points == 0


def test_replay():
    assert compute_points({"replay": True}).points == REPLAY_POINTS


def test_video_complete():
    assert compute_points({"video_complete": True}).points == VIDEO_COMPLETE_POINTS


def test_checkout():
    assert compute_points({"checkout": True}).points == CHECKOUT_POINTS


def test_price():
    assert compute_points({"price": True}).points == PRICE_POINTS


def test_form_submit():
    assert compute_points({"form_submit": True}).points == FORM_SUBMIT_POINTS


def test_interest_category_hypnose():
    assert compute_points({"interest_category": "hypnose"}).points == HYPNOSE_CATEGORY_POINTS
    # Other categories do not add the product-fit bonus.
    assert compute_points({"interest_category": "lifecoach"}).points == 0


def test_weight_constants_match_spec():
    assert BUDGET_POINTS == {
        "6000_8000": 30,
        "4000_6000": 30,
        "2000_4000": 15,
        "unter_2000": 0,
    }
    assert INTEREST_POINTS["naechster_schritt"] == 25
    assert INTEREST_POINTS["grundsaetzlich"] == 10
    assert INTEREST_POINTS["keines"] == 0


# ---------------------------------------------------------------------------
# Disqualify overrides
# ---------------------------------------------------------------------------
def test_interest_keines_disqualifies():
    res = compute_points({"interest": "keines"})
    assert res.tier == DISQUALIFIED_TIER
    assert any("disqualified" in r for r in res.reasons)


def test_disqualify_overrides_high_points():
    # Strong behavioral signals but "gar nicht interessiert" -> still disqualified.
    res = compute_points({
        "budget": "4000_6000",
        "checkout": True,
        "interest": "keines",
    })
    assert res.tier == DISQUALIFIED_TIER
    # Points still accumulate (budget+checkout) but the tier is forced.
    assert res.points == 55


def test_unsubscribed_disqualifies():
    res = compute_points({"budget": "4000_6000", "unsubscribed": True})
    assert res.tier == DISQUALIFIED_TIER
    assert any("Unsubscribed" in r for r in res.reasons)


# ---------------------------------------------------------------------------
# Tier boundaries (re-calibrated 07.07: Hot >= 80, Warm >= 50, else Cold)
# ---------------------------------------------------------------------------
def test_tier_cold_below_warm():
    # 15 points -> Cold.
    assert compute_points({"budget": "2000_4000"}).tier == "3_cold"


def test_tier_cold_zero():
    assert compute_points({}).tier == "3_cold"


def test_tier_mid_scores_are_cold_now():
    # 35 and 55 were Warm/Hot under the old 50/35 thresholds — calibration
    # showed those bands close at only 1.2-3.3%, below the 4% warm target.
    assert compute_points({"interest": "naechster_schritt", "form_submit": True}).points == 35
    assert compute_points({"interest": "naechster_schritt", "form_submit": True}).tier == "3_cold"


def test_tier_warm_at_50():
    # checkout 25 + naechster_schritt 25 = 50 -> Warm (>= boundary).
    res = compute_points({"checkout": True, "interest": "naechster_schritt"})
    assert res.points == 50
    assert res.tier == "2_warm"


def test_tier_warm_range_below_hot():
    # budget 30 + naechster_schritt 25 = 55 -> Warm (was Hot pre-calibration).
    res = compute_points({"budget": "4000_6000", "interest": "naechster_schritt"})
    assert res.points == 55
    assert res.tier == "2_warm"


def test_tier_hot_at_80():
    # budget 30 + interest 25 + checkout 25 = 80 -> Hot (>= boundary).
    res = compute_points({
        "budget": "4000_6000", "interest": "naechster_schritt", "checkout": True,
    })
    assert res.points == 80
    assert res.tier == "1_hot"


# ---------------------------------------------------------------------------
# Reasons breakdown + missing-signal safety
# ---------------------------------------------------------------------------
def test_reasons_explain_each_contribution():
    res = compute_points({
        "budget": "4000_6000",
        "interest": "naechster_schritt",
        "consult": True,
        "replay": True,
    })
    assert res.points == 30 + 25 + 15 + 20
    # One reason line per contributing signal.
    assert len(res.reasons) == 4
    joined = " | ".join(res.reasons)
    assert "Budget 4000_6000 +30" in joined
    assert "Interesse naechster_schritt +25" in joined
    assert "Beratung Ja +15" in joined
    assert "Replay +20" in joined


def test_empty_signals_no_crash():
    res = compute_points({})
    assert res.points == 0
    assert res.tier == "3_cold"
    assert res.reasons == []


def test_missing_signals_treated_as_zero():
    # Unknown / partial keys must not raise.
    res = compute_points({"foo": "bar", "budget": None, "interest": None})
    assert res.points == 0
    assert res.tier == "3_cold"


def test_unknown_budget_enum_ignored():
    res = compute_points({"budget": "weird_value"})
    assert res.points == 0


# --- Email engagement (ADL fix 07.07) ---------------------------------------

def test_email_click_scores():
    r = compute_points({"email_click": True})
    assert r.points == 10
    assert any("Email-Klick" in x for x in r.reasons)


def test_email_engaged_scores_without_click():
    r = compute_points({"email_engaged": True})
    assert r.points == 5


def test_email_click_supersedes_opens_no_double_count():
    r = compute_points({"email_click": True, "email_engaged": True})
    assert r.points == 10  # click only, opens not double-counted


def test_adl_buyer_profile_email_plus_form_is_not_floor():
    # ADL 6-case: buyers had form baseline + email engagement -> must exceed
    # the old 10-point floor that made them invisible in the shadow model.
    r = compute_points({"form_submit": True, "email_click": True})
    assert r.points == 20
