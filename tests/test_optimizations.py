"""
Tests for the 4-part optimization:
  A — Purchase-Signal in engagement score
  B — Aircall priority header
  C1 — Recency boost for same-day events
  C2 — Inner Journey hook rule
  D — Funnel fallback from purchased_products
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from datetime import datetime, timezone, timedelta

from scoring.engagement import calculate_engagement_score, PURCHASE_BONUS, _purchase_bonus
from scoring.interest import detect_interest_category, _infer_from_purchased
from scoring.hook_engine import generate_hook


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _event(event_type: str, days_ago: float = 1.0) -> dict:
    ts = datetime.now(timezone.utc) - timedelta(days=days_ago)
    return {"event_type": event_type, "timestamp": ts.isoformat()}


# ---------------------------------------------------------------------------
# A — Purchase Bonus
# ---------------------------------------------------------------------------

class TestPurchaseBonus:

    def test_inner_journey_adds_20(self):
        result = calculate_engagement_score([], purchased_products=["inner_journey"])
        assert result["purchase_bonus"] == 20
        assert result["score"] == 20

    def test_bootcamp_adds_15(self):
        result = calculate_engagement_score([], purchased_products=["bootcamp"])
        assert result["purchase_bonus"] == 15
        assert result["score"] == 15

    def test_afk_adds_10(self):
        result = calculate_engagement_score([], purchased_products=["afk"])
        assert result["purchase_bonus"] == 10
        assert result["score"] == 10

    def test_no_purchases_no_bonus(self):
        result = calculate_engagement_score([], purchased_products=[])
        assert result["purchase_bonus"] == 0
        assert result["score"] == 0

    def test_none_purchased_products_no_bonus(self):
        result = calculate_engagement_score([])
        assert result["purchase_bonus"] == 0

    def test_purchase_combined_with_events(self):
        events = [_event("email_opened", days_ago=1)]  # 5 pts * recency
        result = calculate_engagement_score(events, purchased_products=["inner_journey"])
        assert result["purchase_bonus"] == 20
        assert result["score"] > 20  # events add on top

    def test_product_name_substring_matches(self):
        # Product key as part of longer product name string
        result = calculate_engagement_score([], purchased_products=["inner journey paket"])
        assert result["purchase_bonus"] == 20

    def test_no_double_counting_per_product(self):
        # Two products, but inner_journey appears only once
        bonus = _purchase_bonus(["inner_journey", "inner_journey"])
        assert bonus == 20  # capped per product

    def test_multiple_different_products(self):
        bonus = _purchase_bonus(["inner_journey", "bootcamp"])
        assert bonus == 35  # 20 + 15

    def test_unknown_product_no_bonus(self):
        bonus = _purchase_bonus(["some_unknown_thing"])
        assert bonus == 0


# ---------------------------------------------------------------------------
# C1 — Recency: same-day boost 1.5
# ---------------------------------------------------------------------------

class TestRecencyBoost:

    def test_same_day_event_gets_1_5_multiplier(self):
        """Event from < 1 day ago should use 1.5 multiplier."""
        event = _event("email_opened", days_ago=0.5)  # 12 hours ago
        result = calculate_engagement_score([event])
        # email_opened base = 5, multiplier = 1.5 → 7.5 → rounds to 8
        assert result["score"] == 8

    def test_two_day_event_gets_1_3_multiplier(self):
        """Event from 2 days ago → 1.3 multiplier."""
        event = _event("email_opened", days_ago=2.0)
        result = calculate_engagement_score([event])
        # 5 * 1.3 = 6.5 → Python banker's rounding → rounds to 6 (nearest even)
        assert result["score"] == 6

    def test_week_old_event_gets_1_0_multiplier(self):
        """Event from 5 days ago → 1.0."""
        event = _event("email_opened", days_ago=5.0)
        result = calculate_engagement_score([event])
        assert result["score"] == 5


# ---------------------------------------------------------------------------
# C2 — Inner Journey Hook Rule
# ---------------------------------------------------------------------------

class TestInnerJourneyHook:

    def test_hook_fires_for_inner_journey_buyer(self):
        ctx = {"purchased_products": ["inner_journey"], "funnel": "meditation"}
        hook = generate_hook(ctx)
        assert "Inner Journey" in hook

    def test_hook_fires_for_inner_journey_with_spaces(self):
        ctx = {"purchased_products": ["inner journey paket"], "funnel": "hypnose"}
        hook = generate_hook(ctx)
        assert "Inner Journey" in hook

    def test_hook_does_not_fire_if_has_ausbildung(self):
        """If lead already bought the full Ausbildung, don't pitch Inner Journey."""
        ctx = {"purchased_products": ["inner_journey", "mc"], "funnel": "meditation"}
        hook = generate_hook(ctx)
        assert "Inner Journey" not in hook

    def test_hook_does_not_fire_without_inner_journey(self):
        ctx = {"purchased_products": ["bootcamp"], "funnel": "meditation"}
        hook = generate_hook(ctx)
        assert "Inner Journey" not in hook

    def test_checkout_hook_takes_priority_over_inner_journey(self):
        """checkout_abandoned should fire before inner_journey rule."""
        ctx = {
            "purchased_products": ["inner_journey"],
            "checkout_visited": True,
            "funnel": "hypnose",
        }
        hook = generate_hook(ctx)
        assert "zurückgehalten" in hook  # checkout hook


# ---------------------------------------------------------------------------
# D — Funnel Fallback from Purchased Products
# ---------------------------------------------------------------------------

class TestFunnelFallback:

    def test_infer_hc_to_hypnose(self):
        assert _infer_from_purchased(["hc"]) == "hypnose"

    def test_infer_mc_to_meditation(self):
        assert _infer_from_purchased(["mc"]) == "meditation"

    def test_infer_gc_to_lifecoach(self):
        assert _infer_from_purchased(["gc"]) == "lifecoach"

    def test_infer_full_name_hypnose(self):
        assert _infer_from_purchased(["hypnose ausbildung"]) == "hypnose"

    def test_infer_full_name_meditation(self):
        assert _infer_from_purchased(["meditationscoach 2026"]) == "meditation"

    def test_empty_list_returns_none(self):
        assert _infer_from_purchased([]) is None

    def test_unknown_products_returns_none(self):
        assert _infer_from_purchased(["something_else"]) is None

    def test_detect_interest_uses_fallback_when_no_url_events(self):
        """No URL events → fallback to purchased_products."""
        result = detect_interest_category([], purchased_products=["hc"])
        assert result["category"] == "hypnose"
        assert result["inferred_from_purchase"] is True
        assert result["confidence"] == 0.5

    def test_detect_interest_url_wins_over_purchase(self):
        """URL signal takes priority over purchase fallback."""
        events = [{
            "event_type": "sales_page_visited",
            "url": "https://example.com/meditation-ausbildung",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }]
        result = detect_interest_category(events, purchased_products=["hc"])
        assert result["category"] == "meditation"
        assert result["inferred_from_purchase"] is False

    def test_detect_interest_no_events_no_purchase_returns_none(self):
        result = detect_interest_category([], purchased_products=[])
        assert result["category"] is None
        assert result["inferred_from_purchase"] is False
