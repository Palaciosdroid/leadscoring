"""Tests for combined scoring and tier assignment."""

from scoring.combined import (
    _determine_tier,
    combine_scores,
    ScoringResult,
    ENGAGEMENT_WEIGHT,
    AI_WEIGHT,
)


class TestDetermineTier:
    def test_hot(self):
        assert _determine_tier(75) == "1_hot"
        assert _determine_tier(100) == "1_hot"

    def test_warm(self):
        assert _determine_tier(40) == "2_warm"
        assert _determine_tier(74.9) == "2_warm"

    def test_cold(self):
        assert _determine_tier(0) == "3_cold"
        assert _determine_tier(39.9) == "3_cold"

    def test_disqualified(self):
        assert _determine_tier(-1) == "4_disqualified"
        assert _determine_tier(-50) == "4_disqualified"


class TestCombineScores:
    def _engagement(self, score: int, days: float = 1.0, unsub: bool = False) -> dict:
        return {
            "score": score,
            "raw_score": float(score),
            "event_breakdown": [],
            "inactivity_malus": 0,
            "days_since_last_activity": days,
            "unsubscribed": unsub,
        }

    def _interest(self, cat: str | None = None, conf: float = 0.0) -> dict:
        return {"category": cat, "confidence": conf, "category_scores": {}}

    def test_engagement_only_hot(self):
        result = combine_scores(self._engagement(80), self._interest("hypnose", 0.9))
        assert result.lead_tier == "1_hot"
        assert result.combined_score == 80.0
        assert result.ai_score is None

    def test_engagement_only_warm(self):
        result = combine_scores(self._engagement(50), self._interest())
        assert result.lead_tier == "2_warm"

    def test_engagement_only_cold(self):
        result = combine_scores(self._engagement(20), self._interest())
        assert result.lead_tier == "3_cold"

    def test_disqualified_negative(self):
        result = combine_scores(self._engagement(-10, unsub=True), self._interest())
        assert result.lead_tier == "4_disqualified"
        assert result.unsubscribed is True

    def test_interest_category_passed_through(self):
        result = combine_scores(self._engagement(60), self._interest("meditation", 0.85))
        assert result.interest_category == "meditation"
        assert result.interest_confidence == 0.85


class TestScoringResult:
    def test_is_hot(self):
        r = ScoringResult(
            engagement_score=80, ai_score=None, combined_score=80.0,
            lead_tier="1_hot", interest_category="hypnose",
            interest_confidence=0.9, days_since_last_activity=1.0,
            unsubscribed=False,
        )
        assert r.is_hot is True
        assert r.tier_label == "Hot"

    def test_tier_labels(self):
        for tier, label in [("1_hot", "Hot"), ("2_warm", "Warm"), ("3_cold", "Cold"), ("4_disqualified", "Disqualified")]:
            r = ScoringResult(
                engagement_score=0, ai_score=None, combined_score=0,
                lead_tier=tier, interest_category=None,
                interest_confidence=0, days_since_last_activity=0,
                unsubscribed=False,
            )
            assert r.tier_label == label

    def test_hubspot_payload_fields(self):
        r = ScoringResult(
            engagement_score=60, ai_score=None, combined_score=60.0,
            lead_tier="2_warm", interest_category="lifecoach",
            interest_confidence=0.7, days_since_last_activity=3.0,
            unsubscribed=False,
        )
        payload = r.to_hubspot_payload()
        assert payload["lead_engagement_score"] == 60
        assert payload["lead_tier"] == "2_warm"
        assert payload["lead_interest_category"] == "lifecoach"
        assert payload["lead_score_version"] == "1.0.0"
        assert "lead_score_updated_at" in payload
