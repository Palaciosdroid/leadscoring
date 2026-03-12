"""
Combined Score + Lead Tier
Merges Engagement Score with optional AI Predictive Score
and returns the final tier (Hot / Warm / Cold / Disqualified).

Tiers (calibrated for Supabase touchpoints — email + ads + forms):
  Hot          >= 65  -> highest priority in Aircall
  Warm         30-64  -> follow-up queue in Aircall
  Cold          0-29  -> nurturing only (Customer.io)
  Disqualified  < 0   -> do not contact (unsubscribed)

Updated 2026-03-12: Supabase-first architecture, thresholds raised.
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import Any

# ---------------------------------------------------------------------------
# Weights
# ---------------------------------------------------------------------------
ENGAGEMENT_WEIGHT = 0.70   # when AI score is present
AI_WEIGHT = 0.30

# ---------------------------------------------------------------------------
# Tier thresholds — descending, first match wins
# Using threshold-only (>= check) avoids float boundary gaps (e.g. 39.5)
# ---------------------------------------------------------------------------
TIERS: list[tuple[str, float]] = [
    ("1_hot",          65.0),
    ("2_warm",         30.0),
    ("3_cold",          0.0),
    ("4_disqualified", float("-inf")),
]


def _determine_tier(score: float) -> str:
    for tier_id, threshold in TIERS:
        if score >= threshold:
            return tier_id
    return "4_disqualified"


# ---------------------------------------------------------------------------
# AI Score skeleton
# ---------------------------------------------------------------------------
def _load_ai_model():
    """
    Attempt to load a trained scikit-learn model from disk.
    Returns None if no model available yet.
    Activate once >= 100 labelled conversions are available.
    """
    try:
        import pickle
        import os
        model_path = os.environ.get("AI_MODEL_PATH", "lead_scoring_model.pkl")
        with open(model_path, "rb") as f:
            return pickle.load(f)
    except FileNotFoundError:
        return None
    except Exception:
        return None


_AI_MODEL = _load_ai_model()


def predict_ai_score(features: dict[str, float]) -> float | None:
    """
    Return AI predictive score (0-100) or None if model not ready.

    Features used:
      engagement_score, email_opens, email_clicks,
      video_views, sales_page_visits, checkout_visits, webinar_attended
    """
    if _AI_MODEL is None:
        return None

    import numpy as np  # only needed when model is active
    feature_vector = [
        features.get("engagement_score", 0),
        features.get("email_opens", 0),
        features.get("email_clicks", 0),
        features.get("video_views", 0),
        features.get("sales_page_visits", 0),
        features.get("checkout_visits", 0),
        features.get("webinar_attended", 0),
    ]
    try:
        prob = _AI_MODEL.predict_proba([feature_vector])[0][1]  # P(purchase)
        return round(prob * 100, 2)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Main combiner
# ---------------------------------------------------------------------------
@dataclass
class ScoringResult:
    engagement_score: int
    ai_score: float | None
    combined_score: float
    lead_tier: str
    interest_category: str | None
    interest_confidence: float
    days_since_last_activity: float
    unsubscribed: bool
    score_version: str = "1.0.0"

    def to_hubspot_payload(self) -> dict[str, Any]:
        from datetime import datetime, timezone
        return {
            "lead_engagement_score":  self.engagement_score,
            "lead_ai_score":          self.ai_score,
            "lead_combined_score":    self.combined_score,
            "lead_tier":         self.lead_tier,
            "lead_interest_category": self.interest_category,
            "lead_score_updated_at":  datetime.now(timezone.utc).isoformat(),
            "lead_score_version":     self.score_version,
        }

    @property
    def is_hot(self) -> bool:
        return self.lead_tier == "1_hot"

    @property
    def tier_label(self) -> str:
        return {
            "1_hot":          "Hot",
            "2_warm":         "Warm",
            "3_cold":         "Cold",
            "4_disqualified": "Disqualified",
        }.get(self.lead_tier, "Unknown")


def combine_scores(
    engagement_result: dict[str, Any],
    interest_result: dict[str, Any],
    ai_features: dict[str, float] | None = None,
) -> ScoringResult:
    """
    Build final ScoringResult from engagement + interest + optional AI input.
    """
    engagement_score: int = engagement_result["score"]
    ai_score = predict_ai_score(ai_features) if ai_features else None

    if ai_score is not None:
        combined = round(engagement_score * ENGAGEMENT_WEIGHT + ai_score * AI_WEIGHT, 2)
    else:
        combined = float(engagement_score)

    lead_tier = _determine_tier(combined)

    return ScoringResult(
        engagement_score=engagement_score,
        ai_score=ai_score,
        combined_score=combined,
        lead_tier=lead_tier,
        interest_category=interest_result.get("category"),
        interest_confidence=interest_result.get("confidence", 0.0),
        days_since_last_activity=engagement_result.get("days_since_last_activity", 999.0),
        unsubscribed=engagement_result.get("unsubscribed", False),
    )
