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
# Two-factor (engagement + AI only, no WhatsApp)
ENGAGEMENT_WEIGHT = 0.70   # when AI score is present
AI_WEIGHT = 0.30

# Three-factor (engagement + WhatsApp + AI)
ENGAGEMENT_WEIGHT_3F = 0.50
WHATSAPP_WEIGHT_3F = 0.30
AI_WEIGHT_3F = 0.20

# Two-factor (engagement + WhatsApp, no AI)
ENGAGEMENT_WEIGHT_WA = 0.60
WHATSAPP_WEIGHT_WA = 0.40

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
        if not (0.0 <= prob <= 1.0):  # guard against NaN / corrupt model output
            return None
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
    whatsapp_score: float | None = None
    score_version: str = "1.0.0"

    def to_hubspot_payload(self) -> dict[str, Any]:
        from datetime import datetime, timezone
        payload = {
            "lead_engagement_score":  self.engagement_score,
            "lead_ai_score":          self.ai_score,
            "lead_combined_score":    self.combined_score,
            "lead_tier":         self.lead_tier,
            "lead_interest_category": self.interest_category,
            "lead_score_updated_at":  datetime.now(timezone.utc).isoformat(),
            "lead_score_version":     self.score_version,
        }
        if self.whatsapp_score is not None:
            payload["lead_whatsapp_score"] = self.whatsapp_score
        return payload

    @property
    def is_hot(self) -> bool:
        return self.lead_tier == "1_hot"

    @property
    def tier_label(self) -> str:
        return {
            "0_booked":       "Booked",
            "1_hot":          "Hot",
            "2_warm":         "Warm",
            "3_cold":         "Cold",
            "4_disqualified": "Disqualified",
        }.get(self.lead_tier, "Unknown")


def map_whatsapp_to_engagement(whatsapp_data: dict[str, Any] | None) -> float | None:
    """
    Map WhatsApp qualification signals to an engagement-compatible score (0-100).

    Score mapping:
      - whatsapp_score >= 70: +35 points
      - whatsapp_score 40-69: +20 points
      - whatsapp_score < 40: +5 points
      - wants_to_coach = true: +15
      - personal_growth = true: +10
      - has_calendar_link = true: +20
      - opted_out = true: -100
    """
    if whatsapp_data is None:
        return None

    wa_score = whatsapp_data.get("whatsapp_score", 0)
    points = 0.0

    # Base score from WhatsApp bot qualification
    if wa_score >= 70:
        points += 35
    elif wa_score >= 40:
        points += 20
    else:
        points += 5

    # Bonus signals
    if whatsapp_data.get("wants_to_coach"):
        points += 15
    if whatsapp_data.get("personal_growth"):
        points += 10
    if whatsapp_data.get("has_calendar_link"):
        points += 20

    # Hard penalty for opt-out
    if whatsapp_data.get("opted_out"):
        points -= 100

    return max(min(round(points, 2), 100), -100)


def combine_scores(
    engagement_result: dict[str, Any],
    interest_result: dict[str, Any],
    ai_features: dict[str, float] | None = None,
    whatsapp_data: dict[str, Any] | None = None,
) -> ScoringResult:
    """
    Build final ScoringResult from engagement + interest + optional AI + optional WhatsApp.

    Formula depends on available data:
      - All three:           engagement*0.5 + whatsapp*0.3 + ai*0.2
      - Engagement + WA:     engagement*0.6 + whatsapp*0.4
      - Engagement + AI:     engagement*0.7 + ai*0.3  (original)
      - Engagement only:     engagement*1.0
    """
    engagement_score: int = engagement_result["score"]
    ai_score = predict_ai_score(ai_features) if ai_features else None
    wa_mapped = map_whatsapp_to_engagement(whatsapp_data)

    if ai_score is not None and wa_mapped is not None:
        # Three-factor: engagement + WhatsApp + AI
        combined = round(
            engagement_score * ENGAGEMENT_WEIGHT_3F
            + wa_mapped * WHATSAPP_WEIGHT_3F
            + ai_score * AI_WEIGHT_3F,
            2,
        )
    elif wa_mapped is not None:
        # Two-factor: engagement + WhatsApp (no AI)
        combined = round(
            engagement_score * ENGAGEMENT_WEIGHT_WA
            + wa_mapped * WHATSAPP_WEIGHT_WA,
            2,
        )
    elif ai_score is not None:
        # Two-factor: engagement + AI (no WhatsApp) — original formula
        combined = round(engagement_score * ENGAGEMENT_WEIGHT + ai_score * AI_WEIGHT, 2)
    else:
        # Single factor: engagement only
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
        whatsapp_score=wa_mapped,
    )
