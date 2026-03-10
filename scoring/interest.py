"""
Interest Category Detector
Identifies which SBC coaching programme a lead is most interested in
based on page URLs visited and custom events.

Categories: hypnose | lifecoach | meditation
"""

import re
from collections import defaultdict
from typing import Any

# ---------------------------------------------------------------------------
# URL / event keyword mapping per product category
# ---------------------------------------------------------------------------
CATEGORY_SIGNALS: dict[str, list[str]] = {
    "hypnose": [
        "hypnose",
        "hypnosecoach",
        "hypnotherapy",
        "hypnosis",
        "trance",
        "nlp",            # often bundled with hypnosis programmes
    ],
    "lifecoach": [
        "lifecoach",
        "life-coach",
        "life_coach",
        "coaching-ausbildung",
        "coaching_ausbildung",
        "persoenlichkeit",
        "persoenlichkeitsentwicklung",
        "lebenscoach",
    ],
    "meditation": [
        "meditation",
        "meditationscoach",
        "achtsamkeit",
        "mindfulness",
        "breathwork",
        "yoga",
    ],
}

# Weight: how strongly each signal type counts toward category score
SIGNAL_WEIGHTS = {
    "page_visited": 1,
    "sales_page_visited": 3,
    "video_watched_50": 4,
    "video_watched_75": 6,
    "webinar_registered": 5,
    "webinar_attended": 8,
    "application_submitted": 15,   # strongest buying signal
    "checkout_visited": 12,
    "free_resource_downloaded": 4,
    "email_link_clicked": 2,
}


def _extract_category_from_url(url: str) -> str | None:
    """Return the first matching category for a URL, or None."""
    url_lower = url.lower()
    for category, keywords in CATEGORY_SIGNALS.items():
        if any(kw in url_lower for kw in keywords):
            return category
    return None


def detect_interest_category(events: list[dict[str, Any]]) -> dict[str, Any]:
    """
    Analyse events and return the most likely product interest.

    Each event may contain:
      - event_type: str
      - url: str (optional, for page events)
      - metadata: dict (optional, e.g. video_title, resource_name)

    Returns:
      - category: str | None  ('hypnose' | 'lifecoach' | 'meditation' | None)
      - confidence: float      (0.0 - 1.0)
      - category_scores: dict  (raw scores per category)
    """
    category_scores: dict[str, float] = defaultdict(float)

    for event in events:
        event_type = event.get("event_type", "")
        url = event.get("url", "")
        metadata = event.get("metadata", {}) or {}

        weight = SIGNAL_WEIGHTS.get(event_type, 1)

        # Detect via URL
        if url:
            cat = _extract_category_from_url(url)
            if cat:
                category_scores[cat] += weight

        # Detect via metadata fields (e.g. video_title, resource_name)
        for field in ("video_title", "resource_name", "webinar_title", "page_title"):
            value = metadata.get(field, "")
            if value:
                cat = _extract_category_from_url(value)
                if cat:
                    category_scores[cat] += weight * 0.5  # metadata = half weight

    if not category_scores:
        return {"category": None, "confidence": 0.0, "category_scores": {}}

    top_category = max(category_scores, key=lambda c: category_scores[c])
    total = sum(category_scores.values())
    confidence = round(category_scores[top_category] / total, 2) if total > 0 else 0.0

    return {
        "category": top_category,
        "confidence": confidence,
        "category_scores": dict(category_scores),
    }
