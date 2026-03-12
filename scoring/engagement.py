"""
Engagement Score Calculator
B2C Behaviour-based scoring via Customer.io events.
Spec: Schneider Business Consulting, March 2026
"""

from collections import Counter
from datetime import datetime, timezone
from typing import Any

# ---------------------------------------------------------------------------
# Base points per Customer.io event type
# ---------------------------------------------------------------------------
BASE_POINTS: dict[str, int] = {
    "checkout_visited":         40,
    "application_submitted":    35,
    "video_watched_100":        30,   # watched complete — strong commitment signal
    "video_watched_75":         25,
    "webinar_attended":         25,
    "sales_page_visited":       20,
    "price_info_viewed":        20,
    "webinar_registered":       15,
    "video_watched_50":         15,
    "cta_clicked":              12,   # CTA click on sales page
    "email_link_clicked":       10,
    "free_resource_downloaded": 10,
    "email_opened":              5,
    "page_visited":              3,
}

# ---------------------------------------------------------------------------
# Recency multipliers — based on days since event
# ---------------------------------------------------------------------------
def recency_multiplier(days_ago: float) -> float:
    if days_ago <= 7:
        return 1.0
    elif days_ago <= 14:
        return 0.7
    elif days_ago <= 30:
        return 0.4
    else:
        return 0.1


# ---------------------------------------------------------------------------
# Inactivity + unsubscribe malus
# ---------------------------------------------------------------------------
# Maximum number of times the same event type is counted (prevents score inflation)
MAX_EVENTS_PER_TYPE = 3


def inactivity_malus(days_since_last_activity: float, unsubscribed: bool) -> int:
    malus = 0
    if unsubscribed:
        malus -= 50
    elif days_since_last_activity > 30:
        malus -= 30
    elif days_since_last_activity > 14:
        malus -= 15
    return malus


# ---------------------------------------------------------------------------
# Main engagement score calculation
# ---------------------------------------------------------------------------
def calculate_engagement_score(events: list[dict[str, Any]]) -> dict[str, Any]:
    """
    Calculate engagement score from a list of Customer.io events.

    Each event must contain:
      - event_type: str  (mapped Customer.io event name, see EVENT_MAP in main.py)
      - timestamp: str   (ISO 8601, e.g. '2026-03-05T14:30:00Z')

    Returns dict with:
      - score: int (0-100, clamped)
      - raw_score: float (before clamping)
      - event_breakdown: list of scored events
      - days_since_last_activity: float
    """
    now = datetime.now(timezone.utc)

    raw_score = 0.0
    breakdown = []
    last_activity_ts: datetime | None = None
    type_counts: Counter = Counter()

    for event in events:
        event_type = event.get("event_type", "")
        ts_str = event.get("timestamp", "")

        if event_type not in BASE_POINTS:
            continue

        # Cap: max N events of same type to prevent score inflation
        type_counts[event_type] += 1
        if type_counts[event_type] > MAX_EVENTS_PER_TYPE:
            continue

        try:
            ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            continue

        days_ago = (now - ts).total_seconds() / 86400
        multiplier = recency_multiplier(days_ago)
        points = BASE_POINTS[event_type] * multiplier

        raw_score += points
        breakdown.append({
            "event_type": event_type,
            "base_points": BASE_POINTS[event_type],
            "days_ago": round(days_ago, 1),
            "multiplier": multiplier,
            "points_awarded": round(points, 2),
        })

        if last_activity_ts is None or ts > last_activity_ts:
            last_activity_ts = ts

    days_since_last = (
        (now - last_activity_ts).total_seconds() / 86400
        if last_activity_ts else 999.0
    )

    unsubscribed = any(e.get("event_type") == "email_unsubscribed" for e in events)
    malus = inactivity_malus(days_since_last, unsubscribed)
    raw_score += malus

    score = max(min(round(raw_score), 100), -100)  # allow negative for Disqualified tier

    return {
        "score": score,
        "raw_score": round(raw_score, 2),
        "event_breakdown": breakdown,
        "inactivity_malus": malus,
        "days_since_last_activity": round(days_since_last, 1),
        "unsubscribed": unsubscribed,
    }
