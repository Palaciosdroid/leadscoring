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
# Bonus points for purchased products — signals buying intent
# Added before decay so that purchase history raises the base score.
# Only entry-level / bundle products count; full Ausbildung buyers are
# excluded from calling lists anyway (handled in scorer.py).
# ---------------------------------------------------------------------------
PURCHASE_BONUS: dict[str, int] = {
    "inner_journey": 20,  # Inner Journey bundle — strong intent signal
    "bootcamp":       15, # Bootcamp participant
    "afk":            10, # Aktiv-Formel Kurs
}


def _purchase_bonus(purchased_products: list[str]) -> int:
    """Return total bonus points for a lead's purchase history."""
    total = 0
    seen: set[str] = set()
    for product in purchased_products:
        product_lower = product.lower()
        if product_lower in seen:
            continue  # deduplicate identical product entries
        seen.add(product_lower)
        for key, pts in PURCHASE_BONUS.items():
            # Match both underscore form ("inner_journey") and space form ("inner journey")
            if key in product_lower or key.replace("_", " ") in product_lower:
                total += pts
                break  # one bonus per product, no double-counting
    return total


# ---------------------------------------------------------------------------
# Recency multipliers — based on days since event
# ---------------------------------------------------------------------------
def recency_multiplier(days_ago: float) -> float:
    if days_ago <= 1:
        return 1.5       # same-day / yesterday = strong boost (was 1.3 for <=3d)
    elif days_ago <= 3:
        return 1.3       # very recent = boost
    elif days_ago <= 7:
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
MAX_EVENTS_PER_TYPE = 5


def inactivity_malus(days_since_last_activity: float, unsubscribed: bool) -> int:
    """Flat point deduction for unsubscribe only.

    Inactivity is now handled by inactivity_decay_factor() (multiplicative).
    The flat -30/-15 malus was removed to avoid double-penalizing inactive leads.
    Unsubscribe malus (-50) remains as a hard signal.
    """
    if unsubscribed:
        return -50
    return 0


def inactivity_decay_factor(days_since_last_activity: float) -> float:
    """
    Multiplicative decay factor based on inactivity.

    Applied to the total score AFTER event scoring + malus.
    This ensures that leads who haven't engaged in months
    don't stay at score 100 forever.

    Decay schedule:
      - 0-30 days:  no decay (1.0)
      - 31-60 days: mild decay (0.7)
      - 61-90 days: strong decay (0.5)
      - 91+ days:   heavy decay (0.25)
    """
    if days_since_last_activity <= 30:
        return 1.0
    elif days_since_last_activity <= 60:
        return 0.7
    elif days_since_last_activity <= 90:
        return 0.5
    else:
        return 0.25


# ---------------------------------------------------------------------------
# Main engagement score calculation
# ---------------------------------------------------------------------------
def calculate_engagement_score(
    events: list[dict[str, Any]],
    purchased_products: list[str] | None = None,
) -> dict[str, Any]:
    """
    Calculate engagement score from a list of Customer.io events.

    Each event must contain:
      - event_type: str  (mapped Customer.io event name, see EVENT_MAP in main.py)
      - timestamp: str   (ISO 8601, e.g. '2026-03-05T14:30:00Z')

    purchased_products: list of product keys from Supabase (e.g. ["hc", "inner_journey"]).
    Adds bonus points for entry-level purchases (see PURCHASE_BONUS).

    Returns dict with:
      - score: int (0-100, clamped)
      - raw_score: float (before clamping)
      - event_breakdown: list of scored events
      - days_since_last_activity: float
      - purchase_bonus: int
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

    # Purchase bonus — added before decay so history raises base score
    purchase_pts = _purchase_bonus(purchased_products or [])
    raw_score += purchase_pts

    # Apply inactivity decay — only when there are actual engagement events.
    # Purchase bonus is a pure intent signal and should not be decayed by inactivity.
    decay = inactivity_decay_factor(days_since_last) if last_activity_ts else 1.0
    if decay < 1.0:
        raw_score = raw_score * decay

    score = max(min(round(raw_score), 200), -100)  # cap at 200 for better differentiation

    return {
        "score": score,
        "raw_score": round(raw_score, 2),
        "event_breakdown": breakdown,
        "inactivity_malus": malus,
        "purchase_bonus": purchase_pts,
        "days_since_last_activity": round(days_since_last, 1),
        "unsubscribed": unsubscribed,
    }
