"""
Touchpoint Mapper — Maps Supabase touchpoints to internal event types.

Converts raw touchpoint records from the Supabase touchpoints table
into normalized event dicts consumed by the engagement scorer.
"""

from datetime import datetime, timedelta, timezone
from typing import Optional


# Mapping rules: (channel, source_prefix, touchpoint_type) -> internal event_type
# Source prefix allows matching "customerio" and "cio" separately.
# Use "*" as wildcard for any value.
_MAPPING_RULES: list[tuple[str | None, str | None, str | None, str]] = [
    # Email events from Customer.io
    ("email", "customerio", "opened", "email_opened"),
    ("email", "customerio", "clicked", "email_link_clicked"),
    ("email", "cio", "email_action", "email_link_clicked"),

    # Direct form submissions
    ("direct", "*", "form_submit", "application_submitted"),

    # Ad clicks (channel-level)
    ("meta_ads", "*", "*", "cta_clicked"),
    ("google_ads", "*", "*", "cta_clicked"),

    # Ad click by touchpoint_type regardless of channel
    ("*", "*", "ad_click", "cta_clicked"),
]


def _matches(rule_value: str | None, actual_value: str | None) -> bool:
    """Check if a rule value matches the actual touchpoint field value."""
    if rule_value == "*" or rule_value is None:
        return True
    if actual_value is None:
        return False
    return actual_value.lower() == rule_value.lower()


def _resolve_event_type(channel: str | None, source: str | None,
                        touchpoint_type: str | None) -> str | None:
    """
    Walk through mapping rules in priority order and return the first
    matching internal event type, or None if no rule matches.
    """
    for rule_channel, rule_source, rule_type, event_type in _MAPPING_RULES:
        if (_matches(rule_channel, channel)
                and _matches(rule_source, source)
                and _matches(rule_type, touchpoint_type)):
            return event_type
    return None


def map_touchpoint_to_event(touchpoint: dict) -> dict | None:
    """
    Convert a Supabase touchpoint to an internal scored event.

    Returns a dict with ``event_type`` and ``timestamp`` keys,
    or None if the touchpoint doesn't map to any scored event type.
    """
    event_type = _resolve_event_type(
        channel=touchpoint.get("channel"),
        source=touchpoint.get("source"),
        touchpoint_type=touchpoint.get("touchpoint_type"),
    )
    if event_type is None:
        return None

    return {
        "event_type": event_type,
        "timestamp": touchpoint.get("created_at"),
    }


def map_touchpoints_batch(touchpoints: list[dict]) -> list[dict]:
    """
    Map a list of touchpoints, filtering out unmappable ones.

    Returns only those touchpoints that successfully map to an internal
    event type.
    """
    events: list[dict] = []
    for tp in touchpoints:
        event = map_touchpoint_to_event(tp)
        if event is not None:
            events.append(event)
    return events


# ---------------------------------------------------------------------------
# Browser event mapping (Supabase events table)
# ---------------------------------------------------------------------------

# URL patterns that indicate high-intent pages
_OFFER_URL_PATTERNS = ("/offer", "/angebot")
_CHECKOUT_URL_PATTERNS = ("/checkout", "/bezahlen", "/payment", "/order")
_PRICE_URL_PATTERNS = ("/kosten", "/preise", "/pricing", "/kosten-termine")


def _classify_page_url(url: str | None) -> str | None:
    """Classify a page URL into an intent category."""
    if not url:
        return None
    url_lower = url.lower()
    for pattern in _CHECKOUT_URL_PATTERNS:
        if pattern in url_lower:
            return "checkout"
    for pattern in _OFFER_URL_PATTERNS:
        if pattern in url_lower:
            return "offer"
    for pattern in _PRICE_URL_PATTERNS:
        if pattern in url_lower:
            return "price"
    return None


def map_browser_event(event: dict) -> dict | None:
    """
    Convert a Supabase browser event to an internal scored event.

    Handles: pageview, click, video_play, video_complete, video_progress,
    scroll, form_submit on high-intent pages (offer, checkout, pricing).

    Returns a dict with event_type + timestamp, or None if not scoreable.
    """
    event_type = (event.get("event_type") or "").lower()
    page_url = event.get("page_url") or ""
    page_class = _classify_page_url(page_url)
    props = event.get("event_properties") or {}

    scored_type: str | None = None

    if event_type == "pageview":
        if page_class == "checkout":
            scored_type = "checkout_visited"
        elif page_class == "offer":
            scored_type = "sales_page_visited"
        elif page_class == "price":
            scored_type = "price_info_viewed"
        else:
            scored_type = "page_visited"

    elif event_type == "click":
        if page_class in ("offer", "checkout"):
            scored_type = "cta_clicked"

    elif event_type == "video_complete":
        scored_type = "video_watched_100"

    elif event_type == "video_progress":
        # Support multiple field names: percent_complete (CIO), depth, progress (older versions)
        depth = (
            props.get("percent_complete")
            or props.get("depth")
            or props.get("progress")
            or 0
        )
        if isinstance(depth, str):
            try:
                depth = int(depth.replace("%", ""))
            except ValueError:
                depth = 0
        if depth >= 75:
            scored_type = "video_watched_75"
        elif depth >= 50:
            scored_type = "video_watched_50"

    elif event_type == "video_play":
        # Video started on offer/checkout page = strong signal
        if page_class in ("offer", "checkout"):
            scored_type = "video_watched_50"  # conservative: started != watched
        else:
            scored_type = "page_visited"  # minimal credit for non-offer video

    elif event_type == "form_submit":
        scored_type = "application_submitted"

    elif event_type == "scroll":
        depth = props.get("depth") or 0
        if isinstance(depth, str):
            try:
                depth = int(depth.replace("%", ""))
            except ValueError:
                depth = 0
        # Deep scroll on offer page = interest signal
        if page_class in ("offer", "checkout") and depth >= 66:
            scored_type = "page_visited"

    if scored_type is None:
        return None

    result = {
        "event_type": scored_type,
        "timestamp": event.get("created_at"),
    }
    # Pass URL through so detect_interest_category() can determine funnel
    if page_url:
        result["url"] = page_url
    return result


def map_browser_events_batch(events: list[dict]) -> list[dict]:
    """
    Map a list of browser events, filtering out non-scoreable ones.

    Returns scored event dicts ready for the engagement scorer.
    """
    scored: list[dict] = []
    for ev in events:
        mapped = map_browser_event(ev)
        if mapped is not None:
            scored.append(mapped)
    return scored


def extract_first_last_touch(
    touchpoints: list[dict],
) -> tuple[dict | None, dict | None]:
    """
    Extract first touch and last touch from touchpoints using the
    ``is_first_touch`` / ``is_last_touch`` flags set by Supabase.

    Returns a (first_touch, last_touch) tuple.  Either value can be
    None if no touchpoint carries the corresponding flag.
    """
    first_touch: dict | None = None
    last_touch: dict | None = None

    for tp in touchpoints:
        if tp.get("is_first_touch"):
            first_touch = tp
        if tp.get("is_last_touch"):
            last_touch = tp

    return first_touch, last_touch


def summarize_email_activity(
    touchpoints: list[dict],
    days: int = 14,
    scored_events: list[dict] | None = None,
) -> dict:
    """
    Summarize email activity within the last *days* for card display.

    Checks TWO sources:
    1. Touchpoints table (channel=email, touchpoint_type=opened/clicked)
    2. Scored events (event_type=email_opened/email_link_clicked)

    This dual-source approach handles both CIO webhook events (which land
    in the events table) and Supabase touchpoints (from attribution tracking).

    Returns::

        {
            "opens": int,
            "clicks": int,
            "last_email_subject": str
        }
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    opens = 0
    clicks = 0
    last_email_subject: str = ""
    last_email_time: Optional[datetime] = None

    # Source 1: Touchpoints table (channel=email)
    for tp in touchpoints:
        if (tp.get("channel") or "").lower() != "email":
            continue

        created_at_raw = tp.get("created_at")
        if not created_at_raw:
            continue

        try:
            created_at = datetime.fromisoformat(created_at_raw.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            continue

        if created_at < cutoff:
            continue

        tp_type = (tp.get("touchpoint_type") or "").lower()
        if tp_type == "opened":
            opens += 1
        elif tp_type in ("clicked", "email_action"):
            clicks += 1

        if last_email_time is None or created_at > last_email_time:
            last_email_time = created_at
            last_email_subject = tp.get("content") or ""

    # Source 2: Scored events (from CIO webhooks / browser events)
    if scored_events:
        for ev in scored_events:
            et = (ev.get("event_type") or "").lower()
            if et not in ("email_opened", "email_link_clicked"):
                continue

            ts_raw = ev.get("timestamp")
            if not ts_raw:
                continue

            try:
                ts = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                continue

            if ts < cutoff:
                continue

            if et == "email_opened":
                opens += 1
            elif et == "email_link_clicked":
                clicks += 1

    return {
        "opens": opens,
        "clicks": clicks,
        "last_email_subject": last_email_subject,
    }
