"""
Batch Scorer — Supabase-driven lead scoring pipeline.

Fetches contacts + touchpoints from Supabase (bulk), scores each lead,
applies Do Not Call filters, assigns to funnel lists, and pushes
qualified leads to HubSpot + Aircall with rich card content.

Called by APScheduler in main.py.
"""

import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from batch.do_not_call import DoNotCallResult, check_do_not_call
from integrations.customerio import is_unsubscribed
from integrations.supabase import (
    fetch_touchpoints_for_emails,
    fetch_all_lead_data,
)
from scoring.combined import ScoringResult, combine_scores
from scoring.engagement import calculate_engagement_score
from scoring.hook_engine import generate_hook
from scoring.interest import detect_interest_category
from scoring.touchpoint_mapper import (
    extract_first_last_touch,
    map_touchpoints_batch,
    map_browser_events_batch,
    summarize_email_activity,
)

logger = logging.getLogger(__name__)

HUBSPOT_BASE = "https://api.hubapi.com"
HUBSPOT_TOKEN = os.environ.get("HUBSPOT_ACCESS_TOKEN", "")

# Only re-score leads updated in the last N days to keep API calls low
RESCORE_WINDOW_DAYS = int(os.environ.get("RESCORE_WINDOW_DAYS", "30"))

# Score thresholds
SCORE_WARM = 30      # >= 30 -> push to HubSpot/Aircall
SCORE_HOT = 65       # >= 65 -> same list as warm, tagged hot
FRESH_WINDOW = timedelta(hours=24)

# Funnel list definitions: hubspot_list_id will be populated from env/config
LISTS: dict[str, dict[str, Any]] = {
    "eignungscheck":     {"hubspot_list_id": None, "aircall_tag": "eignungscheck"},
    "hypnose_fresh":     {"hubspot_list_id": None, "aircall_tag": "hc-fresh"},
    "hypnose_warm":      {"hubspot_list_id": None, "aircall_tag": "hc-warm"},
    "meditation_fresh":  {"hubspot_list_id": None, "aircall_tag": "mc-fresh"},
    "meditation_warm":   {"hubspot_list_id": None, "aircall_tag": "mc-warm"},
    "lifecoach_fresh":   {"hubspot_list_id": None, "aircall_tag": "gc-fresh"},
    "lifecoach_warm":    {"hubspot_list_id": None, "aircall_tag": "gc-warm"},
}

# Valid funnels for category mapping
VALID_FUNNELS = {"hypnose", "meditation", "lifecoach"}

# Short display names for Aircall tags (Kevin's naming convention)
FUNNEL_SHORT: dict[str, str] = {
    "hypnose": "HC",       # Hypnose Coach
    "meditation": "MC",     # Meditationscoach
    "lifecoach": "GC",      # Gesprächscoach / Lifecoach
}


# ---------------------------------------------------------------------------
# HubSpot: fetch active leads
# ---------------------------------------------------------------------------

async def _fetch_active_hubspot_leads() -> list[dict[str, Any]]:
    """
    Pull all contacts that have been scored at least once.
    Paginates through HubSpot search results (max 100 per page).
    """
    headers = {
        "Authorization": f"Bearer {HUBSPOT_TOKEN}",
        "Content-Type": "application/json",
    }
    payload: dict[str, Any] = {
        "filterGroups": [
            {
                "filters": [
                    {"propertyName": "lead_tier", "operator": "HAS_PROPERTY"}
                ]
            }
        ],
        "properties": [
            "email", "firstname", "lastname", "phone",
            "lead_engagement_score", "lead_tier",
            "lead_last_call_date", "lead_not_interested",
            "lead_call_booked",
        ],
        "limit": 100,
    }

    results: list[dict[str, Any]] = []
    after: str | None = None

    async with httpx.AsyncClient(timeout=30.0) as client:
        while True:
            if after:
                payload["after"] = after

            resp = await client.post(
                f"{HUBSPOT_BASE}/crm/v3/objects/contacts/search",
                headers=headers,
                json=payload,
            )
            resp.raise_for_status()
            data = resp.json()
            results.extend(data.get("results", []))

            paging = data.get("paging", {}).get("next", {})
            after = paging.get("after")
            if not after:
                break
            # Small delay between pages to avoid HubSpot 429
            await asyncio.sleep(0.5)

    return results


# ---------------------------------------------------------------------------
# HubSpot: shared client + batch update (max 100 per call)
# ---------------------------------------------------------------------------

_HUBSPOT_BATCH_SIZE = 100  # HubSpot batch API limit


async def _batch_update_hubspot_contacts(
    updates: list[dict[str, Any]],
) -> int:
    """
    Batch-update HubSpot contacts via /crm/v3/objects/contacts/batch/update.

    Each item in updates: {"id": contact_id, "properties": {...}}.
    Returns number of successfully updated contacts.
    """
    if not updates:
        return 0

    headers = {
        "Authorization": f"Bearer {HUBSPOT_TOKEN}",
        "Content-Type": "application/json",
    }
    updated = 0

    async with httpx.AsyncClient(timeout=30.0) as client:
        for i in range(0, len(updates), _HUBSPOT_BATCH_SIZE):
            chunk = updates[i:i + _HUBSPOT_BATCH_SIZE]
            try:
                resp = await client.post(
                    f"{HUBSPOT_BASE}/crm/v3/objects/contacts/batch/update",
                    headers=headers,
                    json={"inputs": chunk},
                )
                resp.raise_for_status()
                updated += len(chunk)
                # Delay between batch chunks to avoid HubSpot 429
                if i + _HUBSPOT_BATCH_SIZE < len(updates):
                    await asyncio.sleep(0.5)
            except httpx.HTTPStatusError as e:
                logger.error(
                    "HubSpot batch update failed (chunk %d-%d): %s %s",
                    i, i + len(chunk), e.response.status_code,
                    e.response.text[:500],
                )
            except Exception as e:
                logger.error("HubSpot batch update error: %s", e)

    return updated


# ---------------------------------------------------------------------------
# Scoring pipeline for a single lead
# ---------------------------------------------------------------------------

def _determine_freshness(
    touchpoints: list[dict],
) -> tuple[bool, float]:
    """
    Check if the lead is fresh (first touchpoint within FRESH_WINDOW).

    Returns (is_fresh, hours_since_first_touch).
    """
    if not touchpoints:
        return False, float("inf")

    # Find the earliest touchpoint
    earliest: datetime | None = None
    for tp in touchpoints:
        created_at_raw = tp.get("created_at")
        if not created_at_raw:
            continue
        try:
            ts = datetime.fromisoformat(created_at_raw.replace("Z", "+00:00"))
            if earliest is None or ts < earliest:
                earliest = ts
        except (ValueError, AttributeError):
            continue

    if earliest is None:
        return False, float("inf")

    hours = (datetime.now(timezone.utc) - earliest).total_seconds() / 3600
    return hours < FRESH_WINDOW.total_seconds() / 3600, hours


def _determine_tier_label(score: float, is_fresh: bool) -> str:
    """Human-readable tier label for Aircall card."""
    if is_fresh:
        return "FRESH"
    if score >= SCORE_HOT:
        return "HOT"
    if score >= SCORE_WARM:
        return "WARM"
    return "COLD"


def _determine_list_key(
    funnel: str | None,
    is_fresh: bool,
    score: float,
    qualifies_eignungscheck: bool,
) -> str | None:
    """
    Determine which list a lead belongs to.

    Returns a key from LISTS dict or None if the lead should not be listed.
    """
    if qualifies_eignungscheck:
        return "eignungscheck"

    if funnel not in VALID_FUNNELS:
        return None

    if is_fresh:
        return f"{funnel}_fresh"

    if score >= SCORE_WARM:
        return f"{funnel}_warm"

    # Cold leads (< 30) stay in CIO nurturing only
    return None


def _build_funnel_source(touchpoints: list[dict]) -> str:
    """
    Build a human-readable funnel source string from the first touchpoint.
    Example: "Meta Ad -> Hypnose Masterclass"
    """
    if not touchpoints:
        return ""

    # Use the first touch if flagged, otherwise earliest by date
    first_touch = None
    for tp in touchpoints:
        if tp.get("is_first_touch"):
            first_touch = tp
            break

    if first_touch is None and touchpoints:
        first_touch = touchpoints[-1]  # touchpoints are desc by created_at

    if first_touch is None:
        return ""

    channel = first_touch.get("channel", "")
    campaign = first_touch.get("campaign", "")
    content = first_touch.get("content", "")

    # Build readable source: "Meta Ad -> Campaign Name"
    channel_label = {
        "meta_ads": "Meta Ad",
        "google_ads": "Google Ad",
        "email": "Email",
        "direct": "Direct",
    }.get(channel, channel.replace("_", " ").title() if channel else "Unknown")

    detail = campaign or content or ""
    if detail:
        return f"{channel_label} -> {detail}"
    return channel_label


def _format_touch_summary(tp: dict | None) -> str:
    """Format a touchpoint as 'channel | YYYY-MM-DD' for HubSpot card."""
    if not tp:
        return ""
    channel = tp.get("channel", "unknown")
    tp_type = tp.get("touchpoint_type", "")
    created_at = tp.get("created_at", "")

    # Parse date portion
    date_str = ""
    if created_at:
        try:
            dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
            date_str = dt.strftime("%Y-%m-%d")
        except (ValueError, AttributeError):
            date_str = created_at[:10] if len(created_at) >= 10 else created_at

    label = tp_type or channel
    return f"{label} | {date_str}" if date_str else label


def _build_hubspot_card_properties(
    scoring: ScoringResult,
    funnel: str | None,
    funnel_source: str,
    first_touch: dict | None,
    last_touch: dict | None,
    purchased_funnels: list[str],
    multi_funnel_info: str,
) -> dict[str, Any]:
    """Build the HubSpot properties dict for the lead card."""
    now_iso = datetime.now(timezone.utc).isoformat()

    return {
        "lead_tier": scoring.lead_tier,
        "lead_combined_score": scoring.combined_score,
        "lead_interest_category": funnel or "",
        "lead_funnel_source": funnel_source,
        "lead_first_touch": _format_touch_summary(first_touch),
        "lead_last_touch": _format_touch_summary(last_touch),
        "lead_purchased_products": ", ".join(purchased_funnels) if purchased_funnels else "",
        "lead_multi_funnel": multi_funnel_info,
        "lead_score_updated_at": now_iso,
        "lead_engagement_score": scoring.engagement_score,
    }


def _build_aircall_card(
    tier_label: str,
    funnel: str | None,
    score: float,
    last_call_date: str | None,
    email_summary: dict,
    first_touch: dict | None,
    last_touch: dict | None,
    hook: str,
    purchased_funnels: list[str] | None = None,
) -> str:
    """
    Build the Aircall card info string for the closer.

    Example output:
        WARM -- Hypnose | Score: 72
        Letzter Call: 05.03
        3x opened, 1x clicked (14d)
        First: Meta Ad | Last: Email Click
        Kunde: Meditation
        Hook: Du hast das Video fast komplett geschaut...
        → Ziel: Termin buchen
    """
    funnel_label = (funnel or "unknown").title()
    lines = [
        f"WARM -- {funnel_label} | Score: {score:.0f}"
        if tier_label == "WARM" else
        f"{tier_label} -- {funnel_label} | Score: {score:.0f}",
    ]

    # Last call info
    if last_call_date:
        try:
            dt = datetime.fromisoformat(last_call_date.replace("Z", "+00:00"))
            lines.append(f"Letzter Call: {dt.strftime('%d.%m')}")
        except (ValueError, AttributeError):
            lines.append(f"Letzter Call: {last_call_date}")
    else:
        lines.append("Letzter Call: keiner")

    # Email activity
    opens = email_summary.get("opens", 0)
    clicks = email_summary.get("clicks", 0)
    lines.append(f"{opens}x opened, {clicks}x clicked (14d)")

    # First/last touch
    first_label = (first_touch or {}).get("channel", "?")
    last_label = (last_touch or {}).get("touchpoint_type", "") or (last_touch or {}).get("channel", "?")
    lines.append(f"First: {first_label} | Last: {last_label}")

    # Existing customer info — critical for the closer to know
    if purchased_funnels:
        lines.append(f"Kunde: {', '.join(f.title() for f in purchased_funnels)}")

    # Hook
    lines.append(f"Hook: {hook}")

    # Call-to-action per tier — tells Kevin what to aim for
    cta = {
        "FRESH": "Eignungscheck pitchen",
        "HOT": "Abschluss machen",
        "WARM": "Termin buchen",
        "COLD": "Interesse wecken",
    }.get(tier_label, "Interesse wecken")
    lines.append(f"\u2192 Ziel: {cta}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main batch job
# ---------------------------------------------------------------------------

async def run_batch_scoring() -> None:
    """
    Main batch job: fetch leads from HubSpot, enrich with Supabase touchpoints
    and CIO segment data, score, filter, assign to lists, push qualified leads.
    """
    logger.info("Batch scoring: starting run")

    # Step 1: Fetch active HubSpot leads
    try:
        leads = await _fetch_active_hubspot_leads()
    except Exception as e:
        logger.error("Batch: failed to fetch HubSpot leads: %s", e)
        return

    if not leads:
        logger.info("Batch scoring: no leads to process")
        return

    logger.info("Batch: %d leads to re-score", len(leads))

    # Collect all emails for bulk operations
    email_lead_map: dict[str, dict[str, Any]] = {}
    for contact in leads:
        props = contact.get("properties", {})
        email = props.get("email", "")
        if email and contact.get("id"):
            email_lead_map[email] = contact

    all_emails = list(email_lead_map.keys())
    if not all_emails:
        logger.info("Batch scoring: no valid emails found")
        return

    # Step 2: Bulk fetch touchpoints + events + purchases + meetings from Supabase
    try:
        touchpoints_by_email = await fetch_touchpoints_for_emails(
            all_emails, days=RESCORE_WINDOW_DAYS,
        )
    except Exception as e:
        logger.error("Batch: Supabase touchpoint fetch failed: %s", e)
        return

    # Fetch events + purchases + meetings in one consolidated call (single contacts query)
    try:
        all_lead_data = await fetch_all_lead_data(all_emails, days=RESCORE_WINDOW_DAYS)
    except Exception as exc:
        logger.warning("Batch: Supabase lead data fetch failed: %s", exc)
        all_lead_data = {em: {"events": [], "purchases": [], "meetings": []} for em in all_emails}

    # Step 3: Score each contact — collect HubSpot updates for batch push
    hubspot_updates: list[dict[str, Any]] = []  # {"id": ..., "properties": {...}}
    aircall_queue: list[dict[str, Any]] = []     # leads to push to Aircall
    skipped_dnc = 0
    skipped_cold = 0

    now_utc = datetime.now(timezone.utc)

    for email, contact in email_lead_map.items():
        props = contact.get("properties", {})
        contact_id = contact["id"]
        touchpoints = touchpoints_by_email.get(email, [])
        lead_data = all_lead_data.get(email, {
            "events": [], "purchases": [], "meetings": [], "customerio_id": None,
        })
        browser_events = lead_data["events"]
        purchases = lead_data["purchases"]
        meetings = lead_data["meetings"]
        # customerio_id from bulk Supabase lookup — no per-lead query needed
        cio_id = lead_data.get("customerio_id")

        # Derive purchased funnels from Supabase purchases (product_key -> funnel)
        purchased_funnels = _extract_purchased_funnels(purchases)

        try:
            # Map touchpoints to scored events
            scored_events = map_touchpoints_batch(touchpoints)

            # Map browser events (offer views, videos, checkout) and merge
            browser_scored = map_browser_events_batch(browser_events)
            scored_events.extend(browser_scored)

            # Calculate engagement score
            engagement_result = calculate_engagement_score(scored_events)

            # Detect interest category
            interest_result = detect_interest_category(scored_events)

            # Combine scores
            scoring = combine_scores(engagement_result, interest_result)
            score = scoring.combined_score
            funnel = scoring.interest_category

            # Determine freshness
            is_fresh, fresh_hours = _determine_freshness(touchpoints)
            tier_label = _determine_tier_label(score, is_fresh)

            # Extract first/last touch
            first_touch, last_touch = extract_first_last_touch(touchpoints)

            # Email activity summary (14 days)
            email_summary = summarize_email_activity(touchpoints, days=14)

            # Build funnel source string
            funnel_source = _build_funnel_source(touchpoints)

            # Check for multi-funnel interest
            cat_scores = interest_result.get("category_scores", {})
            multi_funnels = [f for f in VALID_FUNNELS if cat_scores.get(f, 0) > 0]
            multi_funnel_info = ", ".join(multi_funnels) if len(multi_funnels) > 1 else ""

            # Unsubscribed check via Customer.io (uses pre-fetched cio_id)
            unsubscribed = False
            if cio_id:
                try:
                    unsubscribed = await is_unsubscribed(cio_id)
                except Exception as e:
                    logger.warning("Batch: CIO unsubscribed check failed for %s: %s", email, e)

            last_call_date = props.get("lead_last_call_date")
            # Check call_booked: only future/recent scheduled meetings count
            has_scheduled_meeting = False
            for m in meetings:
                if m.get("status") != "scheduled":
                    continue
                sched_raw = m.get("scheduled_at") or ""
                try:
                    sched_dt = datetime.fromisoformat(sched_raw.replace("Z", "+00:00"))
                    if sched_dt >= now_utc - timedelta(hours=24):
                        has_scheduled_meeting = True
                        break
                except (ValueError, AttributeError):
                    continue
            call_booked = has_scheduled_meeting or _truthy(props.get("lead_call_booked"))
            not_interested = _truthy(props.get("lead_not_interested"))
            has_phone = bool(props.get("phone"))

            dnc_result = await check_do_not_call(
                email=email,
                funnel=funnel or "",
                cio_id=cio_id,
                hubspot_contact_id=contact_id,
                last_call_date=last_call_date,
                call_booked=call_booked,
                not_interested=not_interested,
                unsubscribed=unsubscribed,
                purchased_funnels=purchased_funnels,
            )

            # Eignungscheck qualification
            qualifies_eignungscheck = (
                has_phone
                and not call_booked
                and not unsubscribed
                and not not_interested
                and funnel is not None
                and funnel not in purchased_funnels
            )

            # Determine list assignment — ONLY leads with phone go to HubSpot/Aircall
            should_push = has_phone and (is_fresh or score >= SCORE_WARM)
            list_key = _determine_list_key(funnel, is_fresh, score, qualifies_eignungscheck)

            if not has_phone:
                logger.debug("Batch: skip %s — no phone number", email)
                skipped_cold += 1
                continue

            if dnc_result.should_skip:
                skipped_dnc += 1
                logger.debug("Batch: DNC skip %s — %s", email, dnc_result.reason)
                should_push = False

            if not should_push and not dnc_result.should_skip:
                skipped_cold += 1

            # Build HubSpot card properties
            hs_properties = _build_hubspot_card_properties(
                scoring=scoring,
                funnel=funnel,
                funnel_source=funnel_source,
                first_touch=first_touch,
                last_touch=last_touch,
                purchased_funnels=purchased_funnels,
                multi_funnel_info=multi_funnel_info,
            )

            # Collect for batch HubSpot update (instead of per-lead PATCH)
            hubspot_updates.append({"id": contact_id, "properties": hs_properties})

            # Queue Aircall push if qualified and not DNC
            if should_push and list_key:
                # Derive offer/video signals from browser events for hook
                offer_signals = _extract_offer_signals(browser_events)

                hook_context = {
                    "email_clicked": email_summary.get("clicks", 0) > 0,
                    "last_email_subject": email_summary.get("last_email_subject", ""),
                    "is_fresh": is_fresh,
                    "fresh_hours": fresh_hours,
                    "funnel": funnel,
                    "score": score,
                    "eignungscheck": qualifies_eignungscheck,
                    "call_booked": call_booked,
                    "purchased_products": purchased_funnels,
                    "visited_offer_page": offer_signals.get("visited_offer"),
                    "visited_checkout": offer_signals.get("visited_checkout"),
                    "watched_video_on_offer": offer_signals.get("video_on_offer"),
                    "viewed_pricing": offer_signals.get("viewed_pricing"),
                }
                hook = generate_hook(hook_context)

                aircall_card = _build_aircall_card(
                    tier_label=tier_label,
                    funnel=funnel,
                    score=score,
                    last_call_date=last_call_date,
                    email_summary=email_summary,
                    first_touch=first_touch,
                    last_touch=last_touch,
                    hook=hook,
                    purchased_funnels=purchased_funnels,
                )

                aircall_queue.append({
                    "email": email,
                    "list_key": list_key,
                    "score": score,
                    "tier_label": tier_label,
                    "funnel": funnel,
                    "lead_tier": scoring.lead_tier,
                    "phone": props.get("phone", ""),
                    "firstname": props.get("firstname", ""),
                    "lastname": props.get("lastname", ""),
                    "aircall_card": aircall_card,
                })

        except Exception as e:
            logger.error("Batch: failed to score %s: %s", email, e)

    # Step 4: Batch-update HubSpot (100 per API call instead of 1-by-1)
    updated = await _batch_update_hubspot_contacts(hubspot_updates)

    # Step 5: Push qualified leads to Aircall
    pushed = 0
    for item in aircall_queue:
        try:
            from integrations.aircall import add_to_power_dialer
            if item["phone"]:
                lead_dict = {
                    "phone": item["phone"],
                    "firstname": item["firstname"],
                    "lastname": item["lastname"],
                    "email": item["email"],
                    "notes": item["aircall_card"],
                }
                await add_to_power_dialer(
                    lead_dict,
                    score=item["score"],
                    interest_category=item["funnel"],
                    lead_tier=item["lead_tier"],
                    list_key=item["list_key"],
                )
                pushed += 1
                logger.info(
                    "Batch: pushed %s to Aircall [%s] score=%.0f tier=%s",
                    item["email"], item["list_key"], item["score"], item["tier_label"],
                )
        except Exception as e:
            logger.error("Batch: Aircall push failed for %s: %s", item["email"], e)

    logger.info(
        "Batch scoring: done — %d/%d updated, %d pushed, %d DNC-skipped, %d cold-skipped",
        updated, len(email_lead_map), pushed, skipped_dnc, skipped_cold,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# Supabase product_key -> funnel mapping
_PRODUCT_KEY_TO_FUNNEL: dict[str, str] = {
    "gc": "lifecoach",
    "mc": "meditation",
    "hc": "hypnose",
    "afk": "hypnose",  # Angstfrei event is under hypnose umbrella
}


def _extract_purchased_funnels(purchases: list[dict]) -> list[str]:
    """
    Derive purchased funnel names from Supabase purchases.

    Maps product_key (gc/mc/hc/afk) to funnel names.
    Returns deduplicated list of funnel names.
    """
    funnels: set[str] = set()
    for p in purchases:
        pk = (p.get("product_key") or "").lower()
        funnel = _PRODUCT_KEY_TO_FUNNEL.get(pk)
        if funnel:
            funnels.add(funnel)
    return list(funnels)


def _extract_offer_signals(browser_events: list[dict]) -> dict[str, bool]:
    """
    Check browser events for high-intent offer/checkout/video signals.

    Returns dict with boolean flags for hook generation.
    """
    signals = {
        "visited_offer": False,
        "visited_checkout": False,
        "video_on_offer": False,
        "viewed_pricing": False,
    }

    offer_patterns = ("/offer", "/angebot")
    checkout_patterns = ("/checkout", "/bezahlen", "/payment", "/order")
    price_patterns = ("/kosten", "/preise", "/pricing", "/kosten-termine")

    for ev in browser_events:
        url = (ev.get("page_url") or "").lower()
        etype = (ev.get("event_type") or "").lower()

        if any(p in url for p in checkout_patterns):
            signals["visited_checkout"] = True
        if any(p in url for p in offer_patterns):
            signals["visited_offer"] = True
            if etype in ("video_play", "video_complete", "video_progress"):
                signals["video_on_offer"] = True
        if any(p in url for p in price_patterns):
            signals["viewed_pricing"] = True

    return signals


def _truthy(value: Any) -> bool:
    """Convert HubSpot property values to boolean (handles 'true'/'false' strings)."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ("true", "1", "yes")
    return bool(value)
