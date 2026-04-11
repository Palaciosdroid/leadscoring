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

from batch.do_not_call import check_do_not_call
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
from integrations.slack import send_decay_alert, send_batch_report, BatchRunStats

logger = logging.getLogger(__name__)

HUBSPOT_BASE = "https://api.hubapi.com"
HUBSPOT_TOKEN = os.environ.get("HUBSPOT_ACCESS_TOKEN", "")

# Only re-score leads updated in the last N days to keep API calls low
RESCORE_WINDOW_DAYS = int(os.environ.get("RESCORE_WINDOW_DAYS", "14"))

# Score thresholds (reverted to v1 — v2 thresholds were too aggressive,
# caused 152 leads to drop from Warm to Cold and disappear from Aircall)
SCORE_WARM = 30      # >= 30 -> push to HubSpot/Aircall
SCORE_HOT = 65       # >= 65 -> same list as warm, tagged hot
FRESH_WINDOW = timedelta(days=7)  # Wave 4: was 72h, now 7d
FRESH_MIN_SCORE = 10  # fresh leads need at least this to enter Aircall (avoids single page_visited)

# Human-readable tier labels for dormant leads (use stored old_tier value)
_OLD_TIER_LABELS: dict[str, str] = {"1_hot": "HOT", "2_warm": "WARM"}

# Cooldown after call — prevent Kevin from calling the same person repeatedly
COOLDOWN_ANSWERED_HOURS = 7 * 24      # 7 days after answered call
COOLDOWN_NO_ANSWER_HOURS = 3 * 24     # 3 days after no-answer
COOLDOWN_VOICEMAIL_HOURS = 3 * 24     # 3 days after voicemail

# Call outcomes that permanently remove a lead from calling lists
# These map to HubSpot lead_last_call_outcome values (written by call_poller.py)
PERMANENT_REMOVE_OUTCOMES: frozenset[str] = frozenset({
    "Falsche Nummer",           # wrong number — never call again
    "nicht_qualifiziert",       # Kevin marked as not qualified
    "nicht qualifiziert",       # alternate spelling
    "disqualified",             # English variant
    "abgesagt",                 # cancelled consultation
    "Beratungsgespräch abgesagt",
})

# Max call attempts before giving up on a lead
MAX_CALL_ATTEMPTS = 5  # after 5 unanswered attempts → remove from queue

# Aircall priority order — lower number = called first
AIRCALL_PRIORITY = {
    "eignungscheck": 1,
    "hypnose_fresh": 2, "meditation_fresh": 2, "lifecoach_fresh": 2,
    "hypnose_warm": 3, "meditation_warm": 3, "lifecoach_warm": 3,
}

# Funnel list definitions — HubSpot list IDs confirmed 2026-03-13
# Warm lists: lead_interest_category=X AND lead_tier IN [1_hot,2_warm]; Fresh: lead_is_fresh=true AND lead_interest_category=X
LISTS: dict[str, dict[str, Any]] = {
    # --- Calling lists (Kevin's pipeline) ---
    "eignungscheck":     {"hubspot_list_id": 352, "aircall_tag": "eignungscheck"},
    "hypnose_fresh":     {"hubspot_list_id": 368, "aircall_tag": "hc-fresh"},
    "hypnose_warm":      {"hubspot_list_id": 365, "aircall_tag": "hc-warm"},
    "meditation_fresh":  {"hubspot_list_id": 369, "aircall_tag": "mc-fresh"},
    "meditation_warm":   {"hubspot_list_id": 366, "aircall_tag": "mc-warm"},
    "lifecoach_fresh":   {"hubspot_list_id": 370, "aircall_tag": "gc-fresh"},
    "lifecoach_warm":    {"hubspot_list_id": 367, "aircall_tag": "gc-warm"},
    # --- Käufer-Listen (observation only, NOT routed to Kevin) ---
    # Entry-level / event product buyers. No aircall_tag — these do not go to the dialer.
    "bf_kaeufer":        {"hubspot_list_id": 362, "aircall_tag": ""},
    "tfmw_kaeufer":      {"hubspot_list_id": 363, "aircall_tag": ""},
    "med_kaeufer":       {"hubspot_list_id": 364, "aircall_tag": ""},
}

# Valid funnels for category mapping
VALID_FUNNELS = {"hypnose", "meditation", "lifecoach"}

# Only the main Ausbildung products count as "already a customer" and trigger
# exclusion from calling lists. Entry-level products (afk, tfmw, bf) are
# high-intent signals but NOT customer exclusions — those leads can still be
# called for the full Ausbildung upgrade.
_AUSBILDUNG_KEYS: frozenset[str] = frozenset({"hc", "mc", "gc"})

# Product name patterns that indicate entry-level bundle purchases.
# These do NOT count as "customer" exclusions — the lead can still be called.
_BUNDLE_PRODUCT_PATTERNS: frozenset[str] = frozenset({
    "inner journey",
    "meditationspaket",
})

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
            "email", "firstname", "lastname", "phone", "mobilephone",
            "lead_engagement_score", "lead_tier",
            "lead_interest_category",   # stored funnel — fallback for dormant leads
            "lead_last_call_date", "lead_last_call_outcome",
            "lead_call_attempts", "lead_not_interested", "lead_call_booked",
            "hs_email_open_count", "hs_email_click_count",
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
) -> tuple[int, int, list[str]]:
    """
    Batch-update HubSpot contacts via /crm/v3/objects/contacts/batch/update.

    Each item in updates: {"id": contact_id, "properties": {...}}.
    Returns (ok_count, chunk_error_count, error_sample_messages).
    Callers should surface chunk_error_count > 0 as an alert — it means
    some contacts were NOT updated even though the batch appeared to run.
    """
    if not updates:
        return 0, 0, []

    headers = {
        "Authorization": f"Bearer {HUBSPOT_TOKEN}",
        "Content-Type": "application/json",
    }
    updated = 0
    chunk_errors = 0
    error_samples: list[str] = []

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
                chunk_errors += 1
                err_msg = f"HTTP {e.response.status_code} chunk {i}-{i+len(chunk)}: {e.response.text[:300]}"
                logger.error("HubSpot batch update failed — %s", err_msg)
                if len(error_samples) < 2:
                    error_samples.append(err_msg)
            except Exception as e:
                chunk_errors += 1
                err_msg = f"chunk {i}-{i+len(chunk)}: {e}"
                logger.error("HubSpot batch update error — %s", err_msg)
                if len(error_samples) < 2:
                    error_samples.append(err_msg)

    return updated, chunk_errors, error_samples


_HUBSPOT_NOTE_MARKER = "── Lead Score Card ──"


async def _write_hubspot_note(contact_id: str, body: str) -> None:
    """
    Write a note on a HubSpot contact visible in the activity timeline.

    Uses the Engagements v3 API (POST /crm/v3/objects/notes).
    Deduplicates: searches for existing scorer notes and deletes them first.
    """
    headers = {
        "Authorization": f"Bearer {HUBSPOT_TOKEN}",
        "Content-Type": "application/json",
    }
    timestamped_body = f"{_HUBSPOT_NOTE_MARKER}\n{body}"

    async with httpx.AsyncClient(timeout=15.0) as client:
        # Search for existing scorer notes on this contact to dedup
        try:
            assoc_resp = await client.get(
                f"{HUBSPOT_BASE}/crm/v3/objects/contacts/{contact_id}/associations/notes",
                headers=headers,
            )
            if assoc_resp.status_code == 200:
                for assoc in assoc_resp.json().get("results", []):
                    note_id = assoc.get("id")
                    if not note_id:
                        continue
                    # Fetch note body to check marker
                    note_resp = await client.get(
                        f"{HUBSPOT_BASE}/crm/v3/objects/notes/{note_id}",
                        headers=headers,
                        params={"properties": "hs_note_body"},
                    )
                    if note_resp.status_code == 200:
                        existing_body = (
                            note_resp.json()
                            .get("properties", {})
                            .get("hs_note_body", "")
                        )
                        if _HUBSPOT_NOTE_MARKER in existing_body:
                            # Delete old scorer note
                            await client.delete(
                                f"{HUBSPOT_BASE}/crm/v3/objects/notes/{note_id}",
                                headers=headers,
                            )
        except Exception:
            pass  # Dedup is best-effort

        # Create new note
        resp = await client.post(
            f"{HUBSPOT_BASE}/crm/v3/objects/notes",
            headers=headers,
            json={
                "properties": {
                    "hs_note_body": timestamped_body,
                    "hs_timestamp": datetime.now(timezone.utc).isoformat(),
                },
                "associations": [
                    {
                        "to": {"id": contact_id},
                        "types": [
                            {
                                "associationCategory": "HUBSPOT_DEFINED",
                                "associationTypeId": 10,  # note_to_contact (HubSpot v3)
                            }
                        ],
                    }
                ],
            },
        )
        if resp.status_code not in (200, 201):
            logger.warning(
                "HubSpot note create failed for contact %s: %s %s",
                contact_id, resp.status_code, resp.text[:300],
            )


# ---------------------------------------------------------------------------
# Phone normalization
# ---------------------------------------------------------------------------

def _normalize_phone(phone: str) -> str:
    """
    Normalize a phone number to E.164 format (+prefix) for Aircall.

    Handles common European formats stored in HubSpot:
      +41791234567   → +41791234567   (already correct)
      0041791234567  → +41791234567   (00XX international prefix)
      41791234567    → +41791234567   (Swiss CC without +, 11 digits)
      4917612345678  → +4917612345678 (German CC without +, 13 digits)
      '+49 170 7094840 → +491707094840 (apostrophe + spaces, Excel artefact)
      017612345678   → +4917612345678 (German mobile local, 01[5-7]x)
      0791234567     → +410791234567  (Swiss mobile local, 07x)
      763263775      → unchanged      (no recognizable prefix)
    """
    import re as _re
    if not phone:
        return phone

    # Strip leading apostrophe (Excel CSV artefact: '+49... stored as text)
    phone = phone.lstrip("'")
    # Strip all spaces and dashes for normalization
    digits_only = _re.sub(r"[\s\-\(\)\/\.]", "", phone)

    if digits_only.startswith("+"):
        return digits_only  # already E.164

    if digits_only.startswith("00") and len(digits_only) >= 11:
        # 00XX international prefix → +XX
        return "+" + digits_only[2:]

    # Known DACH country codes present without '+': 41 (CH), 49 (DE), 43 (AT)
    # Require full international length to avoid misidentifying local prefixes
    for cc, min_len, max_len in [("41", 11, 12), ("49", 12, 14), ("43", 11, 13)]:
        if digits_only.startswith(cc) and min_len <= len(digits_only) <= max_len:
            return "+" + digits_only

    # German local mobile: 015x / 016x / 017x — strip trunk 0, add +49
    if _re.match(r"^01[5-7]\d{7,9}$", digits_only):
        return "+49" + digits_only[1:]  # 0151... → +4915...

    # Swiss local mobile: 07x — strip trunk 0, add +41
    if _re.match(r"^07[5-9]\d{6,7}$", digits_only):
        return "+41" + digits_only[1:]  # 079... → +4179...

    # Austrian local mobile: 06x — strip trunk 0, add +43
    if _re.match(r"^06[5-9]\d{7,9}$", digits_only):
        return "+43" + digits_only[1:]  # 0699... → +4369...

    # Cannot safely determine country code — return cleaned but unchanged
    return digits_only


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


def _should_exclude_from_queue(
    last_call_date: str | None,
    call_outcome: str | None = None,
    call_attempts: int = 0,
) -> tuple[bool, str]:
    """
    Check if a lead should be excluded from the Aircall queue.

    Returns (should_exclude, reason).

    Exclusion rules:
      1. Permanent remove outcomes (falsche Nummer, nicht qualifiziert, abgesagt)
      2. Max call attempts exceeded (5+ unanswered)
      3. Cooldown: answered=7d, voicemail=3d, no-answer=3d
    """
    # 1. Permanent remove — these leads should never be called again
    if call_outcome and call_outcome.lower().strip() in {o.lower() for o in PERMANENT_REMOVE_OUTCOMES}:
        return True, f"permanent_remove:{call_outcome}"

    # 2. Max attempts — after 5 tries, stop calling
    if call_attempts >= MAX_CALL_ATTEMPTS:
        return True, f"max_attempts:{call_attempts}"

    # 3. Cooldown — temporary exclusion based on last call
    if not last_call_date:
        return False, ""
    try:
        dt = datetime.fromisoformat(last_call_date.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return False, ""

    hours_since = (datetime.now(timezone.utc) - dt).total_seconds() / 3600

    if call_outcome and "kontakt aufgenommen" in call_outcome.lower():
        if hours_since < COOLDOWN_ANSWERED_HOURS:
            return True, "cooldown:answered"
    elif call_outcome and "voicemail" in call_outcome.lower():
        if hours_since < COOLDOWN_VOICEMAIL_HOURS:
            return True, "cooldown:voicemail"
    else:
        # No answer, busy, unknown — 3 day cooldown
        if hours_since < COOLDOWN_NO_ANSWER_HOURS:
            return True, "cooldown:no_answer"

    return False, ""


def _aircall_priority_key(item: dict) -> tuple:
    """
    Sort key for Aircall queue. Lower = higher priority.

    Order: EC first → Fresh (by recency) → Hot (by score desc) → Warm (by score desc).
    """
    priority = AIRCALL_PRIORITY.get(item.get("list_key", ""), 99)
    # Within same priority: higher score first (negate for ascending sort)
    # For fresh leads: more recent first (lower fresh_hours first)
    fresh_hours = item.get("fresh_hours", float("inf"))
    score = item.get("score", 0)
    return (priority, fresh_hours, -score)


def _determine_tier_label(score: float, is_fresh: bool, is_booked: bool = False) -> str:
    """Human-readable tier label for Aircall card."""
    if is_booked:
        return "BOOKED"
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
    purchased_funnels: list[str] | None = None,
) -> str | None:
    """
    Determine which list a lead belongs to.

    Priority: Eignungscheck > Funnel fresh/warm > None.
    Leads who filled the Eignungscheck form always go there first (top prio).
    Others route to their funnel's fresh or warm list.

    Leads who already purchased the Ausbildung in this funnel (hc/mc/gc) are
    excluded from all calling lists — they are customers, not prospects.
    """
    if qualifies_eignungscheck:
        return "eignungscheck"

    if funnel not in VALID_FUNNELS:
        return None

    # Already bought this Ausbildung → they are a customer, not a prospect
    if purchased_funnels and funnel in purchased_funnels:
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
    is_fresh: bool = False,
    purchases: list[dict] | None = None,
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
        "lead_purchased_products": _format_purchases_display(purchases or []) or (
            ", ".join(purchased_funnels) if purchased_funnels else ""
        ),
        "lead_multi_funnel": multi_funnel_info,
        "lead_score_updated_at": now_iso,
        "lead_engagement_score": scoring.engagement_score,
        # Drives the "HC/MC/GC — Frisch" HubSpot dynamic lists (resets to False after 24h)
        "lead_is_fresh": "true" if is_fresh else "false",
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
    purchases: list[dict] | None = None,
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
        f"⭐ Priorität: {score:.0f}/100",
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
    purchase_display = _format_purchases_display(purchases or [])
    if purchase_display:
        lines.append(f"Kauf: {purchase_display}")
    elif purchased_funnels:
        lines.append(f"Kauf: {', '.join(f.title() for f in purchased_funnels)}")

    next_product = _next_product_recommendation(purchased_funnels or [], funnel, purchases)
    if next_product:
        lines.append(f"Naechstes Produkt: {next_product}")

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
    import time as _time
    _batch_start = _time.monotonic()
    _stats = BatchRunStats()

    async def _finish() -> None:
        _stats.duration_seconds = _time.monotonic() - _batch_start
        await send_batch_report(_stats)

    logger.info("Batch scoring: starting run")

    # Step 1: Fetch active HubSpot leads
    try:
        leads = await _fetch_active_hubspot_leads()
    except Exception as e:
        logger.error("Batch: failed to fetch HubSpot leads: %s", e)
        _stats.fatal_error = f"Step 1 HubSpot fetch: {e}"
        await _finish()
        return

    if not leads:
        logger.info("Batch scoring: no leads to process")
        await _finish()
        return

    _stats.leads_fetched = len(leads)
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
        _stats.fatal_error = f"Step 2 Supabase touchpoints: {e}"
        await _finish()
        return

    # Fetch events + purchases + meetings in one consolidated call (single contacts query)
    try:
        all_lead_data = await fetch_all_lead_data(all_emails, days=RESCORE_WINDOW_DAYS)
    except Exception as exc:
        logger.warning("Batch: Supabase lead data fetch failed: %s", exc)
        all_lead_data = {em: {"events": [], "purchases": [], "meetings": []} for em in all_emails}

    # Step 3: Score each contact — collect HubSpot updates for batch push
    hubspot_updates: list[dict[str, Any]] = []  # {"id": ..., "properties": {...}}
    hubspot_notes_queue: list[dict[str, Any]] = []  # {"contact_id": ..., "email": ..., "card": ...}
    aircall_queue: list[dict[str, Any]] = []     # leads to push to Aircall
    new_hot_leads: list[dict[str, Any]] = []     # leads that became hot THIS run
    decay_alerts: list[dict[str, Any]] = []      # tier downgrades for Slack
    # Map list_key -> [contact_ids] for bulk HubSpot list membership updates
    list_memberships: dict[str, list[str]] = {k: [] for k in LISTS}
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

        # Fallback: if Supabase has no purchases but HubSpot has lead_purchased_products,
        # use the HubSpot value. Most buyers aren't in Supabase yet (Bexio sync incomplete).
        hs_purchased = (props.get("lead_purchased_products") or "").strip()
        if not purchased_funnels and hs_purchased:
            # HubSpot stores funnel slugs: "meditation", "hypnose", "lifecoach"
            purchased_funnels = [f.strip() for f in hs_purchased.split(",") if f.strip()]

        try:
            # Map touchpoints to scored events
            scored_events = map_touchpoints_batch(touchpoints)

            # Map browser events (offer views, videos, checkout) and merge
            browser_scored = map_browser_events_batch(browser_events)
            scored_events.extend(browser_scored)

            # Build flat product key list for scoring signals + interest fallback
            # Include both product_key (e.g. "hc") and lowercased product_name
            # (e.g. "inner journey") so both PURCHASE_BONUS and funnel fallback fire.
            purchased_product_keys: list[str] = []
            for p in purchases:
                if pk := (p.get("product_key") or "").lower().strip():
                    purchased_product_keys.append(pk)
                if pn := (p.get("product_name") or "").lower().strip():
                    purchased_product_keys.append(pn)

            # Calculate engagement score (purchase bonus applied inside)
            engagement_result = calculate_engagement_score(
                scored_events, purchased_products=purchased_product_keys,
            )

            # Detect interest category (purchased_products fallback for no-URL leads)
            interest_result = detect_interest_category(
                scored_events, purchased_products=purchased_product_keys,
            )

            # Combine scores
            scoring = combine_scores(engagement_result, interest_result)
            score = scoring.combined_score
            funnel = scoring.interest_category

            # Funnel fallback: if Supabase scoring returned no category,
            # use the stored HubSpot value (set in a previous batch run).
            # Critical for dormant leads whose events are older than RESCORE_WINDOW_DAYS.
            if not funnel:
                hs_stored_funnel = (props.get("lead_interest_category") or "").strip()
                if hs_stored_funnel in VALID_FUNNELS:
                    funnel = hs_stored_funnel
                    scoring.interest_category = funnel

            # Determine freshness
            is_fresh, fresh_hours = _determine_freshness(touchpoints)
            tier_label = _determine_tier_label(score, is_fresh)

            # Detect tier decay — compare old tier from HubSpot with new
            old_tier = props.get("lead_tier") or ""
            new_tier = scoring.lead_tier
            old_score_val = float(props.get("lead_engagement_score") or 0)
            _TIER_RANK = {"1_hot": 1, "2_warm": 2, "3_cold": 3, "4_disqualified": 4}
            if (
                old_tier in _TIER_RANK
                and new_tier in _TIER_RANK
                and _TIER_RANK[new_tier] > _TIER_RANK[old_tier]
            ):
                name = f"{props.get('firstname', '')} {props.get('lastname', '')}".strip() or email
                decay_alerts.append({
                    "name": name, "email": email,
                    "old_tier": old_tier, "new_tier": new_tier,
                    "old_score": old_score_val, "new_score": score,
                    "interest_category": funnel,
                })

            # Extract first/last touch
            first_touch, last_touch = extract_first_last_touch(touchpoints)

            # Email activity summary (14 days)
            # Pass both touchpoints AND scored_events — CIO email events may
            # appear in events table (as email_opened/email_link_clicked) rather
            # than in touchpoints table (as channel=email, touchpoint_type=opened).
            email_summary = summarize_email_activity(
                touchpoints, days=14, scored_events=scored_events,
            )
            # Fallback: if Supabase has no email data, use HubSpot's native
            # email tracking (hs_email_open_count / hs_email_click_count).
            # These are cumulative (all-time), not 14-day windowed, but better
            # than showing 0/0 on the Aircall card.
            if email_summary["opens"] == 0 and email_summary["clicks"] == 0:
                hs_opens = int(props.get("hs_email_open_count") or 0)
                hs_clicks = int(props.get("hs_email_click_count") or 0)
                if hs_opens or hs_clicks:
                    email_summary["opens"] = hs_opens
                    email_summary["clicks"] = hs_clicks

            # Build funnel source string
            funnel_source = _build_funnel_source(touchpoints)

            # Check for multi-funnel interest
            cat_scores = interest_result.get("category_scores", {})
            multi_funnels = [f for f in VALID_FUNNELS if cat_scores.get(f, 0) > 0]
            multi_funnel_info = ", ".join(multi_funnels) if len(multi_funnels) > 1 else ""

            # Unsubscribed check — use event data already fetched from Supabase.
            # Avoids 1 CIO API call per lead (was: 8,582 calls × 200ms = 28 min).
            # email_unsubscribed events land in Supabase via CIO webhook → same source.
            unsubscribed = any(
                e.get("event_type") == "email_unsubscribed" for e in scored_events
            )

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

            # HubSpot meeting check — rely on lead_call_booked property (updated by call_poller)
            # and Supabase meetings (already fetched above).
            # Avoids 1 HubSpot API call per lead (was: 8,582 calls × 200ms = 28 min).
            # The call_poller syncs booked/completed meetings back to lead_call_booked every 5 min.
            has_hs_meeting = False  # covered by _truthy(props.get("lead_call_booked")) below

            # Check for calendar_link_sent in browser events / touchpoints
            # (WhatsApp INA bot sends Kevin's calendar link — lead may book soon)
            calendar_link_sent = any(
                (ev.get("event_type") or "").lower() in (
                    "calendar_link_sent", "calendar_link_clicked",
                    "wa_calendar_link_sent",
                )
                for ev in browser_events
            ) or any(
                (tp.get("touchpoint_type") or "").lower() in (
                    "calendar_link_sent", "calendar_link_clicked",
                )
                for tp in touchpoints
            )

            call_booked = (
                has_scheduled_meeting
                or has_hs_meeting
                or _truthy(props.get("lead_call_booked"))
            )
            not_interested = _truthy(props.get("lead_not_interested"))
            _raw_phone = _normalize_phone(
                (props.get("phone") or props.get("mobilephone") or "").strip()
            )
            has_phone = len(_raw_phone) > 6  # reject stubs like "+41", "+49", "+"

            # Read call outcome early — needed for both DNC and cooldown
            call_outcome = props.get("lead_last_call_outcome")

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
                call_outcome=call_outcome,
            )

            # --- TASK A: Booked leads skip Aircall ---
            # If a meeting is booked (HubSpot, Supabase, or calendar link sent),
            # override tier to 'booked' and remove from all calling lists.
            # Calendar link sent = lead received Kevin's booking link via WhatsApp,
            # they may book imminently, so don't cold-call them.
            if call_booked:
                scoring.lead_tier = "0_booked"
                tier_label = "BOOKED"
                logger.info(
                    "Batch: %s has booked meeting — tier=0_booked, skipping Aircall "
                    "(hs_meeting=%s, supabase=%s, hs_prop=%s, calendar_link=%s)",
                    email, has_hs_meeting, has_scheduled_meeting,
                    _truthy(props.get("lead_call_booked")), calendar_link_sent,
                )

            # Eignungscheck qualification — ONLY for leads who submitted the form.
            # Warm/fresh leads without a form submission go to their funnel list instead.
            # This prevents all phone-owning leads from being funnelled to Eignungscheck.
            has_submitted_form = any(
                e.get("event_type") == "application_submitted"
                for e in scored_events
            )
            qualifies_eignungscheck = (
                has_phone
                and has_submitted_form
                and not call_booked
                and not unsubscribed
                and not not_interested
                and funnel is not None
                and funnel not in purchased_funnels
            )

            # Determine list assignment first — needed to decide should_push logic
            list_key = _determine_list_key(
                funnel, is_fresh, score, qualifies_eignungscheck, purchased_funnels
            )

            # Dormant Hot/Warm: lead was previously scored Hot/Warm but has no recent events.
            # These leads should still be called — their stored tier reflects real engagement
            # that happened before the current RESCORE_WINDOW_DAYS cutoff.
            # We use the stored HubSpot score/tier for display and use SCORE_WARM as floor
            # for list assignment. The exclusion logic (cooldown, max attempts) still applies.
            is_dormant_warm = (
                old_tier in ("1_hot", "2_warm")
                and score < SCORE_WARM
                and not call_booked
            )
            if is_dormant_warm and list_key is None:
                # Assign to the appropriate warm/hot list using stored score as floor
                list_key = _determine_list_key(
                    funnel, is_fresh, SCORE_WARM, qualifies_eignungscheck, purchased_funnels
                )

            # Booked leads never go to Aircall — they already have a meeting
            if call_booked:
                list_key = None
                should_push = False
            else:
                # Eignungscheck leads always get called — form submission is the qualifier,
                # no score threshold applies. Score is shown on the Aircall card for context only.
                # Fresh/warm funnel lists require score >= 30 or freshness as a quality gate.
                # FRESH_MIN_SCORE (10) prevents single page_visited leads (3 pts) from being dialled.
                # Dormant Hot/Warm: previously scored leads with no recent events — still callable.
                should_push = has_phone and (
                    list_key == "eignungscheck"
                    or (is_fresh and score >= FRESH_MIN_SCORE)
                    or score >= SCORE_WARM
                    or is_dormant_warm
                )

            # Exclusion check: cooldown, permanent remove, max attempts
            # (call_outcome already read above for DNC check)
            try:
                call_attempts = int(props.get("lead_call_attempts") or 0)
            except (ValueError, TypeError):
                call_attempts = 0

            if should_push:
                excluded, exclude_reason = _should_exclude_from_queue(
                    last_call_date, call_outcome, call_attempts,
                )
                if excluded:
                    logger.debug(
                        "Batch: queue exclude %s — %s (last call %s, attempts=%d)",
                        email, exclude_reason, last_call_date, call_attempts,
                    )
                    should_push = False

            # Käufer-Listen: buyers with phone go into observation lists.
            # Only contacts WITH phone are added — phone-less buyers are tracked in HubSpot only.
            _KAEUFER_LIST_MAP = {"bf": "bf_kaeufer", "tfmw": "tfmw_kaeufer", "med": "med_kaeufer"}
            for p in purchases:
                pk = (p.get("product_key") or "").lower()
                kaeufer_key = _KAEUFER_LIST_MAP.get(pk)
                if kaeufer_key and has_phone and contact_id not in list_memberships[kaeufer_key]:
                    list_memberships[kaeufer_key].append(contact_id)

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
                is_fresh=is_fresh,
                purchases=purchases,
            )

            # For dormant Hot/Warm leads: preserve stored tier in HubSpot.
            # Without this, the batch would write lead_tier="3_cold" (score=0)
            # and on the next run is_dormant_warm=False → lead vanishes from Aircall.
            if is_dormant_warm:
                hs_properties["lead_tier"] = old_tier

            # Collect for batch HubSpot update (instead of per-lead PATCH)
            hubspot_updates.append({"id": contact_id, "properties": hs_properties})

            # Track HubSpot list membership — ONLY contacts with phone number.
            # Lists are calling lists — contacts without phone are useless for Kevin.
            if list_key and has_phone:
                list_memberships[list_key].append(contact_id)

            # Build the call card for ALL scored leads with phone that qualify for Aircall:
            # - Warm/Hot: score >= 30
            # - Fresh: is_fresh=True AND score >= FRESH_MIN_SCORE (10)
            # - Dormant Hot/Warm: previously scored leads with no recent events
            # This card is used for both Aircall notes AND HubSpot notes
            aircall_card = ""
            if has_phone and not call_booked and (
                score >= SCORE_WARM
                or (is_fresh and score >= FRESH_MIN_SCORE)
                or is_dormant_warm
            ):
                # For dormant leads: use stored HubSpot score + tier for the card
                # (current score=0 because no recent events, but last engagement was real)
                card_score = old_score_val if is_dormant_warm and score < SCORE_WARM else score
                card_tier_label = _OLD_TIER_LABELS.get(old_tier, tier_label) if is_dormant_warm and score < SCORE_WARM else tier_label
                offer_signals = _extract_offer_signals(browser_events)
                hook_context = {
                    "email_clicked": email_summary.get("clicks", 0) > 0,
                    "last_email_subject": email_summary.get("last_email_subject", ""),
                    "is_fresh": is_fresh,
                    "fresh_hours": fresh_hours,
                    "funnel": funnel,
                    "score": card_score,
                    "eignungscheck": qualifies_eignungscheck,
                    "call_booked": call_booked,
                    "purchased_products": purchased_product_keys,  # raw keys + names for hook rules
                    "visited_offer_page": offer_signals.get("visited_offer"),
                    "visited_checkout": offer_signals.get("visited_checkout"),
                    "watched_video_on_offer": offer_signals.get("video_on_offer"),
                    "viewed_pricing": offer_signals.get("viewed_pricing"),
                }
                hook = generate_hook(hook_context)
                aircall_card = _build_aircall_card(
                    tier_label=card_tier_label,
                    funnel=funnel,
                    score=card_score,
                    last_call_date=last_call_date,
                    email_summary=email_summary,
                    first_touch=first_touch,
                    last_touch=last_touch,
                    hook=hook,
                    purchased_funnels=purchased_funnels,
                    purchases=purchases,
                )

            # NOTE: lead_call_card property does NOT exist in HubSpot schema.
            # Card content is written as a timeline note via Step 4c instead.
            # Do NOT add lead_call_card to hs_properties — it causes 400 errors
            # on the batch update endpoint and silently kills all property updates.

            # Queue for HubSpot NOTE creation (separate from properties)
            if aircall_card:
                hubspot_notes_queue.append({
                    "contact_id": contact_id,
                    "email": email,
                    "card": aircall_card,
                })

            # Queue Aircall push if qualified and not DNC
            if should_push and list_key and aircall_card:
                priority_num = AIRCALL_PRIORITY.get(list_key, 4)
                priority_tag = f"P{priority_num}-{tier_label}"
                if funnel:
                    priority_tag += f"-{funnel[:3]}"

                # For dormant leads: pass at least SCORE_WARM (30) so _should_dial
                # doesn't reject them on the score<30 gate (current batch score=0).
                # Use the stored HubSpot score when available, otherwise floor at SCORE_WARM.
                aircall_score = max(card_score, SCORE_WARM) if is_dormant_warm else score
                aircall_tier = old_tier if is_dormant_warm else scoring.lead_tier

                aircall_queue.append({
                    "email": email,
                    "list_key": list_key,
                    "score": aircall_score,
                    "tier_label": tier_label,
                    "funnel": funnel,
                    "lead_tier": aircall_tier,
                    "phone": _raw_phone,
                    "firstname": props.get("firstname", ""),
                    "lastname": props.get("lastname", ""),
                    "aircall_card": aircall_card,
                    "fresh_hours": fresh_hours,
                    "priority_tag": priority_tag,
                    "is_fresh": is_fresh,
                })

            # Track NEW hot leads (was not hot before, now hot) for Slack alerts
            if scoring.lead_tier == "1_hot" and old_tier != "1_hot":
                new_hot_leads.append({
                    "email": email,
                    "phone": _raw_phone,
                    "firstname": props.get("firstname", ""),
                    "lastname": props.get("lastname", ""),
                    "score": score,
                    "engagement_score": scoring.engagement_score,
                    "tier": scoring.lead_tier,
                    "interest": funnel or "",
                    "contact_id": contact_id,
                    "funnel_source": props.get("lead_funnel_source", ""),
                    "is_fresh": is_fresh,
                })

        except Exception as e:
            logger.error("Batch: failed to score %s: %s", email, e)

    # Step 4: Batch-update HubSpot contact properties (100 per API call)
    _stats.leads_processed = len(email_lead_map)
    _stats.skipped_cold = skipped_cold
    _stats.skipped_dnc = skipped_dnc
    updated, _hs_errors, _hs_error_samples = await _batch_update_hubspot_contacts(hubspot_updates)
    _stats.hs_updates_ok = updated
    _stats.hs_chunk_errors = _hs_errors
    _stats.hs_error_samples = _hs_error_samples

    # Step 4b: Sync HubSpot list memberships — add contacts to the right static lists
    from integrations.hubspot import batch_add_to_list
    total_listed = 0
    for list_key, contact_ids in list_memberships.items():
        if not contact_ids:
            continue
        list_id = LISTS[list_key]["hubspot_list_id"]
        if not list_id:
            continue
        n = await batch_add_to_list(list_id, contact_ids)
        total_listed += n
        logger.info(
            "Batch: HubSpot list '%s' (id=%d) — added %d/%d contacts",
            list_key, list_id, n, len(contact_ids),
        )

    # Step 4c: Write HubSpot notes — Kevin sees these in the contact timeline
    hs_notes_written = 0
    if hubspot_notes_queue:
        logger.info("Batch: writing %d HubSpot notes", len(hubspot_notes_queue))
        for note_item in hubspot_notes_queue:
            try:
                await _write_hubspot_note(
                    contact_id=note_item["contact_id"],
                    body=note_item["card"],
                )
                hs_notes_written += 1
            except Exception as e:
                logger.warning(
                    "Batch: HubSpot note failed for %s: %s",
                    note_item["email"], e,
                )
        logger.info("Batch: wrote %d/%d HubSpot notes", hs_notes_written, len(hubspot_notes_queue))

    # Step 4d: Send Slack Hot Lead alerts for NEW hot leads
    if new_hot_leads:
        from integrations.slack import send_hot_lead_alert
        logger.info("Batch: %d NEW hot leads — sending Slack alerts", len(new_hot_leads))
        for hl in new_hot_leads[:10]:  # cap at 10 per batch
            try:
                await send_hot_lead_alert(
                    lead=hl,
                    combined_score=hl["score"],
                    lead_tier=hl["tier"],
                    interest_category=hl.get("interest"),
                )
            except Exception as e:
                logger.warning("Batch: Hot Lead Slack alert failed for %s: %s", hl["email"], e)

    # Step 5: Push qualified leads to Aircall — sorted by REVERSE priority
    # Aircall Power Dialer shows the LAST-added contact on TOP.
    # So we push lowest priority FIRST, highest priority LAST:
    # Warm (lowest score) → Hot → Fresh → EC (pushed last = shown first)
    aircall_queue.sort(key=_aircall_priority_key, reverse=True)
    logger.info(
        "Batch: Aircall queue has %d leads (sorted: EC→Fresh→Hot→Warm)",
        len(aircall_queue),
    )
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
                result = await add_to_power_dialer(
                    lead_dict,
                    score=item["score"],
                    is_fresh=item.get("is_fresh", False),
                    interest_category=item["funnel"],
                    lead_tier=item["lead_tier"],
                    list_key=item["list_key"],
                )
                if result is not None:
                    pushed += 1
                    logger.info(
                        "Batch: pushed %s to Aircall [%s] score=%.0f tier=%s",
                        item["email"], item["list_key"], item["score"], item["tier_label"],
                    )
                else:
                    _stats.aircall_rejected += 1
                    logger.warning(
                        "Batch: Aircall rejected %s — score=%.0f tier=%s is_fresh=%s "
                        "(check _should_dial logic)",
                        item["email"], item["score"], item["tier_label"], item.get("is_fresh"),
                    )
        except Exception as e:
            logger.error("Batch: Aircall push failed for %s: %s", item["email"], e)

    # Step 6: Send decay alerts to Slack (tier downgrades)
    if decay_alerts:
        logger.info("Batch: %d tier decay(s) detected — sending Slack alerts", len(decay_alerts))
        for da in decay_alerts[:10]:  # cap at 10 per batch to avoid Slack spam
            try:
                await send_decay_alert(
                    name=da["name"], email=da["email"],
                    old_tier=da["old_tier"], new_tier=da["new_tier"],
                    old_score=da["old_score"], new_score=da["new_score"],
                    interest_category=da["interest_category"],
                )
            except Exception as e:
                logger.warning("Batch: decay alert failed for %s: %s", da["email"], e)

    _stats.aircall_pushed = pushed
    _stats.notes_written = hs_notes_written

    # Step 6b: Verify actual Aircall dialer count — don't trust API response codes alone.
    # This catches the gap between "API returned 200" and "lead actually appears in dialer".
    if pushed > 0:
        try:
            from integrations.aircall import AIRCALL_BASE, AIRCALL_CLOSER_USER_ID, _headers
            async with httpx.AsyncClient(timeout=8.0) as _ac:
                _r = await _ac.get(
                    f"{AIRCALL_BASE}/users/{AIRCALL_CLOSER_USER_ID}/dialer_campaign",
                    headers=_headers(),
                )
                if _r.status_code == 200:
                    _dc = _r.json()
                    _stats.dialer_verified_count = len(_dc.get("phone_numbers", []))
                    logger.info(
                        "Batch: Aircall dialer verified — %d contacts in campaign (pushed %d this run)",
                        _stats.dialer_verified_count, pushed,
                    )
                    if _stats.dialer_verified_count == 0:
                        logger.error(
                            "Batch: AIRCALL GAP — pushed %d leads but dialer campaign is EMPTY. "
                            "Leads are being lost silently.",
                            pushed,
                        )
                else:
                    logger.warning("Batch: Aircall dialer verify failed: %s", _r.status_code)
        except Exception as _e:
            logger.warning("Batch: Aircall dialer verify exception (non-fatal): %s", _e)

    logger.info(
        "Batch scoring: done — %d/%d updated, %d listed, %d pushed, %d DNC-skipped, %d cold-skipped, %d decayed",
        updated, len(email_lead_map), total_listed, pushed, skipped_dnc, skipped_cold, len(decay_alerts),
    )

    # Step 7: Send batch health report to Slack (always — success AND errors)
    await _finish()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# Supabase product_key -> funnel mapping (used for interest scoring & display)
_PRODUCT_KEY_TO_FUNNEL: dict[str, str] = {
    "gc":   "lifecoach",
    "mc":   "meditation",
    "hc":   "hypnose",
    "afk":  "hypnose",   # Angstfrei event — high-intent signal, NOT customer exclusion
    "tfmw": "hypnose",   # Tag für Mentales Wachstum — event signal
}


# Human-readable product display names for Kevin's Aircall card
_PRODUCT_KEY_DISPLAY: dict[str, str] = {
    "hc":   "Hypnosecoach Ausbildung",
    "mc":   "Meditationscoach Ausbildung",
    "gc":   "Life Coach Ausbildung",
    "afk":  "Angstfrei Kongress",
    "tfmw": "Tag f. Mentales Wachstum",
    "bf":   "Bewusstseinsformel",
    "ik":   "Inneres Kind",
    "med":  "Medizinische Grundlagen",
}


def _format_purchases_display(purchases: list[dict]) -> str:
    """
    Build a human-readable purchase history string for Kevin's card.
    Separates full Ausbildung purchases from bundle/entry purchases.
    Example: "Hypnosecoach Ausbildung | Bundle: Inner Journey"
    """
    full_purchases: list[str] = []
    bundle_purchases: list[str] = []

    for p in purchases:
        pk = (p.get("product_key") or "").lower()
        pname = (p.get("product_name") or "").lower()
        display = _PRODUCT_KEY_DISPLAY.get(pk, pk.upper())

        if any(pat in pname for pat in _BUNDLE_PRODUCT_PATTERNS):
            bundle_purchases.append("Inner Journey (Bundle)")
        elif pk in {"hc", "mc", "gc"}:
            full_purchases.append(display)
        elif pk in _PRODUCT_KEY_DISPLAY:
            full_purchases.append(display)

    parts = []
    if full_purchases:
        parts.append(" | ".join(full_purchases))
    if bundle_purchases:
        parts.append("Bundle: " + ", ".join(bundle_purchases))
    return " | ".join(parts) if parts else ""


def _next_product_recommendation(
    purchased_funnels: list[str],
    funnel: str | None,
    purchases: list[dict] | None = None,
) -> str:
    """
    Derive the next product recommendation for Kevin.
    Cross-sell flow: HC -> GC -> MC
    Bundle buyers -> Vollprogramm of same track.
    """
    has_hc = "hypnose" in purchased_funnels
    has_mc = "meditation" in purchased_funnels
    has_gc = "lifecoach" in purchased_funnels

    has_bundle = any(
        any(pat in (p.get("product_name") or "").lower() for pat in _BUNDLE_PRODUCT_PATTERNS)
        for p in (purchases or [])
    )

    if has_hc and has_gc and has_mc:
        return "Fachspezialisierung HC"
    if has_hc and has_gc:
        return "Meditationscoach Ausbildung (MC)"
    if has_hc:
        return "Life Coach Ausbildung (GC)"
    if has_gc:
        return "Meditationscoach Ausbildung (MC)"
    if has_mc:
        return "Life Coach oder Hypnosecoach Ausbildung"
    if has_bundle:
        return "Meditationscoach Vollprogramm"
    if funnel == "hypnose":
        return "Hypnosecoach Ausbildung"
    if funnel == "meditation":
        return "Meditationscoach Ausbildung"
    if funnel == "lifecoach":
        return "Life Coach Ausbildung"
    return ""


def _extract_purchased_funnels(purchases: list[dict]) -> list[str]:
    """
    Return funnels where the lead has purchased the full Ausbildung.

    Only hc/mc/gc count — these make someone a customer who should NOT be
    called again for the same funnel. Entry-level products (afk, tfmw, bf)
    and bundles (Inner Journey) are interest signals, not customer exclusions.
    """
    funnels: set[str] = set()
    for p in purchases:
        pk = (p.get("product_key") or "").lower()
        pname = (p.get("product_name") or "").lower()
        # Skip bundle/entry products — not a full Ausbildung purchase
        if any(pat in pname for pat in _BUNDLE_PRODUCT_PATTERNS):
            continue
        if pk in _AUSBILDUNG_KEYS:
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
