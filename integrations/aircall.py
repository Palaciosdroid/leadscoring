"""
Aircall Power Dialer Integration
Pushes scored leads into Kevin's Power Dialer campaign as two virtual lists:

  🔥 Fresh — brand-new opt-in (< 24h), ANY score → call immediately
  🟡 Warm  — Hot + Warm tier (score ≥ 40) → follow-up queue

Cold + Disqualified leads stay in CIO nurturing only — no Aircall push.
Both lists feed the same Power Dialer queue.  Kevin sees the tag
("fresh" vs "warm") during the call so he knows the context.

Flow:
  1. Create/update Aircall contact with tags (list, score-XX, Interest Category)
  2. Push phone number into the Closer's Dialer Campaign

Docs: https://developer.aircall.io/api-references/
"""

import asyncio
import base64
import os
import logging
from datetime import datetime, timezone
from typing import Any

import httpx

logger = logging.getLogger(__name__)

# Retry config for Aircall 429 rate limits (60 req/min)
_MAX_RETRIES = 3
_RETRY_BASE_DELAY = 2.0  # seconds, doubles each retry

AIRCALL_API_ID       = os.environ.get("AIRCALL_API_ID", "")
AIRCALL_API_TOKEN    = os.environ.get("AIRCALL_API_TOKEN", "")
AIRCALL_BASE         = "https://api.aircall.io/v1"
AIRCALL_CLOSER_USER_ID = os.environ.get("AIRCALL_CLOSER_USER_ID", "")

FRESH_WINDOW_HOURS = 24
# Tiers that qualify for the Aircall Power Dialer (Hot + Warm = "Warm" list)
DIALABLE_TIERS: frozenset[str] = frozenset({"1_hot", "2_warm"})


def _is_fresh(created_at: datetime | None) -> bool:
    """True if lead opted in within the last 24 hours."""
    if not created_at:
        return False
    age_hours = (datetime.now(timezone.utc) - created_at).total_seconds() / 3600
    return age_hours < FRESH_WINDOW_HOURS


def _should_dial(score: float, created_at: datetime | None = None, lead_tier: str = "") -> bool:
    """Decide if lead qualifies for the Power Dialer.

    Fresh list: any score, opted in < 24h → always dial.
    Warm list:  tier is 1_hot or 2_warm   → dial.
    Cold/Disqualified: CIO nurturing only → skip.
    """
    if _is_fresh(created_at):
        return True
    return lead_tier in DIALABLE_TIERS


def _classify_list(created_at: datetime | None) -> str:
    """Return which virtual list this lead belongs to: 'fresh' or 'warm'."""
    return "fresh" if _is_fresh(created_at) else "warm"


def _build_tags(score: float, created_at: datetime | None, interest_category: str | None, list_key: str = "") -> list[str]:
    """Build Aircall contact tags for the Closer to see during calls.

    Uses short funnel names: HC (Hypnose), MC (Meditation), GC (Gesprächscoach).
    Combined tag format: 'hc-fresh', 'mc-warm', 'eignungscheck'.
    """
    # Short funnel mapping
    _SHORT = {"hypnose": "HC", "meditation": "MC", "lifecoach": "GC"}
    funnel_short = _SHORT.get(interest_category or "", interest_category or "")

    tags = [f"score-{int(score)}"]

    # Primary list tag (e.g. 'hc-fresh', 'eignungscheck')
    if list_key:
        tags.insert(0, list_key)

    # Funnel short name as separate tag
    if funnel_short:
        tags.append(funnel_short)

    return tags


def _headers() -> dict[str, str]:
    credentials = base64.b64encode(
        f"{AIRCALL_API_ID}:{AIRCALL_API_TOKEN}".encode()
    ).decode()
    return {
        "Accept":        "application/json",
        "Content-Type":  "application/json",
        "Authorization": f"Basic {credentials}",
    }


async def _aircall_request(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    **kwargs: Any,
) -> httpx.Response:
    """Make an Aircall API request with retry on 429 (rate limit)."""
    for attempt in range(_MAX_RETRIES + 1):
        response = await getattr(client, method)(url, headers=_headers(), **kwargs)
        if response.status_code != 429 or attempt == _MAX_RETRIES:
            return response
        # Exponential backoff: 2s, 4s, 8s
        delay = _RETRY_BASE_DELAY * (2 ** attempt)
        logger.warning(
            "Aircall 429 rate limit — retry %d/%d in %.0fs",
            attempt + 1, _MAX_RETRIES, delay,
        )
        await asyncio.sleep(delay)
    return response  # unreachable but keeps type checker happy


async def add_to_power_dialer(
    lead: dict[str, Any],
    *,
    score: float = 0,
    created_at: datetime | None = None,
    interest_category: str | None = None,
    lead_tier: str = "",
    list_key: str = "",
    timeout: float = 10.0,
) -> dict[str, Any] | None:
    """
    Push a lead into the Closer's Aircall Power Dialer campaign.

    list_key: key from LISTS dict (e.g. 'hc-fresh', 'eignungscheck').
              Used as primary tag on the Aircall contact so Kevin can
              filter by list in the Power Dialer.

    lead must contain: phone, firstname, lastname, email
    Returns None if not qualified (cold/disqualified and not fresh).
    """
    if not AIRCALL_API_ID or not AIRCALL_API_TOKEN:
        raise EnvironmentError("AIRCALL_API_ID and AIRCALL_API_TOKEN must be set")
    if not AIRCALL_CLOSER_USER_ID:
        raise EnvironmentError("AIRCALL_CLOSER_USER_ID must be set")

    if not _should_dial(score, created_at, lead_tier):
        logger.debug("Aircall: score %.0f too low, not fresh — skipping %s", score, lead.get("email"))
        return None

    # Resolve aircall_tag from list_key (e.g. 'hypnose_fresh' -> 'hc-fresh')
    from batch.scorer import LISTS
    aircall_tag = LISTS.get(list_key, {}).get("aircall_tag", list_key)
    tags = _build_tags(score, created_at, interest_category, list_key=aircall_tag)

    # Use shared client for both calls (connection reuse + retry on 429)
    async with httpx.AsyncClient(timeout=timeout) as client:
        # Step 1: Create/update contact with tags
        contact_id = await _upsert_contact(client, lead, tags=tags)

        # Step 2: Push phone number into Closer's dialer campaign
        return await _push_to_dialer_campaign(client, lead)


async def _upsert_contact(
    client: httpx.AsyncClient,
    lead: dict[str, Any],
    *,
    tags: list[str] | None = None,
) -> str:
    """Create or update an Aircall contact. Returns the Aircall contact ID."""
    phone = lead.get("phone", "")
    if not phone:
        raise ValueError(f"No phone number for lead {lead.get('email')}")

    payload: dict[str, Any] = {
        "first_name":    lead.get("firstname", ""),
        "last_name":     lead.get("lastname", ""),
        "information":   lead.get("notes", ""),
        "phone_numbers": [{"label": "mobile", "value": phone}],
        "emails":        [{"label": "work",   "value": lead.get("email", "")}],
    }
    if tags:
        payload["tags"] = tags

    response = await _aircall_request(
        client, "post", f"{AIRCALL_BASE}/contacts", json=payload,
    )

    if response.status_code not in (200, 201):
        logger.error(
            "Aircall: contact upsert failed for %s: %s %s",
            lead.get("email"), response.status_code, response.text,
        )
        response.raise_for_status()

    contact_id = str(response.json().get("contact", {}).get("id", ""))
    logger.info("Aircall: upserted contact %s → id=%s tags=%s", lead.get("email"), contact_id, tags)
    return contact_id


async def _push_to_dialer_campaign(
    client: httpx.AsyncClient,
    lead: dict[str, Any],
) -> dict[str, Any]:
    """Push a phone number into the Closer's Dialer Campaign."""
    phone = lead.get("phone", "")

    response = await _aircall_request(
        client, "post",
        f"{AIRCALL_BASE}/users/{AIRCALL_CLOSER_USER_ID}/dialer_campaign/phone_numbers",
        json={"phone_numbers": [phone]},
    )

    if response.status_code not in (200, 201):
        # 422 with "already imported" is OK — number already in campaign
        if response.status_code == 422 and "already imported" in response.text:
            logger.info("Aircall: %s already in dialer campaign — skipping", lead.get("email"))
            return {"status": "already_imported", "phone": phone}

        logger.error(
            "Aircall: dialer campaign push failed for %s: %s %s",
            lead.get("email"), response.status_code, response.text,
        )
        response.raise_for_status()

    logger.info("Aircall: pushed %s to Closer's dialer campaign (user %s)", lead.get("email"), AIRCALL_CLOSER_USER_ID)
    return {"status": "added", "phone": phone}


async def remove_from_power_dialer(
    phone: str,
    *,
    timeout: float = 10.0,
) -> bool:
    """
    Remove a phone number from the Closer's Aircall Power Dialer campaign.
    Called when a lead unsubscribes or is marked as "do not contact".

    Returns True if successfully removed, False if not found or error.
    """
    if not AIRCALL_API_ID or not AIRCALL_API_TOKEN or not AIRCALL_CLOSER_USER_ID:
        logger.warning("Aircall: credentials missing — cannot remove from Power Dialer")
        return False

    if not phone:
        logger.warning("Aircall: no phone number provided for removal")
        return False

    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await _aircall_request(
                client, "delete",
                f"{AIRCALL_BASE}/users/{AIRCALL_CLOSER_USER_ID}/dialer_campaign/phone_numbers",
                json={"phone_numbers": [phone]},
            )

        if response.status_code in (200, 204):
            logger.info("Aircall: removed %s from Power Dialer (user %s)", phone, AIRCALL_CLOSER_USER_ID)
            return True
        elif response.status_code == 404:
            logger.info("Aircall: phone %s not in Power Dialer — already removed or never added", phone)
            return True  # Already removed, so mission accomplished
        else:
            logger.error(
                "Aircall: failed to remove %s from Power Dialer: %s %s",
                phone, response.status_code, response.text,
            )
            return False

    except Exception as e:
        logger.error("Aircall: exception while removing %s from Power Dialer: %s", phone, e)
        return False


async def log_call_outcome(
    phone: str,
    outcome: str,
    contact_name: str = "",
    *,
    timeout: float = 10.0,
) -> None:
    """
    Append the call outcome to an Aircall contact's information field.

    Looks up the contact by phone number (Aircall search API), then prepends
    a timestamped outcome line so Kevin sees the latest result at the top.
    Silently skips when the contact is not in Aircall (e.g. lead not yet in
    the power dialer) or when credentials are missing.
    """
    if not AIRCALL_API_ID or not AIRCALL_API_TOKEN or not phone:
        return

    async with httpx.AsyncClient(timeout=timeout) as client:
        # Step 1: Find the Aircall contact by phone number
        search_resp = await _aircall_request(
            client, "get", f"{AIRCALL_BASE}/contacts",
            params={"phone_number": phone, "order": "desc", "order_by": "created_at", "per_page": 1},
        )

        if search_resp.status_code != 200:
            logger.warning(
                "Aircall: contact search failed for phone %s: %s — %s",
                phone, search_resp.status_code, search_resp.text[:200],
            )
            return

        contacts = search_resp.json().get("contacts", [])
        if not contacts:
            logger.debug("Aircall: phone %s not in Aircall — skipping outcome log", phone)
            return

        aircall_id    = contacts[0].get("id")
        existing_info = contacts[0].get("information", "") or ""

        # Step 2: Prepend outcome entry so newest result is at the top
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        new_entry    = f"[{ts}] {outcome}"
        updated_info = f"{new_entry}\n{existing_info}".strip()

        # Step 3: PATCH contact with updated information field
        update_resp = await _aircall_request(
            client, "put", f"{AIRCALL_BASE}/contacts/{aircall_id}",
            json={"information": updated_info},
        )

    if update_resp.status_code not in (200, 201):
        logger.warning(
            "Aircall: outcome log failed for %s (id=%s): %s %s",
            phone, aircall_id, update_resp.status_code, update_resp.text,
        )
    else:
        logger.info(
            "Aircall: outcome logged for %s (id=%s) → %s",
            contact_name or phone, aircall_id, outcome,
        )
