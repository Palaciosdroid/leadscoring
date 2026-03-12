"""
Aircall Power Dialer Integration
Pushes scored leads into Kevin's Power Dialer campaign so he always calls the right order.

Flow:
  1. Create/update Aircall contact with tags (fresh/warm, score-XX, Interest Category)
  2. Push phone number into the Closer's Dialer Campaign

Tags visible in Aircall UI during calls:
  🔥 fresh  → brand-new opt-in (< 24h) — call immediately
  🟡 warm   → Score ≥ 50, older leads worth following up

Docs: https://developer.aircall.io/api-references/
"""

import base64
import os
import logging
from datetime import datetime, timezone
from typing import Any

import httpx

logger = logging.getLogger(__name__)

AIRCALL_API_ID       = os.environ.get("AIRCALL_API_ID", "")
AIRCALL_API_TOKEN    = os.environ.get("AIRCALL_API_TOKEN", "")
AIRCALL_BASE         = "https://api.aircall.io/v1"
AIRCALL_CLOSER_USER_ID = os.environ.get("AIRCALL_CLOSER_USER_ID", "")

FRESH_WINDOW_HOURS = 24


def _is_fresh(created_at: datetime | None) -> bool:
    """True if lead opted in within the last 24 hours."""
    if not created_at:
        return False
    age_hours = (datetime.now(timezone.utc) - created_at).total_seconds() / 3600
    return age_hours < FRESH_WINDOW_HOURS


def _should_dial(score: float, created_at: datetime | None = None) -> bool:
    """Decide if lead qualifies for the Power Dialer: fresh OR score >= 5."""
    if _is_fresh(created_at):
        return True
    return score >= 5


def _build_tags(score: float, created_at: datetime | None, interest_category: str | None) -> list[str]:
    """Build Aircall contact tags for the Closer to see during calls."""
    tags = [f"score-{int(score)}"]
    if _is_fresh(created_at):
        tags.append("fresh")
    else:
        tags.append("warm")
    if interest_category:
        tags.append(interest_category)
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


async def add_to_power_dialer(
    lead: dict[str, Any],
    *,
    score: float = 0,
    created_at: datetime | None = None,
    interest_category: str | None = None,
    timeout: float = 10.0,
) -> dict[str, Any] | None:
    """
    Push a lead into the Closer's Aircall Power Dialer campaign.

    1. Upsert contact with tags (score, fresh/warm, interest)
    2. Push phone number into Closer's dialer campaign

    lead must contain: phone, firstname, lastname, email
    Returns None if score too low and not fresh.
    """
    if not AIRCALL_API_ID or not AIRCALL_API_TOKEN:
        raise EnvironmentError("AIRCALL_API_ID and AIRCALL_API_TOKEN must be set")
    if not AIRCALL_CLOSER_USER_ID:
        raise EnvironmentError("AIRCALL_CLOSER_USER_ID must be set")

    if not _should_dial(score, created_at):
        logger.debug("Aircall: score %.0f too low, not fresh — skipping %s", score, lead.get("email"))
        return None

    tags = _build_tags(score, created_at, interest_category)

    # Step 1: Create/update contact with tags
    contact_id = await _upsert_contact(lead, tags=tags, timeout=timeout)

    # Step 2: Push phone number into Closer's dialer campaign
    return await _push_to_dialer_campaign(lead, timeout=timeout)


async def _upsert_contact(
    lead: dict[str, Any],
    *,
    tags: list[str] | None = None,
    timeout: float,
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

    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.post(
            f"{AIRCALL_BASE}/contacts",
            headers=_headers(),
            json=payload,
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
    lead: dict[str, Any],
    *,
    timeout: float,
) -> dict[str, Any]:
    """Push a phone number into the Closer's Dialer Campaign."""
    phone = lead.get("phone", "")

    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.post(
            f"{AIRCALL_BASE}/users/{AIRCALL_CLOSER_USER_ID}/dialer_campaign/phone_numbers",
            headers=_headers(),
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
        search_resp = await client.get(
            f"{AIRCALL_BASE}/contacts",
            headers=_headers(),
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
        update_resp = await client.put(
            f"{AIRCALL_BASE}/contacts/{aircall_id}",
            headers=_headers(),
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
