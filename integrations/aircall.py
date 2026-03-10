"""
Aircall Power Dialer Integration
Routes leads into two lists so the Closer always works the right order.

Lists (create in Aircall Dashboard → Power Dialer):
  🔥 fresh  → brand-new opt-in (< 24h) — call immediately, regardless of score
  🟡 warm   → Score ≥ 50, older leads worth following up

Closer rule: empty 'fresh' first, then work 'warm'.

Docs: https://developer.aircall.io/api-references/
"""

import base64
import os
import logging
from datetime import datetime, timezone
from typing import Any

import httpx

logger = logging.getLogger(__name__)

AIRCALL_API_ID    = os.environ.get("AIRCALL_API_ID", "")
AIRCALL_API_TOKEN = os.environ.get("AIRCALL_API_TOKEN", "")
AIRCALL_BASE      = "https://api.aircall.io/v1"

DIALER_LIST_FRESH = os.environ.get("AIRCALL_LIST_FRESH", "")  # opt-in < 24h
DIALER_LIST_WARM  = os.environ.get("AIRCALL_LIST_WARM", "")   # score ≥ 50

FRESH_WINDOW_HOURS = 24


def _is_fresh(created_at: datetime | None) -> bool:
    """True if lead opted in within the last 24 hours."""
    if not created_at:
        return False
    age_hours = (datetime.now(timezone.utc) - created_at).total_seconds() / 3600
    return age_hours < FRESH_WINDOW_HOURS


def _select_list(score: float, created_at: datetime | None = None) -> str | None:
    """Pick the right Dialer list: fresh wins over score."""
    if _is_fresh(created_at) and DIALER_LIST_FRESH:
        return DIALER_LIST_FRESH
    if score >= 50 and DIALER_LIST_WARM:
        return DIALER_LIST_WARM
    return None


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
    Push a lead into the correct Aircall Power Dialer list.

    lead must contain: phone, firstname, lastname, email
    created_at: when the lead opted in (UTC). Fresh leads bypass score threshold.

    Returns None if score too low and not fresh.
    """
    if not AIRCALL_API_ID or not AIRCALL_API_TOKEN:
        raise EnvironmentError("AIRCALL_API_ID and AIRCALL_API_TOKEN must be set")

    list_id = _select_list(score, created_at)
    if not list_id:
        logger.debug("Aircall: score %.0f too low, not fresh — skipping %s", score, lead.get("email"))
        return None

    # Tags for the Closer
    tags = [f"score-{int(score)}"]
    if _is_fresh(created_at):
        tags.append("fresh")
    else:
        tags.append("warm")
    if interest_category:
        tags.append(interest_category)

    contact_id = await _upsert_contact(lead, tags=tags, timeout=timeout)
    return await _add_to_dialer_list(list_id, contact_id, lead, timeout=timeout)


async def remove_from_all_lists(
    contact_id: str,
    *,
    timeout: float = 10.0,
) -> None:
    """Remove a contact from all dialer lists (used before re-adding to correct list)."""
    for list_id in (DIALER_LIST_FRESH, DIALER_LIST_WARM):
        if not list_id:
            continue
        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                await client.delete(
                    f"{AIRCALL_BASE}/power_dialer/lists/{list_id}/contacts/{contact_id}",
                    headers=_headers(),
                )
        except Exception:
            pass  # not in this list — fine


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


async def _add_to_dialer_list(
    list_id: str,
    contact_id: str,
    lead: dict[str, Any],
    *,
    timeout: float,
) -> dict[str, Any]:
    """Add a contact to a specific Aircall Power Dialer list."""
    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.post(
            f"{AIRCALL_BASE}/power_dialer/lists/{list_id}/contacts",
            headers=_headers(),
            json={"contact_id": contact_id},
        )

    if response.status_code not in (200, 201):
        logger.error(
            "Aircall: power dialer add failed for %s (list %s): %s %s",
            lead.get("email"), list_id, response.status_code, response.text,
        )
        response.raise_for_status()

    logger.info("Aircall: added %s to Power Dialer list %s", lead.get("email"), list_id)
    return response.json()
