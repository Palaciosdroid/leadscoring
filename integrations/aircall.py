"""
Aircall Power Dialer Integration
Routes scored leads into priority-ordered Dialer lists so the Closer
always works the warmest leads first.

Lists (create in Aircall Dashboard → Power Dialer):
  🔴 priority  → Score ≥ 85   (call first — hottest & newest)
  🟠 hot       → Score 75-84  (call second — work through today)
  🟡 warm      → Score 50-74  (follow-up when priority + hot done)

Docs: https://developer.aircall.io/api-references/
"""

import base64
import os
import logging
from typing import Any

import httpx

logger = logging.getLogger(__name__)

AIRCALL_API_ID    = os.environ.get("AIRCALL_API_ID", "")
AIRCALL_API_TOKEN = os.environ.get("AIRCALL_API_TOKEN", "")
AIRCALL_BASE      = "https://api.aircall.io/v1"

# 3 Dialer lists by priority — create in Aircall, paste IDs here
DIALER_LIST_PRIORITY = os.environ.get("AIRCALL_LIST_PRIORITY", "")   # ≥ 85
DIALER_LIST_HOT      = os.environ.get("AIRCALL_LIST_HOT", "")       # 75-84
DIALER_LIST_WARM     = os.environ.get("AIRCALL_LIST_WARM", "")      # 50-74

# Fallback: single list if 3-list setup not configured yet
AIRCALL_DIALER_LIST_ID = os.environ.get("AIRCALL_DIALER_LIST_ID", "")


def _select_list(score: float) -> str | None:
    """Pick the right Dialer list based on lead score."""
    if score >= 85 and DIALER_LIST_PRIORITY:
        return DIALER_LIST_PRIORITY
    if score >= 75 and DIALER_LIST_HOT:
        return DIALER_LIST_HOT
    if score >= 50 and DIALER_LIST_WARM:
        return DIALER_LIST_WARM
    # Fallback to single list for hot leads only
    if score >= 75 and AIRCALL_DIALER_LIST_ID:
        return AIRCALL_DIALER_LIST_ID
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
    interest_category: str | None = None,
    timeout: float = 10.0,
) -> dict[str, Any] | None:
    """
    Push a lead into the correct Aircall Power Dialer list based on score.

    lead must contain: phone, firstname, lastname, email
    Optional: notes (score context for the Closer)

    Returns None if score too low or no list configured.
    """
    if not AIRCALL_API_ID or not AIRCALL_API_TOKEN:
        raise EnvironmentError("AIRCALL_API_ID and AIRCALL_API_TOKEN must be set")

    list_id = _select_list(score)
    if not list_id:
        logger.debug("Aircall: score %.0f too low for dialer — skipping %s", score, lead.get("email"))
        return None

    # Build tags: score tier + interest category
    tags = [f"score-{int(score)}"]
    if score >= 85:
        tags.append("priority")
    elif score >= 75:
        tags.append("hot")
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
    """Remove a contact from all known dialer lists (used before re-adding to correct list)."""
    for list_id in (DIALER_LIST_PRIORITY, DIALER_LIST_HOT, DIALER_LIST_WARM, AIRCALL_DIALER_LIST_ID):
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

    logger.info(
        "Aircall: added %s to Power Dialer list %s",
        lead.get("email"), list_id,
    )
    return response.json()
