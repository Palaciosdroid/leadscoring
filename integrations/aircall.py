"""
Aircall Power Dialer Integration
Adds Hot Leads to the Aircall Power Dialer list for immediate Speed-to-Lead callback.
Docs: https://developer.aircall.io/api-references/
"""

import base64
import os
import logging
from typing import Any

import httpx

logger = logging.getLogger(__name__)

AIRCALL_API_ID      = os.environ.get("AIRCALL_API_ID", "")
AIRCALL_API_TOKEN   = os.environ.get("AIRCALL_API_TOKEN", "")
AIRCALL_BASE        = "https://api.aircall.io/v1"

# Aircall Power Dialer list ID — create one in the Aircall dashboard per programme
# or use a single unified Hot-Lead list
AIRCALL_DIALER_LIST_ID = os.environ.get("AIRCALL_DIALER_LIST_ID", "")


def _headers() -> dict[str, str]:
    # Aircall uses HTTP Basic Auth: base64(api_id:api_token)
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
    timeout: float = 10.0,
) -> dict[str, Any]:
    """
    Push a Hot Lead into the Aircall Power Dialer list.

    lead must contain: phone, firstname, lastname, email
    Optional: notes (used to display score/category to the agent)

    Flow:
      1. Upsert contact in Aircall
      2. Add contact to Power Dialer list
    """
    if not AIRCALL_API_ID or not AIRCALL_API_TOKEN:
        raise EnvironmentError("AIRCALL_API_ID and AIRCALL_API_TOKEN must be set")

    if not AIRCALL_DIALER_LIST_ID:
        raise EnvironmentError("AIRCALL_DIALER_LIST_ID must be set")

    contact_id = await _upsert_contact(lead, timeout=timeout)
    return await _add_to_dialer_list(contact_id, lead, timeout=timeout)


async def _upsert_contact(lead: dict[str, Any], *, timeout: float) -> str:
    """
    Create or update an Aircall contact. Returns the Aircall contact ID.
    Aircall deduplicates by phone number.
    """
    phone = lead.get("phone", "")
    if not phone:
        raise ValueError(f"No phone number for lead {lead.get('email')}")

    payload = {
        "first_name": lead.get("firstname", ""),
        "last_name":  lead.get("lastname", ""),
        "information": lead.get("notes", ""),
        "phone_numbers": [{"label": "mobile", "value": phone}],
        "emails":        [{"label": "work",   "value": lead.get("email", "")}],
    }

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
    logger.info("Aircall: upserted contact %s → id=%s", lead.get("email"), contact_id)
    return contact_id


async def _add_to_dialer_list(
    contact_id: str,
    lead: dict[str, Any],
    *,
    timeout: float,
) -> dict[str, Any]:
    """Add a contact to the Aircall Power Dialer list."""
    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.post(
            f"{AIRCALL_BASE}/power_dialer/lists/{AIRCALL_DIALER_LIST_ID}/contacts",
            headers=_headers(),
            json={"contact_id": contact_id},
        )

    if response.status_code not in (200, 201):
        logger.error(
            "Aircall: power dialer add failed for %s: %s %s",
            lead.get("email"), response.status_code, response.text,
        )
        response.raise_for_status()

    logger.info(
        "Aircall: added Hot Lead %s to Power Dialer list %s",
        lead.get("email"), AIRCALL_DIALER_LIST_ID,
    )
    return response.json()
