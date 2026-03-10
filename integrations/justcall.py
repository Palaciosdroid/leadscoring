"""
JustCall Dynamic Dialer Integration
Adds Hot Leads to the JustCall Dynamic Dialer queue for immediate callback.
Docs: https://justcall.io/developer-api/
"""

import os
import logging
from typing import Any

import httpx

logger = logging.getLogger(__name__)

JUSTCALL_API_KEY    = os.environ.get("JUSTCALL_API_KEY", "")
JUSTCALL_API_SECRET = os.environ.get("JUSTCALL_API_SECRET", "")
JUSTCALL_BASE       = "https://api.justcall.io/v1"

# The campaign/dialer ID to add Hot Leads to (set per programme or unified)
DIALER_CAMPAIGN_ID  = os.environ.get("JUSTCALL_CAMPAIGN_ID", "")


def _headers() -> dict[str, str]:
    return {
        "Accept":       "application/json",
        "Content-Type": "application/json",
        "Authorization": f"{JUSTCALL_API_KEY}:{JUSTCALL_API_SECRET}",
    }


async def add_to_dynamic_dialer(
    lead: dict[str, Any],
    *,
    timeout: float = 10.0,
) -> dict[str, Any]:
    """
    Push a Hot Lead into the JustCall Dynamic Dialer.

    lead must contain: phone, firstname, lastname, email
    Optional: company, notes (used for score/category context)
    """
    if not JUSTCALL_API_KEY or not DIALER_CAMPAIGN_ID:
        raise EnvironmentError(
            "JUSTCALL_API_KEY and JUSTCALL_CAMPAIGN_ID must be set"
        )

    payload = {
        "campaign_id": DIALER_CAMPAIGN_ID,
        "contacts": [
            {
                "phone":     lead.get("phone", ""),
                "firstname": lead.get("firstname", ""),
                "lastname":  lead.get("lastname", ""),
                "email":     lead.get("email", ""),
                "company":   lead.get("company", ""),
                "notes":     lead.get("notes", ""),
            }
        ],
    }

    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.post(
            f"{JUSTCALL_BASE}/autodialer/campaign/addcontacts",
            headers=_headers(),
            json=payload,
        )

    if response.status_code not in (200, 201):
        logger.error(
            "JustCall dialer add failed for %s: %s %s",
            lead.get("email"), response.status_code, response.text,
        )
        response.raise_for_status()

    logger.info("JustCall: added Hot Lead %s to dialer campaign %s",
                lead.get("email"), DIALER_CAMPAIGN_ID)
    return response.json()
