"""
HubSpot API Integration
Writes sbc_* custom properties back to a HubSpot contact.
Uses HubSpot Private App token (HUBSPOT_ACCESS_TOKEN env var).
"""

import os
import logging
from typing import Any

import httpx

logger = logging.getLogger(__name__)

HUBSPOT_BASE = "https://api.hubapi.com"
ACCESS_TOKEN = os.environ.get("HUBSPOT_ACCESS_TOKEN", "")


def _headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }


def _contact_url(contact_id: str) -> str:
    return f"{HUBSPOT_BASE}/crm/v3/objects/contacts/{contact_id}"


async def upsert_contact_score(
    contact_id: str,
    payload: dict[str, Any],
    *,
    timeout: float = 10.0,
) -> dict[str, Any]:
    """
    PATCH sbc_* properties onto a HubSpot contact.
    Returns the HubSpot API response body.
    """
    if not ACCESS_TOKEN:
        raise EnvironmentError("HUBSPOT_ACCESS_TOKEN is not set")

    # Remove None values — HubSpot ignores nulls anyway, but keep it clean
    properties = {k: v for k, v in payload.items() if v is not None}

    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.patch(
            _contact_url(contact_id),
            headers=_headers(),
            json={"properties": properties},
        )

    if response.status_code not in (200, 204):
        logger.error(
            "HubSpot PATCH failed for contact %s: %s %s",
            contact_id, response.status_code, response.text,
        )
        response.raise_for_status()

    logger.info("HubSpot updated contact %s → tier=%s", contact_id, payload.get("sbc_lead_tier"))
    return response.json() if response.content else {}


async def get_contact_events(contact_id: str) -> list[dict[str, Any]]:
    """
    Fetch the latest known sbc_* scores from HubSpot for a contact.
    Useful for batch re-scoring without re-fetching all events.
    """
    properties = [
        "sbc_engagement_score", "sbc_combined_score",
        "sbc_lead_tier", "sbc_interest_category", "sbc_score_updated_at",
    ]
    params = "&".join(f"properties={p}" for p in properties)

    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.get(
            f"{_contact_url(contact_id)}?{params}",
            headers=_headers(),
        )
    response.raise_for_status()
    return response.json().get("properties", {})
