"""
Batch Scorer
Re-scores all active leads every 15-60 minutes via cron.
Fetches leads from HubSpot, pulls latest events from Customer.io,
and updates scores.

Called by APScheduler in main.py.
"""

import logging
import os
from typing import Any

import httpx

logger = logging.getLogger(__name__)

HUBSPOT_BASE    = "https://api.hubapi.com"
HUBSPOT_TOKEN   = os.environ.get("HUBSPOT_ACCESS_TOKEN", "")
CIO_API_KEY     = os.environ.get("CIO_API_KEY", "")          # Customer.io v1 API key
CIO_SITE_ID     = os.environ.get("CIO_SITE_ID", "")

# Only re-score leads updated in the last N days to keep API calls low
RESCORE_WINDOW_DAYS = int(os.environ.get("RESCORE_WINDOW_DAYS", "30"))


async def _fetch_active_hubspot_leads() -> list[dict[str, Any]]:
    """
    Pull contacts that have been active (sbc_score_updated_at not null OR
    recently created) to avoid re-scoring the entire DB every run.
    """
    headers = {
        "Authorization": f"Bearer {HUBSPOT_TOKEN}",
        "Content-Type": "application/json",
    }
    # Search for contacts where sbc_lead_tier exists (i.e. already scored once)
    payload = {
        "filterGroups": [
            {
                "filters": [
                    {"propertyName": "sbc_lead_tier", "operator": "HAS_PROPERTY"}
                ]
            }
        ],
        "properties": ["email", "firstname", "lastname", "phone",
                        "sbc_engagement_score", "sbc_lead_tier"],
        "limit": 100,
    }

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(
            f"{HUBSPOT_BASE}/crm/v3/objects/contacts/search",
            headers=headers,
            json=payload,
        )
    resp.raise_for_status()
    return resp.json().get("results", [])


async def _fetch_cio_events(email: str) -> list[dict[str, Any]]:
    """
    Fetch the latest Customer.io events for a person identified by email.
    Uses the Customer.io Beta API v1.
    """
    if not CIO_API_KEY:
        return []

    async with httpx.AsyncClient(
        timeout=10.0,
        auth=(CIO_SITE_ID, CIO_API_KEY),
    ) as client:
        resp = await client.get(
            "https://api.customer.io/v1/customers",
            params={"email": email},
        )

    if resp.status_code != 200:
        logger.warning("CIO: could not fetch customer for %s: %s", email, resp.status_code)
        return []

    data = resp.json()
    customers = data.get("customers", [])
    if not customers:
        return []

    cio_id = customers[0].get("id")
    return await _fetch_cio_activities(cio_id)


async def _fetch_cio_activities(cio_id: str) -> list[dict[str, Any]]:
    """Fetch recent activities/events for a CIO customer ID."""
    async with httpx.AsyncClient(
        timeout=10.0,
        auth=(CIO_SITE_ID, CIO_API_KEY),
    ) as client:
        resp = await client.get(
            f"https://api.customer.io/v1/customers/{cio_id}/activities",
            params={"limit": 50, "type": "event,page,email"},
        )

    if resp.status_code != 200:
        return []

    return resp.json().get("activities", [])


def _normalise_cio_activity(activity: dict[str, Any]) -> dict[str, Any]:
    """
    Convert a raw Customer.io activity record to our internal event format.
    """
    return {
        "event":     activity.get("type", ""),
        "timestamp": activity.get("timestamp", ""),
        "data": {
            "page":             {"url": activity.get("url", "")},
            "percent_complete": activity.get("data", {}).get("percent_complete", 0),
            **activity.get("data", {}),
        },
    }


async def run_batch_scoring() -> None:
    """
    Main batch job: fetch leads → re-score → update HubSpot.
    """
    from main import _score_and_update, LeadContext

    logger.info("Batch scoring: starting run")

    try:
        leads = await _fetch_active_hubspot_leads()
    except Exception as e:
        logger.error("Batch: failed to fetch HubSpot leads: %s", e)
        return

    logger.info("Batch: %d leads to re-score", len(leads))
    updated = 0

    for contact in leads:
        props = contact.get("properties", {})
        contact_id = contact.get("id", "")
        email = props.get("email", "")

        if not contact_id or not email:
            continue

        try:
            raw_activities = await _fetch_cio_events(email)
            raw_events = [_normalise_cio_activity(a) for a in raw_activities]

            lead = LeadContext(
                contact_id=contact_id,
                email=email,
                firstname=props.get("firstname", ""),
                lastname=props.get("lastname", ""),
                phone=props.get("phone", ""),
            )

            await _score_and_update(raw_events, lead)
            updated += 1

        except Exception as e:
            logger.error("Batch: failed to score %s: %s", email, e)

    logger.info("Batch scoring: done — %d/%d contacts updated", updated, len(leads))
