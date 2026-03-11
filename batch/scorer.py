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
CIO_APP_API_KEY = os.environ.get("CIO_APP_API_KEY", "")      # Customer.io App API key (Bearer)

# Only re-score leads updated in the last N days to keep API calls low
RESCORE_WINDOW_DAYS = int(os.environ.get("RESCORE_WINDOW_DAYS", "30"))


async def _fetch_active_hubspot_leads() -> list[dict[str, Any]]:
    """
    Pull all contacts that have been scored at least once.
    Paginates through HubSpot search results (max 100 per page).
    """
    headers = {
        "Authorization": f"Bearer {HUBSPOT_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {
        "filterGroups": [
            {
                "filters": [
                    {"propertyName": "lead_tier", "operator": "HAS_PROPERTY"},
                    {"propertyName": "phone", "operator": "HAS_PROPERTY"},
                ]
            }
        ],
        "properties": ["email", "firstname", "lastname", "phone",
                        "lead_engagement_score", "lead_tier"],
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

    return results


def _cio_headers() -> dict[str, str]:
    # Customer.io App API uses Bearer token auth
    return {"Authorization": f"Bearer {CIO_APP_API_KEY}"}


async def _fetch_cio_events(email: str) -> list[dict[str, Any]]:
    """
    Fetch the latest Customer.io events for a person identified by email.
    Uses the Customer.io App API v1 (app.customer.io/api/v1).
    """
    if not CIO_APP_API_KEY:
        return []

    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.get(
            "https://api.customer.io/v1/customers",
            headers=_cio_headers(),
            params={"email": email},
        )

    if resp.status_code != 200:
        logger.warning("CIO: could not fetch customer for %s: %s", email, resp.status_code)
        return []

    customers = resp.json().get("customers", [])
    if not customers:
        return []

    cio_id = customers[0].get("id")
    return await _fetch_cio_activities(cio_id)


async def _fetch_cio_activities(cio_id: str) -> list[dict[str, Any]]:
    """Fetch recent activities/events for a CIO customer ID."""
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.get(
            f"https://api.customer.io/v1/customers/{cio_id}/activities",
            headers=_cio_headers(),
            params={"limit": 50, "type": "event,page,email"},
        )

    if resp.status_code != 200:
        return []

    return resp.json().get("activities", [])


def _normalise_cio_activity(activity: dict[str, Any]) -> dict[str, Any]:
    """
    Convert a raw Customer.io activity record to our internal webhook format.

    CIO activity fields:
      type      → "event" | "page" | "email"
      name      → actual event name, e.g. "form_submit", "video_progress"
      timestamp → unix epoch int
      url       → page URL (for page/click events)
      data      → arbitrary event attributes
    """
    activity_type = activity.get("type", "")
    # For page events the name IS the type; for custom events use "name"
    event_name = activity.get("name") or activity_type

    ts_raw = activity.get("timestamp", "")
    # CIO returns unix timestamp as int — convert to ISO string for our pipeline
    if isinstance(ts_raw, (int, float)):
        from datetime import datetime, timezone
        ts_str = datetime.fromtimestamp(ts_raw, tz=timezone.utc).isoformat()
    else:
        ts_str = str(ts_raw)

    inner_data = activity.get("data", {}) or {}
    return {
        "event":     event_name,
        "timestamp": ts_str,
        "data": {
            "page":             {"url": activity.get("url", "")},
            "percent_complete": inner_data.get("percent_complete", 0),
            **inner_data,
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
