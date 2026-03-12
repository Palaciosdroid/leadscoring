"""
Scheduled Calls Summarizer — Daily report of booked calls.

Pulls HubSpot tasks/meetings for today + this week, groups by:
- Date
- Lead tier (Hot/Warm/Cold)
- Call direction (inbound/outbound)

Posts daily summary to Slack #sales-calls at 18:00 CET.
Runs via APScheduler in main.py.
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from integrations.slack import send_daily_summary

logger = logging.getLogger(__name__)

HUBSPOT_BASE = "https://api.hubapi.com"
ACCESS_TOKEN = __import__("os").environ.get("HUBSPOT_ACCESS_TOKEN", "")


def _headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }


async def fetch_scheduled_calls(
    days_ahead: int = 7,
    timeout: float = 30.0,
) -> dict[str, Any]:
    """
    Fetch all scheduled calls (HubSpot tasks/engagements) for next N days.

    Returns:
        {
            "total_scheduled": int,
            "by_date": {
                "2026-03-12": {
                    "scheduled": 5,
                    "hot": 2,
                    "warm": 2,
                    "cold": 1,
                }
            },
            "by_tier": {
                "1_hot": {"scheduled": 10, "confirmed": 2},
                "2_warm": {"scheduled": 15, "confirmed": 1},
                ...
            }
        }
    """
    now = datetime.now(tz=timezone.utc)
    future = now + timedelta(days=days_ahead)

    # Query: Get all tasks/engagements with hs_task_status = "not started" or "in progress"
    # AND hs_timestamp between now and future date
    query = {
        "filterGroups": [
            {
                "filters": [
                    {
                        "propertyName": "hs_task_status",
                        "operator": "IN",
                        "values": ["not_started", "in_progress"],
                    },
                    {
                        "propertyName": "hs_timestamp",
                        "operator": "BETWEEN",
                        "value": int(now.timestamp() * 1000),
                        "highValue": int(future.timestamp() * 1000),
                    },
                ]
            }
        ],
        "sorts": [{"propertyName": "hs_timestamp", "direction": "ASCENDING"}],
        "limit": 100,
    }

    scheduled_calls = {
        "total_scheduled": 0,
        "by_date": {},
        "by_tier": {
            "1_hot": {"scheduled": 0, "confirmed": 0},
            "2_warm": {"scheduled": 0, "confirmed": 0},
            "3_cold": {"scheduled": 0, "confirmed": 0},
            "4_disqualified": {"scheduled": 0, "confirmed": 0},
        },
    }

    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(
                f"{HUBSPOT_BASE}/crm/v3/objects/tasks/search",
                headers=_headers(),
                json=query,
            )

            if response.status_code != 200:
                logger.error(
                    "HubSpot tasks search failed: %s %s",
                    response.status_code,
                    response.text,
                )
                return scheduled_calls

            data = response.json()
            tasks = data.get("results", [])
            logger.info("scheduled_calls_summarizer: fetched %d scheduled tasks", len(tasks))

            # Process each task
            for task in tasks:
                props = task.get("properties", {})
                contact_id = task.get("associations", {}).get("contacts", [{}])[0].get("id")

                if not contact_id:
                    continue

                # Get contact lead_tier
                tier_response = await client.get(
                    f"{HUBSPOT_BASE}/crm/v3/objects/contacts/{contact_id}",
                    headers=_headers(),
                    params={
                        "properties": "lead_tier,hs_timestamp",
                    },
                )

                if tier_response.status_code != 200:
                    continue

                contact = tier_response.json()
                contact_props = contact.get("properties", {})
                lead_tier = contact_props.get("lead_tier", "3_cold")

                # Task timestamp
                ts_raw = props.get("hs_timestamp")
                if not ts_raw:
                    continue

                try:
                    ts = int(ts_raw) / 1000  # HubSpot returns milliseconds
                    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
                    date_str = dt.strftime("%Y-%m-%d")
                except (ValueError, TypeError):
                    continue

                # Initialize date if not present
                if date_str not in scheduled_calls["by_date"]:
                    scheduled_calls["by_date"][date_str] = {
                        "scheduled": 0,
                        "hot": 0,
                        "warm": 0,
                        "cold": 0,
                        "disqualified": 0,
                    }

                # Count by date and tier
                scheduled_calls["by_date"][date_str]["scheduled"] += 1
                scheduled_calls["total_scheduled"] += 1

                if lead_tier == "1_hot":
                    scheduled_calls["by_date"][date_str]["hot"] += 1
                    scheduled_calls["by_tier"]["1_hot"]["scheduled"] += 1
                elif lead_tier == "2_warm":
                    scheduled_calls["by_date"][date_str]["warm"] += 1
                    scheduled_calls["by_tier"]["2_warm"]["scheduled"] += 1
                elif lead_tier == "3_cold":
                    scheduled_calls["by_date"][date_str]["cold"] += 1
                    scheduled_calls["by_tier"]["3_cold"]["scheduled"] += 1
                else:
                    scheduled_calls["by_date"][date_str]["disqualified"] += 1
                    scheduled_calls["by_tier"]["4_disqualified"]["scheduled"] += 1

    except Exception as e:
        logger.error("scheduled_calls_summarizer: fetch failed: %s", e)

    return scheduled_calls


async def fetch_past_calls(
    days_back: int = 7,
    timeout: float = 30.0,
) -> dict[str, Any]:
    """
    Fetch all completed calls from past N days.
    Groups by direction and disposition to calculate:
    - Outbound attempts (Anschläge)
    - Outbound connected (meetings)
    - Inbound connected
    - Inbound duration

    Returns:
        {
            "outbound_total": int,          # all outbound attempts
            "outbound_connected": int,      # connected outbound
            "inbound_connected": int,       # connected inbound
            "inbound_duration_sec": int,    # total inbound talk time
        }
    """
    now = datetime.now(tz=timezone.utc)
    past = now - timedelta(days=days_back)

    query = {
        "filterGroups": [
            {
                "filters": [
                    {
                        "propertyName": "hs_timestamp",
                        "operator": "BETWEEN",
                        "value": int(past.timestamp() * 1000),
                        "highValue": int(now.timestamp() * 1000),
                    },
                ]
            }
        ],
        "sorts": [{"propertyName": "hs_timestamp", "direction": "DESCENDING"}],
        "limit": 100,
    }

    call_stats = {
        "outbound_total": 0,
        "outbound_connected": 0,
        "inbound_connected": 0,
        "inbound_duration_sec": 0,
    }

    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(
                f"{HUBSPOT_BASE}/crm/v3/objects/calls/search",
                headers=_headers(),
                json=query,
            )

            if response.status_code != 200:
                logger.warning(
                    "HubSpot calls search failed: %s %s",
                    response.status_code,
                    response.text[:200],
                )
                return call_stats

            data = response.json()
            calls = data.get("results", [])
            logger.info("scheduled_calls_summarizer: fetched %d past calls", len(calls))

            # Process each call
            for call in calls:
                props = call.get("properties", {})
                direction = props.get("hs_call_direction", "OUTBOUND").upper()
                disposition = props.get("hs_call_disposition", "")
                duration_ms = int(props.get("hs_call_duration", 0) or 0)
                duration_sec = duration_ms // 1000

                # Count outbound attempts
                if direction == "OUTBOUND":
                    call_stats["outbound_total"] += 1

                # Count connected calls
                from integrations.hubspot import CONNECTED_DISPOSITIONS
                if disposition in CONNECTED_DISPOSITIONS:
                    if direction == "OUTBOUND":
                        call_stats["outbound_connected"] += 1
                    else:  # INBOUND
                        call_stats["inbound_connected"] += 1
                        call_stats["inbound_duration_sec"] += duration_sec

    except Exception as e:
        logger.warning("scheduled_calls_summarizer: past_calls fetch failed: %s", e)

    return call_stats


async def run_scheduled_calls_summarizer() -> None:
    """
    Called every day at 17:55 CET (5 min before 18:00 report).
    Fetches:
    1. Past 7 days: completed calls (Anschläge, meetings, conversion metrics)
    2. Next 7 days: scheduled calls by lead tier
    Posts combined daily summary to Slack.
    """
    logger.info("scheduled_calls_summarizer: starting")

    try:
        # Fetch both past and scheduled calls
        past_calls = await fetch_past_calls(days_back=7)
        scheduled_calls = await fetch_scheduled_calls(days_ahead=7)

        # Extract past call metrics
        outbound_total = past_calls.get("outbound_total", 0)
        outbound_connected = past_calls.get("outbound_connected", 0)
        inbound_connected = past_calls.get("inbound_connected", 0)
        inbound_duration_sec = past_calls.get("inbound_duration_sec", 0)

        # Extract scheduled call metrics by tier
        scheduled_total = scheduled_calls.get("total_scheduled", 0)
        scheduled_hot = scheduled_calls.get("by_tier", {}).get("1_hot", {}).get("scheduled", 0)
        scheduled_warm = scheduled_calls.get("by_tier", {}).get("2_warm", {}).get("scheduled", 0)
        scheduled_cold = scheduled_calls.get("by_tier", {}).get("3_cold", {}).get("scheduled", 0)

        # Post combined daily summary to Slack
        await send_daily_summary(
            outbound_total=outbound_total,
            outbound_connected=outbound_connected,
            inbound_connected=inbound_connected,
            inbound_duration_sec=inbound_duration_sec,
            scheduled_total=scheduled_total,
            scheduled_hot=scheduled_hot,
            scheduled_warm=scheduled_warm,
            scheduled_cold=scheduled_cold,
        )

        logger.info(
            "scheduled_calls_summarizer: posted summary (past: %d outbound/%d connected, "
            "scheduled: %d total / %d hot / %d warm / %d cold)",
            outbound_total,
            outbound_connected,
            scheduled_total,
            scheduled_hot,
            scheduled_warm,
            scheduled_cold,
        )

    except Exception as e:
        logger.error("scheduled_calls_summarizer: error: %s", e)
