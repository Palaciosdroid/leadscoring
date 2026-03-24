"""
Sales Calls Summarizer — Multi-view daily report.

Fetches HubSpot calls and generates 3 views:
- TODAY: detailed breakdown of today's calls
- 7 DAYS: weekly trend with per-day breakdown
- MONTH: monthly totals and averages

Posts to Slack #sales-calls at 18:00 CET via APScheduler.
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from integrations.slack import send_daily_summary
from integrations.hubspot import CONNECTED_DISPOSITIONS, HS_DISPOSITION_MAP

logger = logging.getLogger(__name__)

HUBSPOT_BASE = "https://api.hubapi.com"
ACCESS_TOKEN = __import__("os").environ.get("HUBSPOT_ACCESS_TOKEN", "")


def _headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }


async def _fetch_calls(
    since: datetime,
    until: datetime | None = None,
    timeout: float = 30.0,
) -> list[dict[str, Any]]:
    """
    Fetch all calls from HubSpot between since and until.
    Handles pagination (100 per page).
    """
    if until is None:
        until = datetime.now(tz=timezone.utc)

    query: dict[str, Any] = {
        "filterGroups": [{
            "filters": [{
                "propertyName": "hs_timestamp",
                "operator": "BETWEEN",
                "value": int(since.timestamp() * 1000),
                "highValue": int(until.timestamp() * 1000),
            }]
        }],
        "properties": [
            "hs_timestamp", "hs_call_direction", "hs_call_duration",
            "hs_call_disposition", "hs_call_title",
        ],
        "sorts": [{"propertyName": "hs_timestamp", "direction": "DESCENDING"}],
        "limit": 100,
    }

    all_calls: list[dict[str, Any]] = []
    after: str | None = None

    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            while True:
                if after:
                    query["after"] = after

                response = await client.post(
                    f"{HUBSPOT_BASE}/crm/v3/objects/calls/search",
                    headers=_headers(),
                    json=query,
                )

                if response.status_code != 200:
                    logger.warning(
                        "HubSpot calls search failed: %s %s",
                        response.status_code, response.text[:200],
                    )
                    break

                data = response.json()
                results = data.get("results", [])
                all_calls.extend(results)

                # Pagination
                paging = data.get("paging", {}).get("next", {})
                after = paging.get("after")
                if not after or len(results) < 100:
                    break

                await asyncio.sleep(0.3)  # rate limit

    except Exception as e:
        logger.warning("_fetch_calls failed: %s", e)

    logger.info("_fetch_calls: %d calls between %s and %s", len(all_calls), since.date(), until.date())
    return all_calls


def _analyze_calls(calls: list[dict[str, Any]]) -> dict[str, Any]:
    """
    Analyze a list of calls and return detailed stats.
    """
    # Calls under this threshold count as "Kurzverbindung" (pickup + immediate hangup)
    SHORT_CALL_THRESHOLD_SEC = 10

    stats: dict[str, Any] = {
        "total": 0,
        "outbound": 0,
        "inbound": 0,
        "connected": 0,         # real conversations (>10s)
        "short_connect": 0,     # pickup < 10s (Kurzverbindung)
        "no_answer": 0,
        "voicemail": 0,
        "busy": 0,
        "wrong_number": 0,
        "talk_time_sec": 0,     # only real conversations
        "by_date": {},
        "top_calls": [],
    }

    for call in calls:
        props = call.get("properties", {})
        direction = (props.get("hs_call_direction") or "OUTBOUND").upper()
        disposition = props.get("hs_call_disposition", "")
        duration_ms = int(props.get("hs_call_duration", 0) or 0)
        duration_sec = duration_ms // 1000
        ts_raw = props.get("hs_timestamp")

        stats["total"] += 1

        if direction == "OUTBOUND":
            stats["outbound"] += 1
        else:
            stats["inbound"] += 1

        # Disposition mapping
        disp_label = HS_DISPOSITION_MAP.get(disposition, "Unbekannt")
        if disposition in CONNECTED_DISPOSITIONS:
            if duration_sec < SHORT_CALL_THRESHOLD_SEC:
                # Pickup but immediate hangup — not a real conversation
                stats["short_connect"] += 1
            else:
                stats["connected"] += 1
                stats["talk_time_sec"] += duration_sec
                # Track long calls for top list
                if duration_sec > 60:
                    title = props.get("hs_call_title", "")
                    stats["top_calls"].append({
                        "title": title,
                        "duration_sec": duration_sec,
                        "direction": direction,
                        "timestamp": ts_raw,
                    })
        elif "Keine Antwort" in disp_label:
            stats["no_answer"] += 1
        elif "Voicemail" in disp_label:
            stats["voicemail"] += 1
        elif "Besetzt" in disp_label:
            stats["busy"] += 1
        elif "Falsche" in disp_label:
            stats["wrong_number"] += 1
        else:
            stats["no_answer"] += 1  # default

        # Per-date breakdown
        if ts_raw:
            try:
                ts_ms = int(ts_raw)
                dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
                date_str = dt.strftime("%d.%m")
                if date_str not in stats["by_date"]:
                    stats["by_date"][date_str] = {"total": 0, "connected": 0, "talk_sec": 0}
                stats["by_date"][date_str]["total"] += 1
                if disposition in CONNECTED_DISPOSITIONS:
                    stats["by_date"][date_str]["connected"] += 1
                    stats["by_date"][date_str]["talk_sec"] += duration_sec
            except (ValueError, TypeError):
                pass

    # Sort top calls by duration
    stats["top_calls"].sort(key=lambda x: x["duration_sec"], reverse=True)
    stats["top_calls"] = stats["top_calls"][:5]

    return stats


def _format_duration(seconds: int) -> str:
    """Format seconds to human readable."""
    if seconds < 60:
        return f"{seconds}s"
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    if hours > 0:
        return f"{hours}h {minutes}m"
    return f"{minutes}m {secs}s"


def _build_slack_blocks(
    today_stats: dict[str, Any],
    week_stats: dict[str, Any],
    month_stats: dict[str, Any],
) -> list[dict[str, Any]]:
    """Build Slack Block Kit message with 3 views."""
    now = datetime.now(tz=timezone.utc)
    today_str = now.strftime("%d. %B %Y")

    connect_rate_today = (
        f"{today_stats['connected'] / today_stats['total'] * 100:.0f}%"
        if today_stats["total"] > 0 else "-"
    )
    connect_rate_week = (
        f"{week_stats['connected'] / week_stats['total'] * 100:.0f}%"
        if week_stats["total"] > 0 else "-"
    )
    connect_rate_month = (
        f"{month_stats['connected'] / month_stats['total'] * 100:.0f}%"
        if month_stats["total"] > 0 else "-"
    )

    blocks: list[dict[str, Any]] = [
        {"type": "header", "text": {"type": "plain_text", "text": f"Sales Report - {today_str}"}},

        # TODAY
        {"type": "section", "text": {"type": "mrkdwn", "text": "*HEUTE*"}},
        {"type": "section", "fields": [
            {"type": "mrkdwn", "text": f"*Calls:* {today_stats['total']}"},
            {"type": "mrkdwn", "text": f"*Gespraeche:* {today_stats['connected']} ({connect_rate_today})"},
            {"type": "mrkdwn", "text": f"*Kurzverbindung (<10s):* {today_stats['short_connect']}"},
            {"type": "mrkdwn", "text": f"*Keine Antwort:* {today_stats['no_answer']}"},
            {"type": "mrkdwn", "text": f"*Gespraechszeit:* {_format_duration(today_stats['talk_time_sec'])}"},
            {"type": "mrkdwn", "text": f"*Outbound/Inbound:* {today_stats['outbound']}/{today_stats['inbound']}"},
        ]},
        {"type": "divider"},

        # 7 DAYS
        {"type": "section", "text": {"type": "mrkdwn", "text": "*LETZTE 7 TAGE*"}},
        {"type": "section", "fields": [
            {"type": "mrkdwn", "text": f"*Calls:* {week_stats['total']}"},
            {"type": "mrkdwn", "text": f"*Connected:* {week_stats['connected']} ({connect_rate_week})"},
            {"type": "mrkdwn", "text": f"*Gespraechszeit:* {_format_duration(week_stats['talk_time_sec'])}"},
            {"type": "mrkdwn", "text": f"*Avg/Tag:* {week_stats['total'] // max(len(week_stats['by_date']), 1)}"},
        ]},
    ]

    # Per-day breakdown for 7 days
    if week_stats["by_date"]:
        day_lines = []
        for date_str, day in sorted(week_stats["by_date"].items(), reverse=True):
            rate = f"{day['connected'] / day['total'] * 100:.0f}%" if day["total"] > 0 else "-"
            day_lines.append(
                f"{date_str}: {day['total']} Calls, {day['connected']} connected ({rate}), "
                f"{_format_duration(day['talk_sec'])}"
            )
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(day_lines)}})

    blocks.append({"type": "divider"})

    # MONTH
    blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": "*DIESER MONAT*"}})
    blocks.append({"type": "section", "fields": [
        {"type": "mrkdwn", "text": f"*Calls:* {month_stats['total']}"},
        {"type": "mrkdwn", "text": f"*Connected:* {month_stats['connected']} ({connect_rate_month})"},
        {"type": "mrkdwn", "text": f"*Gespraechszeit:* {_format_duration(month_stats['talk_time_sec'])}"},
        {"type": "mrkdwn", "text": f"*Falsche Nr:* {month_stats['wrong_number']}"},
    ]})

    # Top calls (week)
    if week_stats["top_calls"]:
        top_lines = ["*Top Gespraeche (7d):*"]
        for i, tc in enumerate(week_stats["top_calls"][:5], 1):
            title = tc.get("title", "").split(" - ")[-1] if tc.get("title") else "Unbekannt"
            top_lines.append(f"{i}. {title} - {_format_duration(tc['duration_sec'])}")
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(top_lines)}})

    return blocks


async def run_scheduled_calls_summarizer() -> None:
    """
    Called every day at 18:00 CET.
    Fetches calls for 3 time windows and posts combined report to Slack.
    """
    logger.info("scheduled_calls_summarizer: starting")

    try:
        now = datetime.now(tz=timezone.utc)

        # Today: midnight UTC to now
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)

        # 7 days: 7 days ago to now
        week_start = now - timedelta(days=7)

        # Month: 1st of current month to now
        month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

        # Fetch all 3 windows
        today_calls, week_calls, month_calls = await asyncio.gather(
            _fetch_calls(today_start, now),
            _fetch_calls(week_start, now),
            _fetch_calls(month_start, now),
        )

        # Analyze
        today_stats = _analyze_calls(today_calls)
        week_stats = _analyze_calls(week_calls)
        month_stats = _analyze_calls(month_calls)

        # Build and send Slack message
        blocks = _build_slack_blocks(today_stats, week_stats, month_stats)

        # Post to Slack via calls webhook
        import os
        webhook_url = os.environ.get("SLACK_CALLS_WEBHOOK_URL") or os.environ.get("SLACK_WEBHOOK_URL", "")
        if not webhook_url:
            logger.warning("No Slack webhook URL configured for daily summary")
            return

        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(webhook_url, json={"blocks": blocks})
            if resp.status_code == 200:
                logger.info(
                    "scheduled_calls_summarizer: posted (today=%d, week=%d, month=%d calls)",
                    today_stats["total"], week_stats["total"], month_stats["total"],
                )
            else:
                logger.error("Slack daily summary failed: %s %s", resp.status_code, resp.text)

    except Exception as e:
        logger.error("scheduled_calls_summarizer: error: %s", e)
