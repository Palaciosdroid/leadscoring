"""
Slack Alert Integration
Posts Hot Lead notifications to a Slack channel via Incoming Webhook.
"""

import os
import logging
from typing import Any

import httpx

logger = logging.getLogger(__name__)

SLACK_WEBHOOK_URL       = os.environ.get("SLACK_WEBHOOK_URL", "")
SLACK_CALLS_WEBHOOK_URL = os.environ.get("SLACK_CALLS_WEBHOOK_URL", "")
# Falls back to hot-lead webhook if no dedicated channel is configured
SLACK_DECAY_WEBHOOK_URL = os.environ.get("SLACK_DECAY_WEBHOOK_URL", "") or SLACK_WEBHOOK_URL
# HubSpot portal ID — required to build direct contact deep-links
HUBSPOT_PORTAL_ID       = os.environ.get("HUBSPOT_PORTAL_ID", "")

# Tier rank for comparing decay direction — higher number = worse tier
TIER_ORDER: dict[str, int] = {
    "1_hot":          1,
    "2_warm":         2,
    "3_cold":         3,
    "4_disqualified": 4,
}

TIER_EMOJI = {
    "1_hot":          "🔥",
    "2_warm":         "🟡",
    "3_cold":         "🔵",
    "4_disqualified": "⛔",
}

CATEGORY_LABEL = {
    "hypnose":   "Hypnosecoach-Ausbildung",
    "lifecoach": "Lifecoach-Ausbildung",
    "meditation": "Meditationscoach-Ausbildung",
}


def _build_hot_lead_message(
    lead: dict[str, Any],
    combined_score: float,
    lead_tier: str,
    interest_category: str | None,
) -> dict[str, Any]:
    name = f"{lead.get('firstname', '')} {lead.get('lastname', '')}".strip() or "Unknown"
    email = lead.get("email", "N/A")
    phone = lead.get("phone", "N/A")
    emoji = TIER_EMOJI.get(lead_tier, "❓")
    category = CATEGORY_LABEL.get(interest_category or "", interest_category or "Unknown")

    return {
        "blocks": [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": f"{emoji} Hot Lead: {name}"},
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Score:*\n{combined_score:.0f} / 100"},
                    {"type": "mrkdwn", "text": f"*Tier:*\nHot 🔥"},
                    {"type": "mrkdwn", "text": f"*Interesse:*\n{category}"},
                    {"type": "mrkdwn", "text": f"*E-Mail:*\n{email}"},
                    {"type": "mrkdwn", "text": f"*Telefon:*\n{phone}"},
                ],
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "➡️ *Speed-to-Lead:* Lead wurde automatisch in Aircall Power Dialer eingetragen.",
                },
            },
            {"type": "divider"},
        ]
    }


def _build_call_message(
    contact_name: str,
    direction: str,
    outcome: str,
    duration_sec: int,
    timestamp: str,
    calls_7d: int,
    calls_30d: int,
    calls_365d: int,
    outbound_total_today: int = 0,
    outbound_connected_today: int = 0,
    inbound_connected_today: int = 0,
    inbound_duration_sec_today: int = 0,
    contact_id: str = "",
) -> dict[str, Any]:
    dir_emoji = "📞" if direction.lower() == "outbound" else "📲"
    mins, secs = divmod(duration_sec, 60)
    duration_str = f"{mins}m {secs}s" if mins else f"{secs}s"

    # "Meetings" = connected calls (both directions); "Anschläge" = all outbound attempts
    meetings_total = outbound_connected_today + inbound_connected_today
    inbound_dur_min = inbound_duration_sec_today // 60
    meetings_str = (
        f"{meetings_total} ({outbound_connected_today} out · {inbound_connected_today} in)"
        if meetings_total > 0 else "0"
    )

    return {
        "blocks": [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": f"{dir_emoji} Meeting — {contact_name}"},
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Outcome:*\n{outcome}"},
                    {"type": "mrkdwn", "text": f"*Direction:*\n{direction.capitalize()}"},
                    {"type": "mrkdwn", "text": f"*Duration:*\n{duration_str}"},
                    {"type": "mrkdwn", "text": f"*Time:*\n{timestamp}"},
                ],
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Anschläge heute:*\n{outbound_total_today} Outbound"},
                    {"type": "mrkdwn", "text": f"*Meetings heute:*\n{meetings_str}"},
                ],
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Calls (7d):*\n{calls_7d}"},
                    {"type": "mrkdwn", "text": f"*Calls (30d):*\n{calls_30d}"},
                    {"type": "mrkdwn", "text": f"*Calls (365d):*\n{calls_365d}"},
                ],
            },
            # HubSpot deep-link button — only shown when portal ID + contact ID are known
            *([{
                "type": "actions",
                "elements": [{
                    "type": "button",
                    "text": {"type": "plain_text", "text": "📋 In HubSpot öffnen"},
                    "url": f"https://app.hubspot.com/contacts/{HUBSPOT_PORTAL_ID}/contact/{contact_id}",
                    "action_id": "open_hubspot_contact",
                }],
            }] if contact_id and HUBSPOT_PORTAL_ID else []),
            {"type": "divider"},
        ]
    }


def _build_daily_summary(
    outbound_total: int,
    outbound_connected: int,
    inbound_connected: int,
    inbound_duration_sec: int,
    date_label: str,
) -> dict[str, Any]:
    """EOD summary card — sent once per day at 18:00 CET."""
    meetings_total = outbound_connected + inbound_connected
    conversion_pct = (outbound_connected / outbound_total * 100) if outbound_total else 0.0
    total_talk_min = inbound_duration_sec // 60

    lines = [
        f"*Outbound Anschläge:* {outbound_total}",
        f"*Outbound Meetings:* {outbound_connected}  _{conversion_pct:.1f}% conversion_",
        f"*Inbound Meetings:* {inbound_connected}",
        "─" * 30,
        f"*Meetings gesamt:* {meetings_total}",
    ]
    if inbound_connected > 0 and total_talk_min > 0:
        lines.append(f"*Inbound Gesprächszeit:* {total_talk_min}m")

    return {
        "blocks": [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": f"📊 Tages-Report — {date_label}"},
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": "\n".join(lines)},
            },
            {"type": "divider"},
        ]
    }


async def send_call_report(
    contact_name: str,
    direction: str,
    outcome: str,
    duration_sec: int,
    timestamp: str,
    calls_7d: int,
    calls_30d: int,
    calls_365d: int,
    outbound_total_today: int = 0,
    outbound_connected_today: int = 0,
    inbound_connected_today: int = 0,
    inbound_duration_sec_today: int = 0,
    contact_id: str = "",
    *,
    timeout: float = 5.0,
) -> None:
    """Post a connected-call (Meeting) card to the #sales-calls Slack channel."""
    if not SLACK_CALLS_WEBHOOK_URL:
        logger.warning("SLACK_CALLS_WEBHOOK_URL not set — skipping call report")
        return

    message = _build_call_message(
        contact_name, direction, outcome, duration_sec,
        timestamp, calls_7d, calls_30d, calls_365d,
        outbound_total_today, outbound_connected_today,
        inbound_connected_today, inbound_duration_sec_today,
        contact_id,
    )

    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.post(SLACK_CALLS_WEBHOOK_URL, json=message)

    if response.status_code != 200:
        logger.error("Slack call report failed: %s %s", response.status_code, response.text)
    else:
        logger.info("Slack call report sent for %s (direction=%s outcome=%s)", contact_name, direction, outcome)


async def send_daily_summary(
    outbound_total: int,
    outbound_connected: int,
    inbound_connected: int,
    inbound_duration_sec: int,
    *,
    timeout: float = 5.0,
) -> None:
    """Post the EOD summary card to #sales-calls at 18:00 CET."""
    if not SLACK_CALLS_WEBHOOK_URL:
        logger.warning("SLACK_CALLS_WEBHOOK_URL not set — skipping daily summary")
        return

    from datetime import datetime, timezone
    date_label = datetime.now(tz=timezone.utc).strftime("%-d. %B %Y")  # "12. März 2026"

    message = _build_daily_summary(
        outbound_total, outbound_connected, inbound_connected, inbound_duration_sec, date_label,
    )

    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.post(SLACK_CALLS_WEBHOOK_URL, json=message)

    if response.status_code != 200:
        logger.error("Slack daily summary failed: %s %s", response.status_code, response.text)
    else:
        logger.info(
            "Slack daily summary sent: outbound=%d connected=%d inbound=%d",
            outbound_total, outbound_connected, inbound_connected,
        )


def _build_decay_message(
    name: str,
    email: str,
    old_tier: str,
    new_tier: str,
    old_score: float,
    new_score: float,
    interest_category: str | None,
) -> dict[str, Any]:
    old_emoji = TIER_EMOJI.get(old_tier, "❓")
    new_emoji = TIER_EMOJI.get(new_tier, "❓")
    category  = CATEGORY_LABEL.get(interest_category or "", interest_category or "Unknown")

    return {
        "blocks": [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": f"📉 Lead Score Decay: {name}"},
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Tier:*\n{old_emoji} {old_tier} → {new_emoji} {new_tier}"},
                    {"type": "mrkdwn", "text": f"*Score:*\n{old_score:.0f} → {new_score:.0f}"},
                    {"type": "mrkdwn", "text": f"*Interesse:*\n{category}"},
                    {"type": "mrkdwn", "text": f"*E-Mail:*\n{email}"},
                ],
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "⚠️ *Inactivity decay detected.* Lead has not engaged recently — consider a re-activation campaign or remove from calling queue.",
                },
            },
            {"type": "divider"},
        ]
    }


async def send_decay_alert(
    name: str,
    email: str,
    old_tier: str,
    new_tier: str,
    old_score: float,
    new_score: float,
    interest_category: str | None = None,
    *,
    timeout: float = 5.0,
) -> None:
    """
    Post a tier-decay notification to Slack.
    Fires when the batch scorer detects a contact has dropped a tier due to inactivity.
    Uses SLACK_DECAY_WEBHOOK_URL (falls back to SLACK_WEBHOOK_URL if not set).
    """
    if not SLACK_DECAY_WEBHOOK_URL:
        logger.warning("SLACK_DECAY_WEBHOOK_URL not set — skipping decay alert")
        return

    message = _build_decay_message(name, email, old_tier, new_tier, old_score, new_score, interest_category)

    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.post(SLACK_DECAY_WEBHOOK_URL, json=message)

    if response.status_code != 200:
        logger.error("Slack decay alert failed: %s %s", response.status_code, response.text)
    else:
        logger.info(
            "Slack decay alert sent for %s: %s → %s (%.0f → %.0f)",
            email, old_tier, new_tier, old_score, new_score,
        )


async def send_hot_lead_alert(
    lead: dict[str, Any],
    combined_score: float,
    lead_tier: str,
    interest_category: str | None,
    *,
    timeout: float = 5.0,
) -> None:
    """
    Post a formatted Hot Lead alert to Slack.
    Silently skips if SLACK_WEBHOOK_URL is not configured.
    """
    if not SLACK_WEBHOOK_URL:
        logger.warning("SLACK_WEBHOOK_URL not set — skipping Slack alert")
        return

    message = _build_hot_lead_message(lead, combined_score, lead_tier, interest_category)

    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.post(SLACK_WEBHOOK_URL, json=message)

    if response.status_code != 200:
        logger.error("Slack alert failed: %s %s", response.status_code, response.text)
    else:
        logger.info("Slack Hot Lead alert sent for %s (score=%.0f)", lead.get("email"), combined_score)
