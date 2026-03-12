"""
Slack Alert Integration
Posts Hot Lead notifications to a Slack channel via Incoming Webhook.
"""

import os
import logging
from typing import Any

import httpx

logger = logging.getLogger(__name__)

SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")
SLACK_CALLS_WEBHOOK_URL = os.environ.get("SLACK_CALLS_WEBHOOK_URL", "")
# Falls back to hot-lead webhook if no dedicated channel is configured
SLACK_DECAY_WEBHOOK_URL = os.environ.get("SLACK_DECAY_WEBHOOK_URL", "") or SLACK_WEBHOOK_URL

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
    outbound_today: int = 0,
    inbound_today: int = 0,
    inbound_duration_sec_today: int = 0,
) -> dict[str, Any]:
    dir_emoji = "📞" if direction.lower() == "outbound" else "📲"
    mins, secs = divmod(duration_sec, 60)
    duration_str = f"{mins}m {secs}s" if mins else f"{secs}s"

    # Format inbound duration: "3 Meetings (12m)" or "0 Meetings"
    inbound_mins = inbound_duration_sec_today // 60
    inbound_str = (
        f"{inbound_today} Meetings ({inbound_mins}m)"
        if inbound_today > 0
        else f"{inbound_today} Meetings"
    )

    return {
        "blocks": [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": f"{dir_emoji} Call Report — {contact_name}"},
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
                    {"type": "mrkdwn", "text": f"*Outbound heute:*\n{outbound_today} Calls"},
                    {"type": "mrkdwn", "text": f"*Inbound heute:*\n{inbound_str}"},
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
    outbound_today: int = 0,
    inbound_today: int = 0,
    inbound_duration_sec_today: int = 0,
    *,
    timeout: float = 5.0,
) -> None:
    """Post a call summary to the #sales-calls Slack channel."""
    if not SLACK_CALLS_WEBHOOK_URL:
        logger.warning("SLACK_CALLS_WEBHOOK_URL not set — skipping call report")
        return

    message = _build_call_message(
        contact_name, direction, outcome, duration_sec,
        timestamp, calls_7d, calls_30d, calls_365d,
        outbound_today, inbound_today, inbound_duration_sec_today,
    )

    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.post(SLACK_CALLS_WEBHOOK_URL, json=message)

    if response.status_code != 200:
        logger.error("Slack call report failed: %s %s", response.status_code, response.text)
    else:
        logger.info("Slack call report sent for %s (direction=%s outcome=%s)", contact_name, direction, outcome)


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
