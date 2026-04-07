"""
Slack Alert Integration
Posts Hot Lead notifications, decay alerts, and batch health reports
to Slack channels via Incoming Webhooks.
"""

import os
import logging
from dataclasses import dataclass, field
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
    name = f"{lead.get('firstname', '')} {lead.get('lastname', '')}".strip() or "Unbekannt"
    email = lead.get("email", "–")
    phone = lead.get("phone", "–")
    emoji = TIER_EMOJI.get(lead_tier, "❓")
    tier_label = {"1_hot": "Hot", "2_warm": "Warm", "3_cold": "Cold", "4_disqualified": "Disqualifiziert"}.get(lead_tier, lead_tier)
    category = CATEGORY_LABEL.get(interest_category or "", interest_category or "Unbekannt")
    engagement = lead.get("engagement_score", 0)
    wa_contribution = max(0, int(combined_score) - int(engagement))
    contact_id = lead.get("contact_id", "")
    funnel_source = lead.get("funnel_source", "")
    is_fresh = lead.get("is_fresh", False)
    fresh_badge = " ⚡ FRISCH" if is_fresh else ""

    # Score breakdown line
    score_breakdown = f"Engagement: {engagement} | WA-Bonus: +{wa_contribution}"

    blocks: list[dict] = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"{emoji} {tier_label} Lead: {name}{fresh_badge}"},
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Score:*\n{combined_score:.0f} Punkte"},
                {"type": "mrkdwn", "text": f"*Interesse:*\n{category}"},
                {"type": "mrkdwn", "text": f"*Quelle:*\n{funnel_source or 'organisch/CIO'}"},
            ],
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*E-Mail:*\n{email}"},
                {"type": "mrkdwn", "text": f"*Telefon:*\n{phone}"},
            ],
        },
        {
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": f"📊 Score-Breakdown: {score_breakdown}"}],
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "➡️ *Speed-to-Lead:* Lead wurde automatisch in den Aircall Power Dialer eingetragen.",
            },
        },
    ]

    # HubSpot deep-link button
    if contact_id and HUBSPOT_PORTAL_ID:
        blocks.append({
            "type": "actions",
            "elements": [{
                "type": "button",
                "text": {"type": "plain_text", "text": "📋 In HubSpot öffnen"},
                "url": f"https://app.hubspot.com/contacts/{HUBSPOT_PORTAL_ID}/contact/{contact_id}",
                "action_id": "open_hubspot_hot_lead",
            }],
        })

    blocks.append({"type": "divider"})
    return {"blocks": blocks}



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
    old_label = {"1_hot": "Hot", "2_warm": "Warm", "3_cold": "Cold", "4_disqualified": "Disqualifiziert"}.get(old_tier, old_tier)
    new_label = {"1_hot": "Hot", "2_warm": "Warm", "3_cold": "Cold", "4_disqualified": "Disqualifiziert"}.get(new_tier, new_tier)
    category  = CATEGORY_LABEL.get(interest_category or "", interest_category or "Unbekannt")

    return {
        "blocks": [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": f"📉 Score Decay: {name}"},
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Tier:*\n{old_emoji} {old_label} → {new_emoji} {new_label}"},
                    {"type": "mrkdwn", "text": f"*Score:*\n{old_score:.0f} → {new_score:.0f}"},
                    {"type": "mrkdwn", "text": f"*Interesse:*\n{category}"},
                    {"type": "mrkdwn", "text": f"*E-Mail:*\n{email}"},
                ],
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "⚠️ *Score-Rückgang durch Inaktivität.* Lead hat sich länger nicht mehr gemeldet — Re-Aktivierungskampagne prüfen oder aus der Calling-Queue entfernen.",
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


# ---------------------------------------------------------------------------
# Batch health report
# ---------------------------------------------------------------------------

@dataclass
class BatchRunStats:
    """Metrics collected during run_batch_scoring() — passed to send_batch_report()."""
    leads_fetched: int = 0
    leads_processed: int = 0
    hs_updates_ok: int = 0
    hs_chunk_errors: int = 0          # HubSpot batch chunks that returned non-2xx
    hs_error_samples: list[str] = field(default_factory=list)   # first 2 error msgs
    aircall_pushed: int = 0
    aircall_rejected: int = 0
    notes_written: int = 0
    skipped_cold: int = 0
    skipped_dnc: int = 0
    duration_seconds: float = 0.0
    fatal_error: str | None = None    # set if batch crashed before completing


def _build_batch_report_message(stats: BatchRunStats) -> dict[str, Any]:
    """Build a Slack Block Kit message for the batch run health report."""
    ok = stats.fatal_error is None and stats.hs_chunk_errors == 0
    status_emoji = "✅" if ok else ("💥" if stats.fatal_error else "⚠️")
    status_text = "OK" if ok else ("FATAL" if stats.fatal_error else "ERRORS")

    mins, secs = divmod(int(stats.duration_seconds), 60)
    duration_str = f"{mins}m {secs}s" if mins else f"{secs}s"

    header = f"{status_emoji} Batch Run — {status_text} ({duration_str})"

    lines = [
        f"*Leads:* {stats.leads_fetched} fetched → {stats.leads_processed} processed",
        f"*HubSpot:* {stats.hs_updates_ok} updated",
        f"*Aircall:* {stats.aircall_pushed} pushed, {stats.aircall_rejected} rejected",
        f"*Skipped:* {stats.skipped_cold} cold, {stats.skipped_dnc} DNC",
    ]

    if stats.hs_chunk_errors:
        lines.append(
            f":rotating_light: *{stats.hs_chunk_errors} HubSpot chunk error(s)*"
        )
        for sample in stats.hs_error_samples[:2]:
            # Truncate long error bodies — just show the key message
            truncated = sample[:200] + "…" if len(sample) > 200 else sample
            lines.append(f"  › `{truncated}`")

    if stats.fatal_error:
        lines.append(f":skull: *Fatal:* `{stats.fatal_error[:300]}`")

    body = "\n".join(lines)

    return {
        "blocks": [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": header, "emoji": True},
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": body},
            },
        ]
    }


async def send_batch_report(
    stats: BatchRunStats,
    *,
    timeout: float = 5.0,
) -> None:
    """
    Post a batch-run health report to Slack after every run_batch_scoring() call.

    Uses SLACK_WEBHOOK_URL. Silently skips if not configured.
    Always fires — both on success and on error — so missing reports are
    themselves a signal that something is wrong.
    """
    if not SLACK_WEBHOOK_URL:
        logger.debug("SLACK_WEBHOOK_URL not set — skipping batch report")
        return

    message = _build_batch_report_message(stats)
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(SLACK_WEBHOOK_URL, json=message)
        if response.status_code != 200:
            logger.error("Slack batch report failed: %s %s", response.status_code, response.text)
        else:
            logger.info(
                "Slack batch report sent: %d leads, %d hs_ok, %d chunk_errors, %d aircall",
                stats.leads_fetched, stats.hs_updates_ok, stats.hs_chunk_errors, stats.aircall_pushed,
            )
    except Exception as exc:
        # Never let Slack reporting crash the batch
        logger.warning("Slack batch report exception (non-fatal): %s", exc)
