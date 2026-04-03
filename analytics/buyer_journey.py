"""
Buyer Journey Analyzer — SBC Lead Scoring

Answers: "What touchpoints do buyers have in common before they purchase?"

Data sources:
  - HubSpot deals (closed-won) → buyer emails
  - Supabase purchases table   → confirmed purchases
  - Customer.io purchase events → event-level purchase signals

For each buyer, collects all touchpoints from 90 days before purchase,
then analyzes commonalities, funnel sequences, and scoring correlations.

Runs weekly (Sunday 10:00 CET) and posts findings to Slack.
"""

import asyncio
import logging
import os
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from itertools import combinations
from typing import Any

import httpx

from integrations.supabase import get_supabase_client

logger = logging.getLogger(__name__)

HUBSPOT_BASE = "https://api.hubapi.com"
HUBSPOT_ACCESS_TOKEN = os.environ.get("HUBSPOT_ACCESS_TOKEN", "")

# Lookback window: how far before purchase to collect touchpoints
LOOKBACK_DAYS = 90

# Minimum percentage of buyers sharing a touchpoint to be "common"
COMMON_THRESHOLD_PCT = 60


# ---------------------------------------------------------------------------
# Data fetching
# ---------------------------------------------------------------------------

async def _fetch_hubspot_closed_won_emails() -> list[dict]:
    """
    Fetch all closed-won deals from HubSpot and extract associated contact emails.

    Returns list of dicts: {"email": str, "purchased_at": str (ISO), "source": "hubspot"}
    """
    if not HUBSPOT_ACCESS_TOKEN:
        logger.warning("HUBSPOT_ACCESS_TOKEN not set — skipping HubSpot deals")
        return []

    buyers: list[dict] = []
    after = None
    headers = {
        "Authorization": f"Bearer {HUBSPOT_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }

    async with httpx.AsyncClient(timeout=15.0) as client:
        # Paginate through all closed-won deals
        while True:
            body: dict[str, Any] = {
                "filterGroups": [{
                    "filters": [{
                        "propertyName": "dealstage",
                        "operator": "EQ",
                        "value": "closedwon",
                    }]
                }],
                "properties": ["dealname", "closedate", "amount"],
                "limit": 100,
            }
            if after:
                body["after"] = after

            resp = await client.post(
                f"{HUBSPOT_BASE}/crm/v3/objects/deals/search",
                headers=headers,
                json=body,
            )
            if resp.status_code != 200:
                logger.error("HubSpot deals search failed: %s %s", resp.status_code, resp.text[:300])
                break

            data = resp.json()
            deal_ids = [d["id"] for d in data.get("results", [])]

            # For each deal, get associated contacts
            for deal_id in deal_ids:
                assoc_resp = await client.get(
                    f"{HUBSPOT_BASE}/crm/v4/objects/deals/{deal_id}/associations/contacts",
                    headers=headers,
                )
                if assoc_resp.status_code != 200:
                    continue

                contact_ids = [
                    a["toObjectId"]
                    for a in assoc_resp.json().get("results", [])
                ]

                # Get deal close date
                deal_props = next(
                    (d["properties"] for d in data["results"] if d["id"] == deal_id),
                    {},
                )
                close_date = deal_props.get("closedate", "")

                # Fetch contact emails
                for cid in contact_ids:
                    c_resp = await client.get(
                        f"{HUBSPOT_BASE}/crm/v3/objects/contacts/{cid}",
                        headers=headers,
                        params={"properties": "email"},
                    )
                    if c_resp.status_code == 200:
                        email = c_resp.json().get("properties", {}).get("email")
                        if email:
                            buyers.append({
                                "email": email.lower(),
                                "purchased_at": close_date,
                                "source": "hubspot",
                            })

            # Pagination
            paging = data.get("paging", {})
            next_page = paging.get("next", {})
            after = next_page.get("after")
            if not after:
                break

    logger.info("HubSpot closed-won: %d buyer emails", len(buyers))
    return buyers


async def _fetch_supabase_purchases() -> list[dict]:
    """
    Fetch all non-refunded purchases from Supabase.

    Returns list of dicts: {"email": str, "purchased_at": str, "source": "supabase",
                            "product_name": str, "amount_chf": float}
    """
    sb = get_supabase_client()
    rows = await sb._get("purchases", {
        "select": "id,contact_id,product_name,amount_chf,purchased_at",
        "refunded_at": "is.null",
        "order": "purchased_at.desc",
    })

    if not rows:
        logger.info("Supabase purchases: 0 rows")
        return []

    # Resolve contact_ids to emails
    contact_ids = list({str(r["contact_id"]) for r in rows if r.get("contact_id")})
    email_by_id: dict[str, str] = {}

    for i in range(0, len(contact_ids), 200):
        chunk = contact_ids[i:i + 200]
        ids_csv = ",".join(chunk)
        contacts = await sb._get("contacts", {
            "select": "id,email",
            "id": f"in.({ids_csv})",
        })
        for c in contacts:
            email_by_id[str(c["id"])] = c["email"]

    buyers = []
    for r in rows:
        email = email_by_id.get(str(r.get("contact_id", "")))
        if email:
            buyers.append({
                "email": email.lower(),
                "purchased_at": r.get("purchased_at", ""),
                "source": "supabase",
                "product_name": r.get("product_name", ""),
                "amount_chf": r.get("amount_chf", 0),
            })

    logger.info("Supabase purchases: %d buyer records", len(buyers))
    return buyers


async def _fetch_supabase_purchase_events() -> list[dict]:
    """
    Fetch Customer.io purchase events from Supabase events table.

    Returns list of dicts: {"email": str, "purchased_at": str, "source": "customerio"}
    """
    sb = get_supabase_client()
    rows = await sb._get("events", {
        "select": "id,visitor_id,event_type,event_name,created_at",
        "event_type": "in.(purchase,order_completed,application_submitted)",
        "order": "created_at.desc",
    })

    if not rows:
        logger.info("Customer.io purchase events: 0 rows")
        return []

    # Resolve visitor_ids to emails
    visitor_ids = list({str(r["visitor_id"]) for r in rows if r.get("visitor_id")})
    email_by_visitor: dict[str, str] = {}

    for i in range(0, len(visitor_ids), 200):
        chunk = visitor_ids[i:i + 200]
        vids_csv = ",".join(chunk)
        contacts = await sb._get("contacts", {
            "select": "visitor_id,email",
            "visitor_id": f"in.({vids_csv})",
        })
        for c in contacts:
            vid = c.get("visitor_id")
            if vid:
                email_by_visitor[str(vid)] = c["email"]

    buyers = []
    for r in rows:
        email = email_by_visitor.get(str(r.get("visitor_id", "")))
        if email:
            buyers.append({
                "email": email.lower(),
                "purchased_at": r.get("created_at", ""),
                "source": "customerio",
            })

    logger.info("Customer.io purchase events: %d buyer records", len(buyers))
    return buyers


# ---------------------------------------------------------------------------
# Touchpoint collection for buyers
# ---------------------------------------------------------------------------

async def _collect_buyer_touchpoints(
    email: str,
    purchase_date: datetime,
) -> list[dict]:
    """
    Get all touchpoints + events for a buyer in the 90-day window before purchase.

    Merges Supabase touchpoints and events tables into a unified timeline.
    """
    sb = get_supabase_client()
    cutoff = (purchase_date - timedelta(days=LOOKBACK_DAYS)).isoformat()
    purchase_iso = purchase_date.isoformat()

    # Resolve email to contact
    contacts = await sb._get("contacts", {
        "select": "id,visitor_id",
        "email": f"eq.{email}",
        "limit": "1",
    })
    if not contacts:
        return []

    contact = contacts[0]
    contact_id = str(contact["id"])
    visitor_id = contact.get("visitor_id")

    # Fetch touchpoints (marketing attribution)
    touchpoints = await sb._get("touchpoints", {
        "select": "touchpoint_type,channel,source,medium,campaign,created_at",
        "contact_id": f"eq.{contact_id}",
        "created_at": f"gte.{cutoff}",
        "order": "created_at.asc",
    })

    # Fetch events (behavioral)
    events = []
    if visitor_id:
        events = await sb._get("events", {
            "select": "event_type,event_name,page_url,created_at",
            "visitor_id": f"eq.{visitor_id}",
            "created_at": f"gte.{cutoff}",
            "order": "created_at.asc",
        })

    # Unified timeline
    timeline: list[dict] = []

    for tp in touchpoints:
        tp_date = tp.get("created_at", "")
        if tp_date and tp_date <= purchase_iso:
            timeline.append({
                "event": tp.get("touchpoint_type") or tp.get("channel") or "unknown",
                "source": tp.get("source", ""),
                "medium": tp.get("medium", ""),
                "campaign": tp.get("campaign", ""),
                "timestamp": tp_date,
            })

    for ev in events:
        ev_date = ev.get("created_at", "")
        if ev_date and ev_date <= purchase_iso:
            timeline.append({
                "event": ev.get("event_type") or ev.get("event_name") or "unknown",
                "source": "",
                "medium": "",
                "campaign": "",
                "timestamp": ev_date,
            })

    timeline.sort(key=lambda x: x.get("timestamp", ""))
    return timeline


# ---------------------------------------------------------------------------
# Score lookup at time of purchase
# ---------------------------------------------------------------------------

async def _get_score_at_purchase(email: str) -> float | None:
    """Get the current lead_score from Supabase contacts (approximation)."""
    sb = get_supabase_client()
    rows = await sb._get("contacts", {
        "select": "lead_score",
        "email": f"eq.{email}",
        "limit": "1",
    })
    if rows and rows[0].get("lead_score") is not None:
        return float(rows[0]["lead_score"])
    return None


# ---------------------------------------------------------------------------
# Core analysis
# ---------------------------------------------------------------------------

def _parse_date(date_str: str) -> datetime | None:
    """Parse ISO date string into datetime, tolerating various formats."""
    if not date_str:
        return None
    try:
        # Handle both Z and +00:00 formats
        cleaned = date_str.replace("Z", "+00:00")
        return datetime.fromisoformat(cleaned)
    except (ValueError, TypeError):
        return None


def _extract_funnel_sequence(timeline: list[dict], max_steps: int = 6) -> str:
    """
    Extract a simplified funnel sequence from the timeline.

    Deduplicates consecutive identical events and limits to max_steps.
    """
    if not timeline:
        return ""

    steps: list[str] = []
    prev = ""
    for tp in timeline:
        event = tp.get("event", "unknown")
        if event != prev:
            steps.append(event)
            prev = event

    # Take only the last max_steps before purchase
    if len(steps) > max_steps:
        steps = steps[-max_steps:]

    return " -> ".join(steps)


async def analyze_buyer_journeys() -> dict[str, Any]:
    """
    Main analysis function.

    1. Fetch all buyers from HubSpot, Supabase, Customer.io
    2. Deduplicate by email (earliest purchase wins)
    3. For each buyer: get touchpoints from 90 days before purchase
    4. Analyze commonalities and generate scoring suggestions

    Returns structured JSON with findings.
    """
    # Step 1: Fetch buyers from all sources in parallel
    hs_buyers, sb_buyers, cio_buyers = await asyncio.gather(
        _fetch_hubspot_closed_won_emails(),
        _fetch_supabase_purchases(),
        _fetch_supabase_purchase_events(),
    )

    all_buyers = hs_buyers + sb_buyers + cio_buyers
    if not all_buyers:
        logger.warning("No buyers found across any source")
        return {
            "total_buyers": 0,
            "avg_days_to_close": 0,
            "avg_score_at_purchase": 0,
            "common_touchpoints": [],
            "top_sequences": [],
            "scoring_suggestions": [],
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

    # Step 2: Deduplicate — keep earliest purchase per email
    buyer_map: dict[str, dict] = {}
    for b in all_buyers:
        email = b["email"]
        purchase_date = _parse_date(b.get("purchased_at", ""))
        if not purchase_date:
            continue

        if email not in buyer_map or purchase_date < _parse_date(buyer_map[email]["purchased_at"]):
            buyer_map[email] = b

    unique_emails = list(buyer_map.keys())
    logger.info("Unique buyers after dedup: %d (from %d raw)", len(unique_emails), len(all_buyers))

    # Step 3: Collect touchpoints + scores for each buyer (batched for performance)
    buyer_timelines: dict[str, list[dict]] = {}
    buyer_scores: dict[str, float | None] = {}
    days_to_close: list[float] = []

    # Process in batches of 10 to avoid overwhelming APIs
    batch_size = 10
    for i in range(0, len(unique_emails), batch_size):
        batch = unique_emails[i:i + batch_size]
        tasks = []
        for email in batch:
            purchase_date = _parse_date(buyer_map[email]["purchased_at"])
            if purchase_date:
                tasks.append(_collect_buyer_touchpoints(email, purchase_date))
            else:
                tasks.append(asyncio.coroutine(lambda: [])())

        score_tasks = [_get_score_at_purchase(email) for email in batch]

        timelines, scores = await asyncio.gather(
            asyncio.gather(*tasks),
            asyncio.gather(*score_tasks),
        )

        for email, timeline, score in zip(batch, timelines, scores):
            buyer_timelines[email] = timeline
            buyer_scores[email] = score

            # Calculate days from first touchpoint to purchase
            if timeline:
                first_tp_date = _parse_date(timeline[0].get("timestamp", ""))
                purchase_date = _parse_date(buyer_map[email]["purchased_at"])
                if first_tp_date and purchase_date:
                    delta = (purchase_date - first_tp_date).total_seconds() / 86400
                    if delta >= 0:
                        days_to_close.append(delta)

    # Step 4: Analyze commonalities
    total_buyers = len(unique_emails)
    if total_buyers == 0:
        return {
            "total_buyers": 0,
            "avg_days_to_close": 0,
            "avg_score_at_purchase": 0,
            "common_touchpoints": [],
            "top_sequences": [],
            "scoring_suggestions": [],
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

    # 4a: Which events do >= COMMON_THRESHOLD_PCT of buyers share?
    event_counts: Counter = Counter()
    event_avg_days_before: defaultdict[str, list[float]] = defaultdict(list)

    for email, timeline in buyer_timelines.items():
        purchase_date = _parse_date(buyer_map[email]["purchased_at"])
        if not purchase_date:
            continue

        # Count unique events per buyer (not total occurrences)
        seen_events: set[str] = set()
        for tp in timeline:
            event = tp.get("event", "unknown")
            if event not in seen_events:
                seen_events.add(event)
                event_counts[event] += 1

                tp_date = _parse_date(tp.get("timestamp", ""))
                if tp_date:
                    days_before = (purchase_date - tp_date).total_seconds() / 86400
                    event_avg_days_before[event].append(days_before)

    common_touchpoints: list[dict] = []
    for event, count in event_counts.most_common():
        pct = round(count / total_buyers * 100)
        if pct >= COMMON_THRESHOLD_PCT:
            days_list = event_avg_days_before.get(event, [])
            avg_days = round(sum(days_list) / len(days_list), 1) if days_list else 0
            common_touchpoints.append({
                "event": event,
                "pct": pct,
                "count": count,
                "avg_days_before": avg_days,
            })

    # 4b: Most common funnel sequences
    sequence_counter: Counter = Counter()
    for email, timeline in buyer_timelines.items():
        seq = _extract_funnel_sequence(timeline)
        if seq:
            sequence_counter[seq] += 1

    top_sequences: list[str] = []
    for seq, count in sequence_counter.most_common(5):
        pct = round(count / total_buyers * 100)
        top_sequences.append(f"{seq} ({pct}%)")

    # 4c: Score distribution at purchase
    valid_scores = [s for s in buyer_scores.values() if s is not None]
    avg_score = round(sum(valid_scores) / len(valid_scores), 1) if valid_scores else 0

    # Score buckets for distribution
    score_buckets: Counter = Counter()
    for s in valid_scores:
        if s >= 65:
            score_buckets["hot (>=65)"] += 1
        elif s >= 30:
            score_buckets["warm (30-64)"] += 1
        else:
            score_buckets["cold (<30)"] += 1

    # 4d: Average days to close
    avg_days = round(sum(days_to_close) / len(days_to_close), 1) if days_to_close else 0

    # Step 5: Generate scoring suggestions
    scoring_suggestions = _generate_scoring_suggestions(
        common_touchpoints, avg_score, avg_days, total_buyers,
    )

    result = {
        "total_buyers": total_buyers,
        "avg_days_to_close": avg_days,
        "avg_score_at_purchase": avg_score,
        "score_distribution": dict(score_buckets),
        "common_touchpoints": common_touchpoints,
        "top_sequences": top_sequences,
        "scoring_suggestions": scoring_suggestions,
        "sources": {
            "hubspot_deals": len(hs_buyers),
            "supabase_purchases": len(sb_buyers),
            "customerio_events": len(cio_buyers),
        },
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }

    logger.info(
        "Buyer journey analysis complete: %d buyers, avg %.1f days to close, avg score %.1f",
        total_buyers, avg_days, avg_score,
    )
    return result


# ---------------------------------------------------------------------------
# Scoring suggestions engine
# ---------------------------------------------------------------------------

# Current base points from scoring/engagement.py (keep in sync)
_CURRENT_POINTS: dict[str, int] = {
    "checkout_visited": 40,
    "application_submitted": 35,
    "video_watched_100": 30,
    "video_watched_75": 25,
    "webinar_attended": 25,
    "sales_page_visited": 20,
    "price_info_viewed": 20,
    "webinar_registered": 15,
    "video_watched_50": 15,
    "cta_clicked": 12,
    "email_link_clicked": 10,
    "free_resource_downloaded": 10,
    "email_opened": 5,
    "page_visited": 3,
}


def _generate_scoring_suggestions(
    common_touchpoints: list[dict],
    avg_score: float,
    avg_days: float,
    total_buyers: int,
) -> list[str]:
    """
    Generate actionable scoring weight suggestions based on buyer journey data.

    Compares event correlation percentages with current point weights
    to find misalignments.
    """
    suggestions: list[str] = []

    if not common_touchpoints:
        return ["Not enough data to generate suggestions. Need more closed deals."]

    for tp in common_touchpoints:
        event = tp["event"]
        pct = tp["pct"]
        current_points = _CURRENT_POINTS.get(event)

        if current_points is None:
            suggestions.append(
                f"NEW: '{event}' appears in {pct}% of buyer journeys "
                f"(avg {tp['avg_days_before']}d before purchase) — "
                f"consider adding it to the scoring model"
            )
            continue

        # High-correlation events that may be underweighted
        if pct >= 80 and current_points < 30:
            suggested = min(current_points + 10, 40)
            suggestions.append(
                f"Increase '{event}' from {current_points} to {suggested} "
                f"({pct}% correlation with purchase)"
            )
        elif pct >= 70 and current_points < 20:
            suggested = current_points + 5
            suggestions.append(
                f"Consider increasing '{event}' from {current_points} to {suggested} "
                f"({pct}% correlation)"
            )

    # Score threshold suggestions
    if avg_score > 0 and avg_score < 50:
        suggestions.append(
            f"Avg score at purchase is {avg_score} — consider lowering the 'warm' "
            f"threshold (currently 30) to capture more potential buyers earlier"
        )
    elif avg_score > 80:
        suggestions.append(
            f"Avg score at purchase is {avg_score} — scoring weights are well-calibrated "
            f"for identifying buyers"
        )

    # Timing suggestion
    if avg_days > 0 and avg_days < 7:
        suggestions.append(
            f"Buyers convert fast (avg {avg_days}d) — prioritize immediate follow-up "
            f"for newly scored warm/hot leads"
        )
    elif avg_days > 30:
        suggestions.append(
            f"Long sales cycle (avg {avg_days}d) — ensure nurturing sequences "
            f"cover at least {int(avg_days)}+ days"
        )

    return suggestions


# ---------------------------------------------------------------------------
# Slack formatting
# ---------------------------------------------------------------------------

def build_buyer_journey_slack_message(analysis: dict[str, Any]) -> dict[str, Any]:
    """
    Format the buyer journey analysis as a Slack Block Kit message.
    """
    total = analysis.get("total_buyers", 0)
    avg_days = analysis.get("avg_days_to_close", 0)
    avg_score = analysis.get("avg_score_at_purchase", 0)
    common = analysis.get("common_touchpoints", [])
    sequences = analysis.get("top_sequences", [])
    suggestions = analysis.get("scoring_suggestions", [])
    sources = analysis.get("sources", {})
    score_dist = analysis.get("score_distribution", {})

    blocks: list[dict] = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": "Weekly Buyer Journey Analysis",
                "emoji": True,
            },
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Total Buyers:*\n{total}"},
                {"type": "mrkdwn", "text": f"*Avg Days to Close:*\n{avg_days}"},
                {"type": "mrkdwn", "text": f"*Avg Score at Purchase:*\n{avg_score}"},
                {
                    "type": "mrkdwn",
                    "text": (
                        f"*Sources:*\nHS: {sources.get('hubspot_deals', 0)} | "
                        f"SB: {sources.get('supabase_purchases', 0)} | "
                        f"CIO: {sources.get('customerio_events', 0)}"
                    ),
                },
            ],
        },
    ]

    # Score distribution
    if score_dist:
        dist_lines = [f"  {k}: {v}" for k, v in score_dist.items()]
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*Score Distribution at Purchase:*\n" + "\n".join(dist_lines),
            },
        })

    blocks.append({"type": "divider"})

    # Common touchpoints
    if common:
        tp_lines = []
        for tp in common[:8]:  # Limit to top 8
            tp_lines.append(
                f"  *{tp['event']}* — {tp['pct']}% of buyers, "
                f"avg {tp['avg_days_before']}d before purchase"
            )
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*Common Touchpoints (>= 60%):*\n" + "\n".join(tp_lines),
            },
        })
    else:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*Common Touchpoints:*\nNo touchpoints shared by >= 60% of buyers yet.",
            },
        })

    blocks.append({"type": "divider"})

    # Top sequences
    if sequences:
        seq_lines = [f"  {i+1}. {s}" for i, s in enumerate(sequences)]
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*Top Funnel Sequences:*\n" + "\n".join(seq_lines),
            },
        })

    blocks.append({"type": "divider"})

    # Scoring suggestions
    if suggestions:
        sug_lines = [f"  :bulb: {s}" for s in suggestions[:5]]
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*Scoring Suggestions:*\n" + "\n".join(sug_lines),
            },
        })

    blocks.append({
        "type": "context",
        "elements": [{
            "type": "mrkdwn",
            "text": f"Generated {analysis.get('generated_at', 'N/A')} | "
                    f"Lookback: {LOOKBACK_DAYS} days | "
                    f"Threshold: {COMMON_THRESHOLD_PCT}%",
        }],
    })

    return {"blocks": blocks}


async def post_buyer_journey_to_slack(
    analysis: dict[str, Any],
    *,
    timeout: float = 10.0,
) -> None:
    """
    Post buyer journey analysis to Slack.

    Uses SLACK_ANALYTICS_WEBHOOK_URL if set, falls back to SLACK_WEBHOOK_URL.
    """
    webhook_url = (
        os.environ.get("SLACK_ANALYTICS_WEBHOOK_URL")
        or os.environ.get("SLACK_WEBHOOK_URL", "")
    )
    if not webhook_url:
        logger.warning("No Slack webhook URL configured — skipping buyer journey post")
        return

    message = build_buyer_journey_slack_message(analysis)

    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.post(webhook_url, json=message)

    if response.status_code != 200:
        logger.error(
            "Slack buyer journey post failed: %s %s",
            response.status_code, response.text,
        )
    else:
        logger.info(
            "Buyer journey analysis posted to Slack (%d buyers, avg %.1f days)",
            analysis.get("total_buyers", 0),
            analysis.get("avg_days_to_close", 0),
        )


# ---------------------------------------------------------------------------
# S1 Self-Learning: Call-Outcome vs Tier Calibration
# ---------------------------------------------------------------------------

_TIER_LABELS = {
    "1_hot": "Hot",
    "2_warm": "Warm",
    "3_cold": "Cold",
    "4_disqualified": "Disqualified",
}

_CALL_OUTCOME_POSITIVE = {"Kontakt aufgenommen", "Termin vereinbart", "Angebot gemacht"}


async def _fetch_contacts_with_call_outcomes(limit: int = 500) -> list[dict]:
    """
    Fetch HubSpot contacts that have had at least one call, with their tier and score.
    Returns list of dicts with: tier, score, call_outcome, call_date
    """
    if not HUBSPOT_ACCESS_TOKEN:
        return []

    headers = {
        "Authorization": f"Bearer {HUBSPOT_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }
    properties = [
        "lead_tier",
        "lead_combined_score",
        "lead_last_call_outcome",
        "lead_last_call_date",
        "lead_call_booked",
        "email",
    ]
    contacts: list[dict] = []
    after = None

    async with httpx.AsyncClient(timeout=20.0) as client:
        while len(contacts) < limit:
            body: dict[str, Any] = {
                "filterGroups": [{
                    "filters": [{
                        "propertyName": "lead_last_call_outcome",
                        "operator": "HAS_PROPERTY",
                    }]
                }],
                "properties": properties,
                "limit": 100,
            }
            if after:
                body["after"] = after

            resp = await client.post(
                f"{HUBSPOT_BASE}/crm/v3/objects/contacts/search",
                headers=headers,
                json=body,
            )
            if resp.status_code != 200:
                logger.error("HubSpot call-outcome fetch failed: %s", resp.status_code)
                break

            data = resp.json()
            for r in data.get("results", []):
                props = r.get("properties", {})
                contacts.append({
                    "tier": props.get("lead_tier", "unknown"),
                    "score": float(props.get("lead_combined_score") or 0),
                    "outcome": props.get("lead_last_call_outcome", ""),
                    "call_date": props.get("lead_last_call_date", ""),
                    "booked": props.get("lead_call_booked") == "true",
                })

            paging = data.get("paging", {})
            after = paging.get("next", {}).get("after")
            if not after:
                break

    logger.info("S1: fetched %d contacts with call outcomes", len(contacts))
    return contacts


def _analyze_call_calibration(contacts: list[dict]) -> dict[str, Any]:
    """
    For each tier: connection rate, booking rate, avg score.
    Generates calibration recommendations.
    """
    by_tier: dict[str, list[dict]] = defaultdict(list)
    for c in contacts:
        by_tier[c["tier"]].append(c)

    tier_stats: list[dict] = []
    for tier, rows in sorted(by_tier.items()):
        total = len(rows)
        connected = sum(1 for r in rows if r["outcome"] in _CALL_OUTCOME_POSITIVE)
        booked = sum(1 for r in rows if r["booked"])
        avg_score = sum(r["score"] for r in rows) / total if total else 0
        tier_stats.append({
            "tier": tier,
            "label": _TIER_LABELS.get(tier, tier),
            "total_calls": total,
            "connection_rate": round(connected / total * 100, 1) if total else 0,
            "booking_rate": round(booked / total * 100, 1) if total else 0,
            "avg_score": round(avg_score, 1),
        })

    # Generate calibration recommendations
    recommendations: list[str] = []
    for s in tier_stats:
        if s["tier"] == "1_hot" and s["connection_rate"] < 40:
            recommendations.append(
                f"Hot-Schwelle zu niedrig: {s['connection_rate']}% Verbindungsrate bei Hot-Leads "
                f"(avg Score {s['avg_score']}) — Schwelle auf ≥70 erhöhen?"
            )
        if s["tier"] == "2_warm" and s["connection_rate"] > 70:
            recommendations.append(
                f"Warm-Leads performen wie Hot: {s['connection_rate']}% Verbindungsrate "
                f"(avg Score {s['avg_score']}) — Warm-Schwelle nach unten anpassen?"
            )
        if s["tier"] == "1_hot" and s["booking_rate"] > 30:
            recommendations.append(
                f"Hot-Leads konvertieren stark: {s['booking_rate']}% Buchungsrate "
                f"— Score-Kalibrierung funktioniert."
            )
        if s["tier"] == "3_cold" and s["connection_rate"] > 30:
            recommendations.append(
                f"Cold-Leads mit {s['connection_rate']}% Verbindungsrate — "
                f"Cold-Schwelle prüfen (avg Score {s['avg_score']})"
            )

    if not recommendations:
        recommendations.append(
            "Score-Kalibrierung im grünen Bereich — keine Anpassungen empfohlen."
        )

    return {
        "total_contacts": len(contacts),
        "tier_stats": tier_stats,
        "recommendations": recommendations,
    }


def _build_calibration_slack_message(calibration: dict[str, Any]) -> dict[str, Any]:
    """Build Slack Block Kit message for S1 calibration report."""
    total = calibration["total_contacts"]
    tier_stats = calibration["tier_stats"]
    recommendations = calibration["recommendations"]

    blocks: list[dict] = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": "Score-Kalibrierung (S1 Self-Learning)", "emoji": True},
        },
        {
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": f"Basis: *{total}* Kontakte mit Call-Outcomes — automatische Analyse"}],
        },
        {"type": "divider"},
    ]

    # Tier performance table
    for s in tier_stats:
        conn_emoji = "🟢" if s["connection_rate"] >= 50 else ("🟡" if s["connection_rate"] >= 30 else "🔴")
        book_emoji = "🟢" if s["booking_rate"] >= 20 else ("🟡" if s["booking_rate"] >= 10 else "🔴")
        blocks.append({
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*{s['label']} ({s['total_calls']} Calls)*"},
                {"type": "mrkdwn", "text": f"Avg Score: *{s['avg_score']}*"},
                {"type": "mrkdwn", "text": f"{conn_emoji} Verbindung: *{s['connection_rate']}%*"},
                {"type": "mrkdwn", "text": f"{book_emoji} Buchungen: *{s['booking_rate']}%*"},
            ],
        })

    blocks.append({"type": "divider"})

    # Recommendations
    rec_text = "\n".join(f"• {r}" for r in recommendations)
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": f"*Empfehlungen*\n{rec_text}"},
    })

    return {"blocks": blocks}


async def run_call_calibration() -> None:
    """S1 Self-Learning: analyze call outcomes vs tiers, post to Slack."""
    logger.info("S1: Starting call calibration analysis...")
    try:
        contacts = await _fetch_contacts_with_call_outcomes()
        if not contacts:
            logger.warning("S1: No contacts with call outcomes found — skipping")
            return

        calibration = _analyze_call_calibration(contacts)
        message = _build_calibration_slack_message(calibration)

        webhook_url = (
            os.environ.get("SLACK_CALLS_WEBHOOK_URL")
            or os.environ.get("SLACK_WEBHOOK_URL", "")
        )
        if not webhook_url:
            logger.warning("S1: No Slack webhook configured — skipping post")
            return

        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(webhook_url, json=message)

        if resp.status_code != 200:
            logger.error("S1: Slack post failed: %s %s", resp.status_code, resp.text)
        else:
            logger.info(
                "S1: Calibration posted — %d tiers, %d recommendations",
                len(calibration["tier_stats"]),
                len(calibration["recommendations"]),
            )
    except Exception:
        logger.exception("S1: Call calibration failed")


# ---------------------------------------------------------------------------
# Scheduled job entry point
# ---------------------------------------------------------------------------

async def run_weekly_buyer_journey() -> None:
    """
    Scheduled job: runs buyer journey analysis + S1 call calibration, posts to Slack.
    Called by APScheduler Mon/Wed/Fri at 10:00 CET.
    """
    logger.info("Starting buyer journey + S1 calibration analysis...")
    try:
        analysis, _ = await asyncio.gather(
            analyze_buyer_journeys(),
            run_call_calibration(),
            return_exceptions=True,
        )
        if isinstance(analysis, Exception):
            raise analysis
        await post_buyer_journey_to_slack(analysis)
        logger.info("Weekly buyer journey + S1 complete")
    except Exception:
        logger.exception("Weekly buyer journey analysis failed")
