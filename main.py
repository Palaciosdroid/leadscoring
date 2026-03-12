"""
SBC Lead Scoring Engine — FastAPI
Receives Customer.io webhooks, scores leads, writes back to HubSpot,
triggers Aircall Power Dialer + Slack alerts for Hot/Warm Leads.

Deploy: Railway (~$5-10/Mo)
"""

import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI, Header, HTTPException, Request
from pydantic import BaseModel, Field

from batch.scorer import run_batch_scoring
from integrations.hubspot import (
    upsert_contact_score,
    get_call_stats,
    get_latest_call_for_contact,
    get_prioritized_contacts,
    write_call_outcome,
)
from integrations.aircall import add_to_power_dialer
from integrations.slack import send_hot_lead_alert, send_call_report
from scoring.combined import combine_scores
from scoring.engagement import calculate_engagement_score
from scoring.interest import detect_interest_category

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Customer.io event → internal event_type mapping
# ---------------------------------------------------------------------------
CIO_EVENT_MAP: dict[str, str | None] = {
    # Page events (matched by URL pattern in webhook handler)
    "page":                   "page_visited",
    # Email — CIO fires both names depending on version
    "email_opened":           "email_opened",
    "email_link_clicked":     "email_link_clicked",
    "email_clicked":          "email_link_clicked",   # CIO alias
    "email_unsubscribed":     "email_unsubscribed",
    "unsubscribed":           "email_unsubscribed",   # CIO alias
    # Video — progress resolved via threshold, complete = 100%
    "video_progress":         None,          # resolved via threshold below
    "video_complete":         "video_watched_100",
    # Click events — URL-resolved below (CTA vs generic)
    "click":                  None,          # resolved via URL below
    # Webinar / resources / forms
    "webinar_attended":       "webinar_attended",
    "webinar_registered":     "webinar_registered",
    "resource_downloaded":    "free_resource_downloaded",
    # CRITICAL: Customer.io fires "form_submit", NOT "application_submitted"
    "form_submit":            "application_submitted",
    "application_submitted":  "application_submitted",   # keep for manual/direct triggers
}

# ---------------------------------------------------------------------------
# Customer.io Reporting Webhook metric → internal event_type mapping
# These are the "metric" values CIO sends in reporting webhooks
# ---------------------------------------------------------------------------
CIO_METRIC_MAP: dict[str, str | None] = {
    "opened":        "email_opened",
    "clicked":       "email_link_clicked",
    "converted":     "application_submitted",
    "unsubscribed":  "email_unsubscribed",
    # Delivery/technical events — not useful for scoring
    "sent":          None,
    "delivered":     None,
    "bounced":       None,
    "dropped":       None,
    "spammed":       None,
    "failed":        None,
    "attempted":     None,
    "drafted":       None,
}

# ---------------------------------------------------------------------------
# HubSpot call disposition UUIDs → readable labels
# (HubSpot stores call outcomes as internal UUIDs — map them here)
# ---------------------------------------------------------------------------
HS_DISPOSITION_MAP: dict[str, str] = {
    # UUIDs verified against HubSpot /calling/v1/dispositions (German portal)
    "f240bbac-87c9-4f6e-bf70-924b57d47db7": "Kontakt aufgenommen",   # Connected
    "b2cf5968-551e-4856-9783-52b3da59a7d0": "Voicemail hinterlassen",  # Left voicemail
    "a4c4c377-d246-4b32-a13b-75a56a4cd0ff": "Live-Nachricht hinterlassen",  # Left live msg
    "73a0d17f-1163-4015-bdd5-ec830791da20": "Keine Antwort",          # No answer
    "9d9162e7-6cf3-4944-bf63-4dff82258764": "Besetzt",                # Busy
    "17b47fee-58de-441e-a44c-c6300d46f273": "Falsche Nummer",         # Wrong number
}

CHECKOUT_URL_PATTERNS  = ("checkout", "warenkorb", "order", "buy")
SALES_PAGE_PATTERNS    = ("ausbildung", "coaching", "kurs", "programm", "product")
PRICE_INFO_PATTERNS    = ("preis", "price", "invest", "kosten", "cost")


def _map_cio_event(raw_event: dict[str, Any]) -> str | None:
    """Translate a raw Customer.io event dict to our internal event_type."""
    event_name = raw_event.get("event", "")
    url = (raw_event.get("data", {}) or {}).get("page", {}).get("url", "").lower()

    # Page events — resolve by URL
    if event_name == "page":
        if any(p in url for p in CHECKOUT_URL_PATTERNS):
            return "checkout_visited"
        if any(p in url for p in PRICE_INFO_PATTERNS):
            return "price_info_viewed"
        if any(p in url for p in SALES_PAGE_PATTERNS):
            return "sales_page_visited"
        return "page_visited"

    # Video progress — resolve by threshold
    if event_name == "video_progress":
        pct = (raw_event.get("data", {}) or {}).get("percent_complete", 0)
        if pct >= 75:
            return "video_watched_75"
        elif pct >= 50:
            return "video_watched_50"
        return None  # below 50% — ignore

    # Click events — resolve by URL (CTA-level detail)
    if event_name == "click":
        if any(p in url for p in CHECKOUT_URL_PATTERNS):
            return "checkout_visited"     # clicking into checkout = strong signal
        if any(p in url for p in PRICE_INFO_PATTERNS):
            return "price_info_viewed"
        if any(p in url for p in SALES_PAGE_PATTERNS):
            return "cta_clicked"          # CTA on sales page
        return None                       # generic click — not worth scoring

    return CIO_EVENT_MAP.get(event_name)


# ---------------------------------------------------------------------------
# Batch scoring scheduler (every 30 minutes)
# ---------------------------------------------------------------------------
scheduler = AsyncIOScheduler()

BATCH_INTERVAL_MINUTES = int(os.environ.get("BATCH_INTERVAL_MINUTES", "30"))


@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler.add_job(
        run_batch_scoring,
        "interval",
        minutes=BATCH_INTERVAL_MINUTES,
        id="batch_scoring",
        replace_existing=True,
    )
    scheduler.start()
    logger.info("Batch scoring scheduler started (every %d min)", BATCH_INTERVAL_MINUTES)
    yield
    scheduler.shutdown()


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
app = FastAPI(
    title="SBC Lead Scoring Engine",
    description="B2C behaviour-based lead scoring for SBC Coaching Ausbildungen",
    version="1.0.0",
    lifespan=lifespan,
)

WEBHOOK_SECRET = os.environ.get("CIO_WEBHOOK_SECRET", "")


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------
class LeadContext(BaseModel):
    """Minimal lead info sent alongside scoring event for Slack/Aircall."""
    contact_id: str = Field(..., description="HubSpot contact ID")
    email: str = ""
    firstname: str = ""
    lastname: str = ""
    phone: str = ""
    funnel_source: str = Field(
        default="",
        description="Which CIO campaign/funnel brought this lead. Written to lead_funnel_source in HubSpot.",
    )
    created_at: datetime | None = Field(
        default=None,
        description="UTC datetime when lead opted in. Used for Aircall fresh-list routing.",
    )


class ScoreResponse(BaseModel):
    contact_id: str
    engagement_score: int
    ai_score: float | None
    combined_score: float
    lead_tier: str
    interest_category: str | None
    hubspot_updated: bool
    dialer_added: bool


class HubSpotCallPayload(BaseModel):
    """
    Properties HubSpot sends via Workflow → Custom Webhook when a call is logged.

    contact_id is the HubSpot contact object ID (hs_object_id).
    When present, the endpoint fetches live call details directly from HubSpot
    so the workflow only needs to send contact_id + name — no call properties needed.
    """
    contact_id:          str = ""              # HubSpot contact ID (hs_object_id)
    contact_firstname:   str = ""
    contact_lastname:    str = ""
    hs_call_direction:   str = "OUTBOUND"      # OUTBOUND | INBOUND
    hs_call_disposition: str = ""              # UUID — mapped via HS_DISPOSITION_MAP
    hs_call_duration:    int = 0               # milliseconds
    hs_timestamp:        int = 0               # Unix milliseconds


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------
@app.get("/health")
async def health():
    return {"status": "ok", "version": "1.0.0"}


@app.post("/webhook/customerio")
async def customerio_webhook(
    request: Request,
    x_cio_signature: str | None = Header(default=None),
):
    """
    Receive a Customer.io webhook — supports TWO formats:

    1. CIO Reporting Webhook (actual CIO format):
       { "event_id": "...", "metric": "clicked", "object_type": "email",
         "timestamp": 1234, "data": { "customer_id": "...", "identifiers": {...} } }

    2. Custom batch format (manual/direct triggers):
       { "events": [...], "lead": { "contact_id": "...", ... } }
    """
    raw_body = await request.body()

    if WEBHOOK_SECRET:
        if not x_cio_signature:
            raise HTTPException(status_code=401, detail="Missing webhook signature")
        _verify_signature(raw_body, x_cio_signature)

    import json
    body = json.loads(raw_body)

    # --- Detect format: CIO Reporting Webhook vs custom batch vs test/ping ---
    if "metric" in body and "object_type" in body:
        # CIO Reporting Webhook format
        return await _handle_cio_reporting_webhook(body)
    elif "lead" in body and body.get("lead", {}).get("contact_id"):
        # Custom batch format (manual/direct triggers)
        return await _handle_custom_batch(body)
    else:
        # Test/ping payload or unrecognized format — acknowledge with 200
        logger.info("Webhook received unrecognized payload (test/ping?): keys=%s", list(body.keys()))
        return {"status": "ok", "message": "webhook received", "format": "unrecognized"}


async def _handle_cio_reporting_webhook(body: dict[str, Any]) -> ScoreResponse:
    """Handle Customer.io's native Reporting Webhook format."""
    metric = body.get("metric", "")
    data = body.get("data", {}) or {}

    # Map CIO metric → internal event type
    event_type = CIO_METRIC_MAP.get(metric)
    if not event_type:
        logger.info("Ignoring CIO metric=%s (not scored)", metric)
        return ScoreResponse(
            contact_id=data.get("customer_id", "unknown"),
            engagement_score=0, ai_score=None, combined_score=0.0,
            lead_tier="ignored", interest_category=None,
            hubspot_updated=False, dialer_added=False,
        )

    # Extract lead info from CIO's identifiers
    identifiers = data.get("identifiers", {}) or {}
    # Prefer email so hubspot.py resolves it → numeric HubSpot ID.
    # CIO's customer_id is a hex string (not a HubSpot ID) — using it directly causes 404s.
    contact_id = (
        identifiers.get("email")
        or data.get("recipient")
        or data.get("customer_id")
        or identifiers.get("id")
        or identifiers.get("cio_id", "")
    )
    if not contact_id:
        raise HTTPException(status_code=422, detail="No customer_id or identifiers.id in CIO payload")

    # CIO sends campaign_name in reporting webhook — use it to derive funnel source
    campaign_name = data.get("campaign_name", "") or ""
    click_url     = data.get("href", "") or data.get("link_url", "")
    funnel_source = _detect_funnel_source(campaign_name, click_url) if campaign_name else ""

    lead = LeadContext(
        contact_id=str(contact_id),
        email=identifiers.get("email", "") or data.get("recipient", ""),
        funnel_source=funnel_source,
    )

    # Build mapped event — convert CIO Unix epoch → ISO 8601
    raw_ts = body.get("timestamp", "")
    if raw_ts:
        try:
            ts_iso = datetime.fromtimestamp(int(raw_ts), tz=timezone.utc).isoformat()
        except (ValueError, TypeError, OSError):
            ts_iso = str(raw_ts)  # fallback: pass through as-is
    else:
        ts_iso = datetime.now(timezone.utc).isoformat()

    url = data.get("href", "") or data.get("link_url", "")
    mapped_events = [{
        "event_type": event_type,
        "timestamp": ts_iso,
        "url": url,
        "metadata": data,
    }]

    # For click events, refine by URL pattern
    if metric == "clicked" and url:
        url_lower = url.lower()
        if any(p in url_lower for p in CHECKOUT_URL_PATTERNS):
            mapped_events[0]["event_type"] = "checkout_visited"
        elif any(p in url_lower for p in PRICE_INFO_PATTERNS):
            mapped_events[0]["event_type"] = "price_info_viewed"
        elif any(p in url_lower for p in SALES_PAGE_PATTERNS):
            mapped_events[0]["event_type"] = "cta_clicked"

    logger.info("CIO webhook: metric=%s → event_type=%s for %s",
                metric, mapped_events[0]["event_type"], contact_id)

    return await _score_and_update(mapped_events, lead, pre_mapped=True)


async def _handle_custom_batch(body: dict[str, Any]) -> ScoreResponse:
    """Handle the custom batch format (manual triggers, /score endpoint)."""
    raw_events: list[dict] = body.get("events", [body])
    lead_data: dict = body.get("lead", {})

    if not lead_data.get("contact_id"):
        raise HTTPException(status_code=422, detail="lead.contact_id is required")

    return await _score_and_update(raw_events, LeadContext(**lead_data))


@app.post("/score", response_model=ScoreResponse)
async def score_lead(
    events: list[dict[str, Any]],
    lead: LeadContext,
):
    """
    Direct scoring endpoint (useful for manual triggers or testing).
    Accepts pre-mapped events (with event_type already set).
    """
    return await _score_and_update(events, lead, pre_mapped=True)


@app.post("/webhook/hubspot/call")
async def hubspot_call_webhook(payload: HubSpotCallPayload):
    """
    Receive a HubSpot Call activity webhook.
    Setup: HubSpot Workflow → "notes_last_contacted changed" → Custom Webhook → this endpoint.

    When contact_id is provided, fetches live call details from HubSpot via the
    associations API (the workflow can only pass contact properties, not call properties).
    Also writes lead_last_call_date + lead_last_call_outcome back to the contact.
    """
    import asyncio

    contact_name = f"{payload.contact_firstname} {payload.contact_lastname}".strip() or "Unknown"

    # Resolve call properties: prefer live HubSpot data over payload fields
    direction    = payload.hs_call_direction
    disposition  = payload.hs_call_disposition
    duration_ms  = payload.hs_call_duration
    ts_raw_ms    = payload.hs_timestamp

    if payload.contact_id:
        live_call = await get_latest_call_for_contact(payload.contact_id)
        if live_call:
            direction   = live_call.get("hs_call_direction",   direction)
            disposition = live_call.get("hs_call_disposition", disposition)
            try:
                duration_ms = int(live_call.get("hs_call_duration", duration_ms) or duration_ms)
            except (ValueError, TypeError):
                pass
            # Prefer hs_timestamp (call start) over hs_createdate (record created)
            ts_raw_ms = live_call.get("hs_timestamp") or live_call.get("hs_createdate") or ts_raw_ms

    outcome      = HS_DISPOSITION_MAP.get(disposition, disposition or "Unknown")
    duration_sec = (duration_ms or 0) // 1000

    # Parse timestamp
    try:
        if isinstance(ts_raw_ms, str):
            # ISO string from live_call (hs_createdate)
            ts = datetime.fromisoformat(ts_raw_ms.replace("Z", "+00:00"))
        elif ts_raw_ms:
            ts = datetime.fromtimestamp(int(ts_raw_ms) / 1000, tz=timezone.utc)
        else:
            ts = datetime.now(tz=timezone.utc)
        ts_str = ts.strftime("%Y-%m-%d %H:%M UTC")
    except (ValueError, TypeError, OSError):
        ts_str = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    # Write outcome + call date back to HubSpot AND fetch stats — in parallel
    stats_task   = asyncio.create_task(get_call_stats())
    outcome_task = asyncio.create_task(
        write_call_outcome(payload.contact_id, outcome)
    ) if payload.contact_id else None

    (calls_7d, calls_30d, calls_365d) = await stats_task
    if outcome_task:
        await outcome_task

    await send_call_report(
        contact_name=contact_name,
        direction=direction,
        outcome=outcome,
        duration_sec=duration_sec,
        timestamp=ts_str,
        calls_7d=calls_7d,
        calls_30d=calls_30d,
        calls_365d=calls_365d,
    )

    logger.info(
        "Call webhook processed: contact=%s direction=%s outcome=%s duration=%ds",
        contact_name, direction, outcome, duration_sec,
    )
    return {"status": "ok", "contact": contact_name, "outcome": outcome, "duration_sec": duration_sec}


@app.get("/batch/prioritize")
async def batch_prioritize(limit: int = 200):
    """
    Return the outbound calling queue sorted by priority:
      1. Hot  (lead_tier=1_hot)  — not called in last 24h
      2. Warm (lead_tier=2_warm) — not called in last 48h
      3. Cold (lead_tier=3_cold) — not called in last 7d
    Only contacts with a phone number. Never-called contacts are always included.
    """
    contacts = await get_prioritized_contacts(limit=limit)

    queue = []
    for rank, c in enumerate(contacts, start=1):
        props = c.get("properties", {}) or {}
        queue.append({
            "rank":          rank,
            "contact_id":    c.get("id", ""),
            "name":          f"{props.get('firstname', '')} {props.get('lastname', '')}".strip() or "Unknown",
            "phone":         props.get("phone", ""),
            "email":         props.get("email", ""),
            "tier":          props.get("lead_tier", ""),
            "score":         props.get("lead_combined_score"),
            "interest":      props.get("lead_interest_category"),
            "last_called":   props.get("lead_last_call_date"),
            "last_outcome":  props.get("lead_last_call_outcome"),
        })

    return {"total": len(queue), "contacts": queue}


@app.post("/batch/run")
async def batch_run():
    """
    Manually trigger a batch re-score of all HubSpot contacts.
    Fire-and-forget: returns immediately while scoring runs in the background.
    """
    import asyncio
    asyncio.create_task(run_batch_scoring())
    logger.info("/batch/run triggered manually")
    return {"status": "started", "message": "Batch scoring started in background"}


# ---------------------------------------------------------------------------
# Core scoring pipeline
# ---------------------------------------------------------------------------
async def _score_and_update(
    raw_events: list[dict[str, Any]],
    lead: LeadContext,
    pre_mapped: bool = False,
) -> ScoreResponse:

    # 1. Map Customer.io events → internal event types
    if pre_mapped:
        mapped_events = raw_events
    else:
        mapped_events = []
        for raw in raw_events:
            event_type = _map_cio_event(raw)
            if event_type:
                data = raw.get("data", {}) or {}
                mapped_events.append({
                    "event_type": event_type,
                    "timestamp":  raw.get("timestamp", ""),
                    "url":        (data.get("page", {}) or {}).get("url", ""),
                    "metadata":   data,
                })

    # 2. Calculate engagement score
    engagement_result = calculate_engagement_score(mapped_events)

    # 3. Detect interest category
    interest_result = detect_interest_category(mapped_events)

    # 4. Build AI feature vector (for when model is ready)
    ai_features = _build_ai_features(mapped_events, engagement_result)

    # 5. Combine → final score + tier
    result = combine_scores(engagement_result, interest_result, ai_features)

    logger.info(
        "Scored %s → engagement=%d combined=%.1f tier=%s category=%s",
        lead.contact_id, result.engagement_score, result.combined_score,
        result.lead_tier, result.interest_category,
    )

    # 6. Write back to HubSpot
    # Include lead_funnel_source if the CIO event carried campaign info.
    # HubSpot will overwrite an existing value — we only send it when non-empty
    # so contacts without campaign context keep whatever they already have.
    hubspot_ok = False
    try:
        hs_payload = result.to_hubspot_payload()
        if lead.funnel_source:
            hs_payload["lead_funnel_source"] = lead.funnel_source
        await upsert_contact_score(lead.contact_id, hs_payload)
        hubspot_ok = True
    except Exception as e:
        logger.error("HubSpot update failed: %s", e)

    # 7. Aircall Power Dialer (Hot + Warm) + Slack alerts
    dialer_ok = False
    if result.combined_score >= 50 and lead.phone:
        try:
            notes = (
                f"Score: {result.combined_score:.0f} | "
                f"Tier: {result.tier_label} | "
                f"Interesse: {result.interest_category or 'unknown'}"
            )
            dialer_result = await add_to_power_dialer(
                {
                    "phone":     lead.phone,
                    "firstname": lead.firstname,
                    "lastname":  lead.lastname,
                    "email":     lead.email,
                    "notes":     notes,
                },
                score=result.combined_score,
                created_at=lead.created_at,
                interest_category=result.interest_category,
            )
            dialer_ok = dialer_result is not None
        except Exception as e:
            logger.error("Aircall power dialer failed: %s", e)

    # Slack alert only for Hot leads
    if result.is_hot:
        try:
            await send_hot_lead_alert(
                lead.model_dump(),
                result.combined_score,
                result.lead_tier,
                result.interest_category,
            )
        except Exception as e:
            logger.error("Slack alert failed: %s", e)

    return ScoreResponse(
        contact_id=lead.contact_id,
        engagement_score=result.engagement_score,
        ai_score=result.ai_score,
        combined_score=result.combined_score,
        lead_tier=result.lead_tier,
        interest_category=result.interest_category,
        hubspot_updated=hubspot_ok,
        dialer_added=dialer_ok,
    )


def _detect_funnel_source(campaign_name: str, url: str = "") -> str:
    """
    Derive a clean funnel source label from a CIO campaign name or URL.

    Priority order:
      1. Product keyword in campaign name → e.g. "hypnose_email_funnel"
      2. Product keyword in URL           → e.g. "hypnose_landing"
      3. Raw campaign name (trimmed)      → e.g. "Launch Sequence Woche 3"
      4. Empty string if no signal

    Used to write lead_funnel_source to HubSpot so sales + marketing
    can segment by which campaign/landing page produced each lead.
    """
    PRODUCT_KEYWORDS = {
        "hypnose":   ("hypnos",),
        "lifecoach": ("lifecoach", "life coach", "life-coach"),
        "meditation": ("meditati",),
    }
    name_lower = campaign_name.lower()
    url_lower  = url.lower()

    for product, keywords in PRODUCT_KEYWORDS.items():
        if any(k in name_lower for k in keywords):
            return f"{product}_email_funnel"

    for product, keywords in PRODUCT_KEYWORDS.items():
        if any(k in url_lower for k in keywords):
            return f"{product}_landing"

    return campaign_name.strip()


def _build_ai_features(
    events: list[dict[str, Any]],
    engagement_result: dict[str, Any],
) -> dict[str, float]:
    counts: dict[str, int] = {}
    for e in events:
        t = e.get("event_type", "")
        counts[t] = counts.get(t, 0) + 1

    return {
        "engagement_score":   engagement_result["score"],
        "email_opens":        counts.get("email_opened", 0),
        "email_clicks":       counts.get("email_link_clicked", 0),
        "video_views":        counts.get("video_watched_50", 0) + counts.get("video_watched_75", 0),
        "sales_page_visits":  counts.get("sales_page_visited", 0),
        "checkout_visits":    counts.get("checkout_visited", 0),
        "webinar_attended":   counts.get("webinar_attended", 0),
    }


def _verify_signature(raw_body: bytes, signature: str) -> None:
    """HMAC-SHA256 verification for Customer.io webhook secret."""
    import hashlib
    import hmac

    expected = hmac.new(
        WEBHOOK_SECRET.encode(), raw_body, hashlib.sha256
    ).hexdigest()
    if not hmac.compare_digest(expected, signature):
        raise HTTPException(status_code=401, detail="Invalid webhook signature")
