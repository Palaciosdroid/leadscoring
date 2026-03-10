"""
SBC Lead Scoring Engine — FastAPI
Receives Customer.io webhooks, scores leads, writes back to HubSpot,
triggers Aircall Power Dialer + Slack alerts for Hot/Warm Leads.

Deploy: Railway (~$5-10/Mo)
"""

import logging
import os
from contextlib import asynccontextmanager
from typing import Any

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI, Header, HTTPException, Request
from pydantic import BaseModel, Field

from batch.scorer import run_batch_scoring
from integrations.hubspot import upsert_contact_score
from integrations.aircall import add_to_power_dialer
from integrations.slack import send_hot_lead_alert
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


class ScoreResponse(BaseModel):
    contact_id: str
    engagement_score: int
    ai_score: float | None
    combined_score: float
    lead_tier: str
    interest_category: str | None
    hubspot_updated: bool
    dialer_added: bool


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------
@app.get("/health")
async def health():
    return {"status": "ok", "version": "1.0.0"}


@app.post("/webhook/customerio", response_model=ScoreResponse)
async def customerio_webhook(
    request: Request,
    x_cio_signature: str | None = Header(default=None),
):
    """
    Receive a Customer.io event batch webhook.
    Expects JSON body: { "events": [...], "lead": { LeadContext } }
    or a single event payload with lead context embedded.
    """
    # Read raw bytes first so signature verification has the original payload
    raw_body = await request.body()

    # Optional: verify Customer.io webhook signature
    if WEBHOOK_SECRET and x_cio_signature:
        _verify_signature(raw_body, x_cio_signature)

    import json
    body = json.loads(raw_body)

    raw_events: list[dict] = body.get("events", [body])  # support single or batch
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
    hubspot_ok = False
    try:
        await upsert_contact_score(lead.contact_id, result.to_hubspot_payload())
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
