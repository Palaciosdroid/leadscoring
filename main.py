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
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI, Header, HTTPException, Query, Request
from pydantic import BaseModel, Field

from analytics.buyer_journey import analyze_buyer_journeys, run_weekly_buyer_journey
from batch.call_poller import run_call_polling
from batch.scorer import (
    run_batch_scoring,
    _determine_freshness,
    _determine_list_key,
    _determine_tier_label,
    _build_hubspot_card_properties,
    _build_aircall_card,
    _build_funnel_source,
    _extract_purchased_funnels,
    _extract_offer_signals,
    SCORE_WARM,
    VALID_FUNNELS,
)
from batch.scheduled_calls_summarizer import run_scheduled_calls_summarizer
from batch.unsubscribe_handler import handle_unsubscribe
from integrations.hubspot import (
    HS_DISPOSITION_MAP,
    upsert_contact_score,
    get_latest_call_for_contact,
    get_prioritized_contacts,
    write_call_outcome,
    find_contact_by_zoom_meeting,
    find_contact_by_phone,
    add_note,
    has_upcoming_hubspot_meeting,
    update_contact_properties,
)
from integrations.zoom import (
    get_vtt_url,
    download_recording,
    get_lead_email_from_meeting,
    verify_webhook_signature as verify_zoom_signature,
)
from batch.call_summarizer import process_zoom_vtt
from integrations.aircall import add_to_power_dialer
from integrations.supabase import (
    fetch_touchpoints_for_emails,
    fetch_all_lead_data,
    store_whatsapp_event,
)
# Slack hot lead alerts removed — Kevin handles via Aircall
from scoring.combined import combine_scores, map_whatsapp_to_engagement
from scoring.engagement import calculate_engagement_score
from scoring.hook_engine import generate_hook
from scoring.interest import detect_interest_category
from scoring.touchpoint_mapper import (
    extract_first_last_touch,
    map_touchpoints_batch,
    map_browser_events_batch,
    summarize_email_activity,
)

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
    "subscribed":    "webinar_registered",     # CIO attribute change → webinar signup
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


CALL_POLL_INTERVAL_MINUTES = int(os.environ.get("CALL_POLL_INTERVAL_MINUTES", "5"))
CALL_POLL_WINDOW_MINUTES   = int(os.environ.get("CALL_POLL_WINDOW_MINUTES",    "10"))


@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler.add_job(
        run_batch_scoring,
        "interval",
        minutes=BATCH_INTERVAL_MINUTES,
        id="batch_scoring",
        replace_existing=True,
    )
    # Poll HubSpot for completed calls — replaces Workflow → Webhook (Operations Hub Pro)
    scheduler.add_job(
        run_call_polling,
        "interval",
        minutes=CALL_POLL_INTERVAL_MINUTES,
        kwargs={"since_minutes": CALL_POLL_WINDOW_MINUTES},
        id="call_polling",
        replace_existing=True,
    )
    # Daily decay sweep — 17:00 CET, one hour before EOD summary.
    # run_batch_scoring() already contains decay detection + Slack alerts.
    # Running it at a fixed time means decay alerts land predictably instead of
    # randomly during the 30-min interval job.
    scheduler.add_job(
        run_batch_scoring,
        "cron",
        hour=17,
        minute=0,
        timezone=ZoneInfo("Europe/Berlin"),
        id="daily_decay_check",
        replace_existing=True,
    )
    # Scheduled calls summarizer — 18:00 CET daily
    # Fetches past 7 days completed calls + next 7 days booked calls
    # Posts combined summary to Slack with both sections
    scheduler.add_job(
        run_scheduled_calls_summarizer,
        "cron",
        hour=18,
        minute=0,
        timezone=ZoneInfo("Europe/Berlin"),
        id="daily_summary",
        replace_existing=True,
    )
    # Buyer journey analysis — Mon/Wed/Fri 10:00 CET
    # Analyzes common touchpoints across all buyers, posts findings to Slack
    scheduler.add_job(
        run_weekly_buyer_journey,
        "cron",
        day_of_week="mon,wed,fri",
        hour=10,
        minute=0,
        timezone=ZoneInfo("Europe/Berlin"),
        id="weekly_buyer_journey",
        replace_existing=True,
    )
    scheduler.start()
    logger.info(
        "Schedulers started — batch scoring every %dm, call polling every %dm (window=%dm), "
        "decay check at 17:00 CET, daily summary at 18:00 CET, buyer journey Mon/Wed/Fri 10:00 CET",
        BATCH_INTERVAL_MINUTES, CALL_POLL_INTERVAL_MINUTES, CALL_POLL_WINDOW_MINUTES,
    )
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
DEBUG_API_KEY = os.environ.get("DEBUG_API_KEY", "")

# Startup validation — warn about missing critical secrets
_REQUIRED_ENV_VARS = [
    "HUBSPOT_ACCESS_TOKEN",
    "SUPABASE_URL",
    "SUPABASE_SERVICE_KEY",
    "AIRCALL_API_ID",
    "AIRCALL_API_TOKEN",
    "DEBUG_API_KEY",
    "CIO_WEBHOOK_SECRET",   # Required — skip-when-empty silently opens CIO endpoint to anyone
]
_missing = [v for v in _REQUIRED_ENV_VARS if not os.environ.get(v)]
if _missing:
    import logging as _startup_log
    _startup_log.getLogger(__name__).warning(
        "⚠️  Missing required environment variables: %s  — some features will fail.", _missing
    )


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


class RealtimeScoreRequest(BaseModel):
    """
    Payload for the realtime scoring endpoint.

    Triggered by HubSpot Workflow when a new contact is created or phone number added.
    The endpoint fetches all Supabase data, scores, and pushes to HubSpot + Aircall
    in one shot — fresh leads get scored in <5 min instead of waiting for the 30-min batch.
    """
    email: str = Field(..., description="Contact email (used for Supabase lookup)")
    contact_id: str = Field(default="", description="HubSpot contact ID — resolved from email if empty")
    firstname: str = ""
    lastname: str = ""
    phone: str = ""


class RealtimeScoreResponse(BaseModel):
    contact_id: str
    email: str
    engagement_score: int
    combined_score: float
    lead_tier: str
    tier_label: str
    interest_category: str | None
    hubspot_updated: bool
    dialer_added: bool
    is_fresh: bool
    list_key: str | None
    hook: str


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


class WhatsAppEventPayload(BaseModel):
    """
    Payload from the MC-Webinar-Setter WhatsApp bot.

    Sent when a lead completes a WhatsApp qualification conversation.
    The bot scores the lead and sends structured data here for
    integration into the lead scoring pipeline.
    """
    phone: str = Field(..., description="Lead phone number (E.164 format)")
    email: str = Field(default="", description="Lead email if collected")
    name: str = Field(default="", description="Lead name if collected")
    whatsapp_score: int = Field(default=0, description="Bot's qualification score (0-100)")
    interest_level: str = Field(default="", description="interested | not_interested | undecided")
    interest_type: str = Field(default="", description="ausbildung | coaching | meditation | hypnose")
    wants_to_coach: bool = False
    personal_growth: bool = False
    pain_points: list[str] = Field(default_factory=list)
    next_action: str = Field(default="", description="send_calendar | nurture | disqualify")
    summary: str = Field(default="", description="Bot's summary of the conversation")
    message_count: int = 0
    has_calendar_link: bool = False
    opted_out: bool = False
    funnel: str = Field(default="mc", description="Which funnel/bot sent this")
    timestamp: str = Field(default="", description="ISO 8601 timestamp")


class WhatsAppEventResponse(BaseModel):
    phone: str
    email: str
    whatsapp_score_mapped: float
    engagement_score: int
    combined_score: float
    lead_tier: str
    tier_label: str
    interest_category: str | None
    hubspot_updated: bool
    dialer_added: bool
    event_stored: bool


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

    # Extract lead info from CIO's identifiers FIRST (needed for unsubscribe handling)
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

    # Special handling for unsubscribe events
    if metric == "unsubscribed":
        email = identifiers.get("email", "") or data.get("recipient", "")
        phone = identifiers.get("phone", "")

        logger.info("CIO webhook: UNSUBSCRIBE detected for %s — removing from lists and Power Dialer", email)

        # Trigger async unsubscribe handling (list removal, Power Dialer removal)
        unsub_results = await handle_unsubscribe(
            contact_id=str(contact_id),
            email=email,
            phone=phone,
        )
        logger.info("CIO webhook: unsubscribe handling complete: %s", unsub_results)

        # Return response indicating the unsubscribe was processed
        return ScoreResponse(
            contact_id=str(contact_id),
            engagement_score=0, ai_score=None, combined_score=0.0,
            lead_tier="unsubscribed", interest_category=None,
            hubspot_updated=False, dialer_added=False,
        )

    # Map CIO metric → internal event type for non-unsubscribe events
    event_type = CIO_METRIC_MAP.get(metric)
    if not event_type:
        logger.info("Ignoring CIO metric=%s (not scored)", metric)
        return ScoreResponse(
            contact_id=data.get("customer_id", "unknown"),
            engagement_score=0, ai_score=None, combined_score=0.0,
            lead_tier="ignored", interest_category=None,
            hubspot_updated=False, dialer_added=False,
        )

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

    # Persist email events to Supabase so the batch scorer sees them
    # when building the Aircall card (summarize_email_activity).
    if event_type in ("email_opened", "email_link_clicked"):
        from integrations.supabase import store_cio_email_event
        try:
            await store_cio_email_event(
                email=lead.email,
                event_type=event_type,
                timestamp=ts_iso,
                campaign_name=campaign_name,
                url=url,
            )
        except Exception as e:
            logger.warning("CIO webhook: failed to persist %s to Supabase: %s", event_type, e)

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
    x_api_key: str | None = Header(default=None),
):
    """
    Direct scoring endpoint (useful for manual triggers or testing).
    Accepts pre-mapped events (with event_type already set).
    Requires DEBUG_API_KEY — writes HubSpot properties and can push to Aircall.
    """
    if not DEBUG_API_KEY or x_api_key != DEBUG_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing X-Api-Key header")
    return await _score_and_update(events, lead, pre_mapped=True)


@app.post("/webhook/hubspot/new-contact", response_model=RealtimeScoreResponse)
async def realtime_score_webhook(
    payload: RealtimeScoreRequest,
    x_api_key: str | None = Header(default=None),
    api_key: str | None = Query(default=None),
):
    """
    Realtime lead scoring — scores a single lead on-demand in <5 seconds.

    Triggered by HubSpot Workflow when:
      - New contact created (lifecycle stage = subscriber)
      - Phone number added/updated on existing contact

    Flow:
      1. Fetch touchpoints + events + purchases + meetings from Supabase
      2. Score using same pipeline as batch scorer
      3. Push score + card properties to HubSpot
      4. Push to Aircall Power Dialer if qualified (Fresh or Warm+)

    This replaces the 30-min batch wait for fresh leads — they now get
    scored and pushed to the closer's dialer within minutes of opt-in.
    Accepts X-Api-Key header or ?api_key= query param (HubSpot workflow limitation:
    custom headers cannot be set via API, only query params work programmatically).
    """
    provided_key = x_api_key or api_key
    if not DEBUG_API_KEY or provided_key != DEBUG_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing X-Api-Key header")
    email = payload.email.strip().lower()
    if not email:
        raise HTTPException(status_code=422, detail="email is required")

    contact_id = payload.contact_id
    logger.info("Realtime score: starting for %s (contact_id=%s)", email, contact_id)

    # Step 1: Fetch Supabase data (touchpoints + events + purchases + meetings)
    try:
        touchpoints_by_email = await fetch_touchpoints_for_emails([email], days=30)
        all_lead_data = await fetch_all_lead_data([email], days=30)
    except Exception as e:
        logger.error("Realtime score: Supabase fetch failed for %s: %s", email, e)
        raise HTTPException(status_code=502, detail=f"Supabase fetch failed: {e}")

    touchpoints = touchpoints_by_email.get(email, [])
    lead_data = all_lead_data.get(email, {
        "events": [], "purchases": [], "meetings": [], "customerio_id": None,
    })
    browser_events = lead_data["events"]
    purchases = lead_data["purchases"]
    meetings = lead_data["meetings"]

    # Step 2: Score — same pipeline as batch scorer
    scored_events = map_touchpoints_batch(touchpoints)
    browser_scored = map_browser_events_batch(browser_events)
    scored_events.extend(browser_scored)

    engagement_result = calculate_engagement_score(scored_events)
    interest_result = detect_interest_category(scored_events)

    # Build AI features
    ai_features = _build_ai_features(scored_events, engagement_result)

    scoring = combine_scores(engagement_result, interest_result, ai_features)
    score = scoring.combined_score
    funnel = scoring.interest_category

    # Freshness + tier
    is_fresh, fresh_hours = _determine_freshness(touchpoints)
    tier_label = _determine_tier_label(score, is_fresh)

    # Touchpoint analysis
    first_touch, last_touch = extract_first_last_touch(touchpoints)
    email_summary = summarize_email_activity(touchpoints, days=14)
    funnel_source = _build_funnel_source(touchpoints)
    purchased_funnels = _extract_purchased_funnels(purchases)

    # Multi-funnel info
    cat_scores = interest_result.get("category_scores", {})
    multi_funnels = [f for f in VALID_FUNNELS if cat_scores.get(f, 0) > 0]
    multi_funnel_info = ", ".join(multi_funnels) if len(multi_funnels) > 1 else ""

    # Determine list assignment
    has_phone = bool(payload.phone)
    should_push = has_phone and (is_fresh or score >= SCORE_WARM)

    # Simplified eignungscheck check (no DNC filter in realtime — new leads are clean)
    qualifies_eignungscheck = (
        has_phone
        and funnel is not None
        and funnel not in purchased_funnels
    )

    list_key = _determine_list_key(funnel, is_fresh, score, qualifies_eignungscheck)

    # Generate hook for Aircall card
    offer_signals = _extract_offer_signals(browser_events)
    hook_context = {
        "email_clicked": email_summary.get("clicks", 0) > 0,
        "last_email_subject": email_summary.get("last_email_subject", ""),
        "is_fresh": is_fresh,
        "fresh_hours": fresh_hours,
        "funnel": funnel,
        "score": score,
        "eignungscheck": qualifies_eignungscheck,
        "call_booked": False,
        "purchased_products": purchased_funnels,
        "visited_offer_page": offer_signals.get("visited_offer"),
        "visited_checkout": offer_signals.get("visited_checkout"),
        "watched_video_on_offer": offer_signals.get("video_on_offer"),
        "viewed_pricing": offer_signals.get("viewed_pricing"),
    }
    hook = generate_hook(hook_context)

    logger.info(
        "Realtime score: %s → score=%.0f tier=%s fresh=%s list=%s",
        email, score, tier_label, is_fresh, list_key,
    )

    # Step 3: Push to HubSpot
    hubspot_ok = False
    try:
        hs_properties = _build_hubspot_card_properties(
            scoring=scoring,
            funnel=funnel,
            funnel_source=funnel_source,
            first_touch=first_touch,
            last_touch=last_touch,
            purchased_funnels=purchased_funnels,
            multi_funnel_info=multi_funnel_info,
        )
        # Use email as contact_id if none provided (hubspot.py resolves email → ID)
        hs_contact_id = contact_id or email
        await upsert_contact_score(hs_contact_id, hs_properties)
        hubspot_ok = True
    except Exception as e:
        logger.error("Realtime score: HubSpot update failed for %s: %s", email, e)

    # Step 4: Push to Aircall Power Dialer if qualified
    dialer_ok = False
    if should_push and list_key and payload.phone:
        try:
            aircall_card = _build_aircall_card(
                tier_label=tier_label,
                funnel=funnel,
                score=score,
                last_call_date=None,
                email_summary=email_summary,
                first_touch=first_touch,
                last_touch=last_touch,
                hook=hook,
                purchased_funnels=purchased_funnels,
            )
            dialer_result = await add_to_power_dialer(
                {
                    "phone": payload.phone,
                    "firstname": payload.firstname,
                    "lastname": payload.lastname,
                    "email": email,
                    "notes": aircall_card,
                },
                score=score,
                interest_category=funnel,
                lead_tier=scoring.lead_tier,
                list_key=list_key or "",
            )
            dialer_ok = dialer_result is not None
            logger.info("Realtime score: pushed %s to Aircall [%s]", email, list_key)
        except Exception as e:
            logger.error("Realtime score: Aircall push failed for %s: %s", email, e)

    return RealtimeScoreResponse(
        contact_id=contact_id or email,
        email=email,
        engagement_score=scoring.engagement_score,
        combined_score=score,
        lead_tier=scoring.lead_tier,
        tier_label=tier_label,
        interest_category=funnel,
        hubspot_updated=hubspot_ok,
        dialer_added=dialer_ok,
        is_fresh=is_fresh,
        list_key=list_key,
        hook=hook,
    )


# ---------------------------------------------------------------------------
# WhatsApp Bot Qualification Webhook
# ---------------------------------------------------------------------------
@app.post("/webhook/whatsapp-event", response_model=WhatsAppEventResponse)
async def whatsapp_event_webhook(
    payload: WhatsAppEventPayload,
    x_api_key: str | None = Header(default=None),
):
    """
    Receive WhatsApp qualification data from the MC-Webinar-Setter bot.

    When a lead chats with the WhatsApp bot, the bot scores them and sends
    structured data here. This endpoint:
      1. Stores the event in Supabase as 'whatsapp_qualified'
      2. Maps WhatsApp signals to engagement-compatible score
      3. Fetches existing touchpoints for this contact (if email known)
      4. Rescores the contact with the 3-factor formula
      5. Pushes updated score to HubSpot
      6. Pushes to Aircall Power Dialer if qualified
    Requires X-Api-Key header (configured in the MC-Webinar-Setter bot env).
    """
    if not DEBUG_API_KEY or x_api_key != DEBUG_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing X-Api-Key header")
    email = payload.email.strip().lower() if payload.email else ""
    phone = payload.phone.strip()
    timestamp = payload.timestamp or datetime.now(timezone.utc).isoformat()

    logger.info(
        "WhatsApp event: phone=%s email=%s wa_score=%d interest=%s funnel=%s",
        phone, email, payload.whatsapp_score, payload.interest_type, payload.funnel,
    )

    # Step 1: Store event in Supabase
    event_stored = False
    try:
        event_data = payload.model_dump()
        event_data["timestamp"] = timestamp
        await store_whatsapp_event(event_data)
        event_stored = True
    except Exception as e:
        logger.error("WhatsApp event: Supabase store failed: %s", e)

    # Step 2: Build WhatsApp signal data for scoring
    whatsapp_data = {
        "whatsapp_score": payload.whatsapp_score,
        "wants_to_coach": payload.wants_to_coach,
        "personal_growth": payload.personal_growth,
        "has_calendar_link": payload.has_calendar_link,
        "opted_out": payload.opted_out,
    }
    wa_mapped = map_whatsapp_to_engagement(whatsapp_data)

    # Step 3: Fetch existing touchpoints if email known (for full rescore)
    scored_events = []
    browser_events = []
    purchases = []
    touchpoints = []
    if email:
        try:
            touchpoints_by_email = await fetch_touchpoints_for_emails([email], days=30)
            all_lead_data = await fetch_all_lead_data([email], days=30)
            touchpoints = touchpoints_by_email.get(email, [])
            lead_data = all_lead_data.get(email, {
                "events": [], "purchases": [], "meetings": [], "customerio_id": None,
            })
            browser_events = lead_data["events"]
            purchases = lead_data["purchases"]

            # Map existing touchpoints to scored events
            from scoring.touchpoint_mapper import map_touchpoints_batch, map_browser_events_batch
            scored_events = map_touchpoints_batch(touchpoints)
            browser_scored = map_browser_events_batch(browser_events)
            scored_events.extend(browser_scored)
        except Exception as e:
            logger.error("WhatsApp event: Supabase fetch failed for %s: %s", email, e)

    # Step 4: Score with WhatsApp data included
    engagement_result = calculate_engagement_score(scored_events)
    interest_result = detect_interest_category(scored_events)

    # Override interest category with WhatsApp bot's detection if available
    if payload.interest_type and not interest_result.get("category"):
        interest_result["category"] = payload.interest_type

    ai_features = _build_ai_features(scored_events, engagement_result)
    scoring = combine_scores(
        engagement_result, interest_result, ai_features,
        whatsapp_data=whatsapp_data,
    )
    score = scoring.combined_score
    funnel = scoring.interest_category or payload.interest_type

    # Determine tier label
    tier_label = scoring.tier_label

    logger.info(
        "WhatsApp event: %s → engagement=%d wa_mapped=%.0f combined=%.1f tier=%s",
        email or phone, scoring.engagement_score, wa_mapped or 0, score, tier_label,
    )

    # Step 5: Push to HubSpot (if email known — needed for contact lookup)
    hubspot_ok = False
    if email:
        try:
            hs_payload = scoring.to_hubspot_payload()
            hs_payload["lead_funnel_source"] = f"whatsapp_{payload.funnel}"
            if payload.interest_type:
                hs_payload["lead_interest_category"] = payload.interest_type
            await upsert_contact_score(email, hs_payload)
            hubspot_ok = True
        except Exception as e:
            logger.error("WhatsApp event: HubSpot update failed for %s: %s", email, e)

    # Step 6: Push to Aircall Power Dialer if qualified
    dialer_ok = False
    if phone and score >= SCORE_WARM and not payload.opted_out:
        try:
            name_parts = payload.name.split(" ", 1) if payload.name else ["", ""]
            firstname = name_parts[0]
            lastname = name_parts[1] if len(name_parts) > 1 else ""

            notes = (
                f"Score: {score:.0f} | Tier: {tier_label} | "
                f"WA-Score: {payload.whatsapp_score} | "
                f"Interesse: {funnel or 'unknown'} | "
                f"{payload.summary}"
            )
            dialer_result = await add_to_power_dialer(
                {
                    "phone": phone,
                    "firstname": firstname,
                    "lastname": lastname,
                    "email": email,
                    "notes": notes,
                },
                score=score,
                interest_category=funnel,
                lead_tier=scoring.lead_tier,
            )
            dialer_ok = dialer_result is not None
            logger.info("WhatsApp event: pushed %s to Aircall", email or phone)
        except Exception as e:
            logger.error("WhatsApp event: Aircall push failed for %s: %s", email or phone, e)

    return WhatsAppEventResponse(
        phone=phone,
        email=email,
        whatsapp_score_mapped=wa_mapped or 0,
        engagement_score=scoring.engagement_score,
        combined_score=score,
        lead_tier=scoring.lead_tier,
        tier_label=tier_label,
        interest_category=funnel,
        hubspot_updated=hubspot_ok,
        dialer_added=dialer_ok,
        event_stored=event_stored,
    )


# ---------------------------------------------------------------------------
# WhatsApp Booking Confirmed Webhook
# ---------------------------------------------------------------------------
@app.post("/webhook/booking-confirmed")
async def booking_confirmed_webhook(
    request: Request,
    x_api_key: str | None = Header(default=None),
):
    """
    Called by MC-Webinar-Setter when a lead books a meeting via the WhatsApp bot.

    Sets lead_call_booked=true and forces tier to 1_hot in HubSpot so the
    lead doesn't get re-assigned to a lower tier by the next batch run.
    Looks up contact by phone number (payload: phone, hubspotMeetingId, source).
    """
    if not DEBUG_API_KEY or x_api_key != DEBUG_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing X-Api-Key header")

    body = await request.json()
    phone = (body.get("phone") or "").strip()
    meeting_id = body.get("hubspotMeetingId", "")
    source = body.get("source", "mc-setter")

    if not phone:
        raise HTTPException(status_code=422, detail="phone is required")

    logger.info("Booking confirmed: phone=%s meeting_id=%s source=%s", phone, meeting_id, source)

    contact_id = await find_contact_by_phone(phone)
    if not contact_id:
        logger.warning("Booking confirmed: no HubSpot contact found for phone=%s", phone)
        return {"status": "ok", "found": False, "phone": phone}

    ok = await update_contact_properties(contact_id, {
        "lead_call_booked": "true",
        "lead_tier": "1_hot",
        "lead_funnel_source": f"whatsapp_{source}",
    })
    logger.info("Booking confirmed: HubSpot updated contact %s → call_booked=true, tier=1_hot (ok=%s)", contact_id, ok)
    return {"status": "ok", "found": True, "contact_id": contact_id, "hubspot_updated": ok}


@app.post("/webhook/hubspot/call")
async def hubspot_call_webhook(
    payload: HubSpotCallPayload,
    x_api_key: str | None = Header(default=None),
    api_key: str | None = Query(default=None),
):
    """
    Receive a HubSpot Call activity webhook.
    Setup: HubSpot Workflow → "notes_last_contacted changed" → Custom Webhook → this endpoint.

    When contact_id is provided, fetches live call details from HubSpot via the
    associations API (the workflow can only pass contact properties, not call properties).
    Also writes lead_last_call_date + lead_last_call_outcome back to the contact.
    Accepts X-Api-Key header or ?api_key= query param (HubSpot workflow limitation:
    custom headers cannot be set via API, only query params work programmatically).
    """
    provided_key = x_api_key or api_key
    if not DEBUG_API_KEY or provided_key != DEBUG_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing X-Api-Key header")
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

    # Write outcome back to HubSpot contact
    if payload.contact_id:
        try:
            await write_call_outcome(payload.contact_id, outcome)
        except Exception as e:
            logger.error("HubSpot write_call_outcome failed: %s", e)

    # No individual Slack card per call — meetings are summarised in the EOD report (18:00 CET)

    logger.info(
        "Call webhook processed: contact=%s direction=%s outcome=%s duration=%ds",
        contact_name, direction, outcome, duration_sec,
    )
    return {"status": "ok", "contact": contact_name, "outcome": outcome, "duration_sec": duration_sec}


@app.get("/batch/prioritize")
async def batch_prioritize(
    limit: int = 200,
    x_api_key: str | None = Header(default=None),
):
    """
    Return the outbound calling queue sorted by priority:
      1. Hot  (lead_tier=1_hot)  — not called in last 24h
      2. Warm (lead_tier=2_warm) — not called in last 48h
      3. Cold (lead_tier=3_cold) — not called in last 7d
    Only contacts with a phone number. Never-called contacts are always included.
    Requires DEBUG_API_KEY — returns PII (name, phone, email) for all queued leads.
    """
    if not DEBUG_API_KEY or x_api_key != DEBUG_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing X-Api-Key header")
    limit = min(limit, 200)  # Hard cap — prevent unbounded PII dump
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
async def batch_run(x_api_key: str | None = Header(default=None)):
    """
    Manually trigger a batch re-score of all HubSpot contacts.
    Fire-and-forget: returns immediately while scoring runs in the background.
    Requires DEBUG_API_KEY.
    """
    if not DEBUG_API_KEY or x_api_key != DEBUG_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing X-Api-Key header")
    import asyncio
    asyncio.create_task(run_batch_scoring())
    logger.info("/batch/run triggered manually")
    return {"status": "started", "message": "Batch scoring started in background"}


@app.post("/debug/batch")
async def debug_batch(x_api_key: str | None = Header(default=None)):
    """Run batch scoring synchronously — returns result or error. Requires DEBUG_API_KEY."""
    if not DEBUG_API_KEY or x_api_key != DEBUG_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing X-Api-Key header")
    import io, logging as _log
    buf = io.StringIO()
    handler = _log.StreamHandler(buf)
    handler.setLevel(_log.DEBUG)
    handler.setFormatter(_log.Formatter("%(name)s %(levelname)s %(message)s"))
    for name in ("batch.scorer", "integrations.supabase", "root", ""):
        lg = _log.getLogger(name)
        lg.addHandler(handler)
        lg.setLevel(_log.DEBUG)
    try:
        await run_batch_scoring()
        return {"status": "completed", "logs": buf.getvalue()[-5000:]}
    except Exception as exc:
        import traceback
        return {"status": "error", "error": str(exc), "traceback": traceback.format_exc(), "logs": buf.getvalue()[-5000:]}
    finally:
        for name in ("batch.scorer", "integrations.supabase", "root", ""):
            _log.getLogger(name).removeHandler(handler)


@app.post("/debug/poll")
async def debug_poll(window_minutes: int = 10, x_api_key: str | None = Header(default=None)):
    """Manually trigger the call poller for E2E testing. Requires DEBUG_API_KEY."""
    if not DEBUG_API_KEY or x_api_key != DEBUG_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing X-Api-Key header")
    logger.info("/debug/poll triggered manually (window=%dm)", window_minutes)
    await run_call_polling(since_minutes=window_minutes)
    return {"status": "ok", "message": f"Call polling completed (window={window_minutes}m)"}


# ---------------------------------------------------------------------------
# Analytics endpoints
# ---------------------------------------------------------------------------
@app.get("/analytics/buyer-journey")
async def get_buyer_journey(x_api_key: str | None = Header(default=None)):
    """
    Run buyer journey analysis on demand.

    Fetches all buyers from HubSpot (closed-won), Supabase (purchases),
    and Customer.io (purchase events), then analyzes common touchpoints,
    funnel sequences, and scoring correlations.

    Requires DEBUG_API_KEY.
    """
    if not DEBUG_API_KEY or x_api_key != DEBUG_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing X-Api-Key header")
    logger.info("/analytics/buyer-journey triggered")
    analysis = await analyze_buyer_journeys()
    return analysis


@app.post("/analytics/buyer-journey/slack")
async def post_buyer_journey_slack(x_api_key: str | None = Header(default=None)):
    """
    Run buyer journey analysis and post results to Slack.
    Same as the weekly scheduled job, but triggered manually.
    Requires DEBUG_API_KEY.
    """
    if not DEBUG_API_KEY or x_api_key != DEBUG_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing X-Api-Key header")
    logger.info("/analytics/buyer-journey/slack triggered")
    await run_weekly_buyer_journey()
    return {"status": "ok", "message": "Buyer journey analysis posted to Slack"}


@app.post("/debug/daily-summary")
async def debug_daily_summary(x_api_key: str | None = Header(default=None)):
    """Manually trigger the EOD daily summary card. Requires DEBUG_API_KEY."""
    if not DEBUG_API_KEY or x_api_key != DEBUG_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing X-Api-Key header")
    logger.info("/debug/daily-summary triggered manually")
    await run_scheduled_calls_summarizer()
    return {"status": "ok", "message": "Daily summary sent to Slack"}


@app.post("/debug/realtime-score")
async def debug_realtime_score(
    email: str,
    phone: str = "",
    firstname: str = "",
    lastname: str = "",
    x_api_key: str | None = Header(default=None),
):
    """
    Test realtime scoring for a specific email without HubSpot Workflow trigger.
    Requires DEBUG_API_KEY. Calls the same pipeline as /webhook/hubspot/new-contact.
    """
    if not DEBUG_API_KEY or x_api_key != DEBUG_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing X-Api-Key header")
    logger.info("/debug/realtime-score triggered for %s", email)
    return await realtime_score_webhook(RealtimeScoreRequest(
        email=email, phone=phone, firstname=firstname, lastname=lastname,
    ))


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

    # 7. Aircall Power Dialer — _should_dial decides: Fresh (< 24h) or Warm (Hot+Warm tier)
    dialer_ok = False
    if lead.phone:
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
                lead_tier=result.lead_tier,
            )
            dialer_ok = dialer_result is not None
        except Exception as e:
            logger.error("Aircall power dialer failed: %s", e)

    # No Slack alerts for hot leads — Kevin handles everything via Aircall

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
    """HMAC-SHA256 verification for Customer.io webhook secret.

    CIO may send either "sha256=<hex>" or raw "<hex>" — strip prefix before comparing.
    """
    import hashlib
    import hmac

    expected = hmac.new(
        WEBHOOK_SECRET.encode(), raw_body, hashlib.sha256
    ).hexdigest()
    # Strip "sha256=" prefix if present (CIO format varies by account/version)
    canonical = signature.removeprefix("sha256=")
    if not hmac.compare_digest(expected, canonical):
        raise HTTPException(status_code=401, detail="Invalid webhook signature")


# ---------------------------------------------------------------------------
# Phase 5: Zoom Call AI — Recording → Transcription → Summary → HubSpot Note
# ---------------------------------------------------------------------------

@app.post("/webhook/zoom/recording")
async def zoom_recording_webhook(request: Request):
    """
    Zoom recording.completed webhook — triggered when a Zoom meeting recording
    is ready for download.

    Flow:
      1. Verify Zoom webhook signature (optional if ZOOM_WEBHOOK_SECRET set)
      2. Handle Zoom URL validation challenge (one-time setup)
      3. Find HubSpot contact via HubSpot Meetings (hs_meeting_external_url)
         Falls back to Zoom Participants API (lead joined with their email)
      4. Download audio recording from Zoom
      5. Transcribe with Whisper, summarize with Claude Haiku
      6. Write formatted summary as Note on HubSpot contact

    Env vars: ZOOM_ACCOUNT_ID, ZOOM_CLIENT_ID, ZOOM_CLIENT_SECRET,
              ZOOM_WEBHOOK_SECRET (optional), ZOOM_HOST_EMAIL (Kevin's email)
    """
    raw_body = await request.body()

    # Zoom webhook signature verification — always enforce when secret is configured.
    # Never skip: an attacker can simply omit the headers to bypass an "if headers present" check.
    zoom_secret = os.environ.get("ZOOM_WEBHOOK_SECRET", "")
    timestamp = request.headers.get("x-zm-request-timestamp", "")
    signature = request.headers.get("x-zm-signature", "")
    if zoom_secret:
        if not timestamp or not signature:
            raise HTTPException(status_code=401, detail="Missing Zoom webhook signature headers")
        if not verify_zoom_signature(raw_body, timestamp, signature):
            raise HTTPException(status_code=401, detail="Invalid Zoom webhook signature")

    import json as _json
    body = _json.loads(raw_body)
    event = body.get("event", "")

    # Zoom URL validation challenge — sent once during webhook setup
    if event == "endpoint.url_validation":
        plain_token = body.get("payload", {}).get("plainToken", "")
        import hashlib, hmac as _hmac, os as _os
        secret = _os.environ.get("ZOOM_WEBHOOK_SECRET", "")
        encrypted = _hmac.new(
            secret.encode(), plain_token.encode(), hashlib.sha256
        ).hexdigest()
        return {"plainToken": plain_token, "encryptedToken": encrypted}

    if event != "recording.completed":
        logger.debug("Zoom webhook: ignoring event=%s", event)
        return {"status": "ignored", "event": event}

    payload = body.get("payload", {}).get("object", {})
    meeting_id  = str(payload.get("id", ""))        # numeric meeting ID
    meeting_uuid = payload.get("uuid", "")           # unique UUID for this instance
    host_email   = payload.get("host_email", "")
    duration_min = payload.get("duration", 0)        # meeting duration in minutes
    topic        = payload.get("topic", "")

    logger.info(
        "Zoom recording.completed: meeting_id=%s uuid=%s host=%s duration=%dm topic=%s",
        meeting_id, meeting_uuid, host_email, duration_min, topic,
    )

    # Step 1: Find HubSpot contact
    # Primary: HubSpot Meetings (booked via HubSpot calendar)
    contact_id = await find_contact_by_zoom_meeting(meeting_id)

    # Fallback: Zoom Participants API (lead joined with their email)
    if not contact_id:
        logger.info("Zoom: HubSpot meeting lookup failed, trying participants API")
        zoom_host = host_email or os.environ.get("ZOOM_HOST_EMAIL", "")
        lead_email = await get_lead_email_from_meeting(meeting_id, host_email=zoom_host)
        if lead_email:
            # Resolve email → HubSpot contact ID
            import httpx as _httpx
            async with _httpx.AsyncClient(timeout=10.0) as _c:
                from integrations.hubspot import _resolve_hubspot_id
                contact_id = await _resolve_hubspot_id(lead_email, _c)

    if not contact_id:
        logger.warning(
            "Zoom recording: no HubSpot contact found for meeting %s — "
            "writing Slack alert for manual assignment",
            meeting_id,
        )
        # Fire-and-forget: return 200 so Zoom doesn't retry
        return {
            "status": "no_contact",
            "message": f"No HubSpot contact found for meeting {meeting_id}. Manual assignment needed.",
        }

    # Step 2: Download VTT transcript (Zoom auto-generates, free, instant)
    vtt_url = await get_vtt_url(meeting_uuid)
    if not vtt_url:
        logger.error("Zoom recording: no VTT transcript found for meeting %s", meeting_uuid)
        return {
            "status": "no_vtt",
            "meeting_id": meeting_id,
            "hint": "Enable cloud recording + audio transcript in Zoom settings",
        }

    try:
        vtt_bytes = await download_recording(vtt_url)
        vtt_content = vtt_bytes.decode("utf-8", errors="replace")
    except Exception as e:
        logger.error("Zoom recording: VTT download failed for meeting %s: %s", meeting_id, e)
        return {"status": "download_failed", "error": str(e)}

    # Step 3: Parse VTT + Summarize via Claude Haiku
    summary = await process_zoom_vtt(
        vtt_content=vtt_content,
        duration_minutes=duration_min,
    )

    # Step 4: Write Note to HubSpot contact
    note_id = await add_note(
        contact_id=contact_id,
        body=summary,
        timestamp=datetime.now(timezone.utc),
    )

    logger.info(
        "Zoom recording: summary written to HubSpot contact=%s note=%s",
        contact_id, note_id,
    )

    return {
        "status": "ok",
        "contact_id": contact_id,
        "note_id": note_id,
        "meeting_id": meeting_id,
        "duration_minutes": duration_min,
    }


# ---------------------------------------------------------------------------
# CIO Segment Sync — Pull segment members and store as touchpoints
# ---------------------------------------------------------------------------

@app.post("/admin/sync-cio-segment")
async def sync_cio_segment(
    request: Request,
    segment_id: int = 296,
    event_type: str = "webinar_registered",
):
    """
    Pull all members from a CIO segment and store as touchpoints in Supabase.

    Used for bulk-importing CIO segment members (e.g. MC Launchcall registrants)
    into the scoring pipeline. Each member gets a touchpoint with the specified
    event_type, so the batch scorer picks them up on the next run.

    Requires DEBUG_API_KEY header for auth.
    """
    debug_key = os.environ.get("DEBUG_API_KEY", "")
    auth = request.headers.get("x-api-key", "")
    if not debug_key or auth != debug_key:
        raise HTTPException(status_code=401, detail="Unauthorized")

    import urllib.request as _urllib_req
    cio_app_key = os.environ.get("CIO_APP_API_KEY", "")
    if not cio_app_key:
        raise HTTPException(status_code=500, detail="CIO_APP_API_KEY not set")

    # Paginate through CIO segment members
    all_members: list[dict] = []
    cursor: str | None = None
    for _ in range(200):  # safety limit
        url = f"https://api-eu.customer.io/v1/segments/{segment_id}/membership?limit=100"
        if cursor:
            url += f"&start={cursor}"
        req = _urllib_req.Request(url, headers={
            "Authorization": f"Bearer {cio_app_key}",
        })
        try:
            with _urllib_req.urlopen(req) as resp:
                import json as _json
                page = _json.loads(resp.read())
        except Exception as e:
            logger.error("CIO segment sync: fetch page failed: %s", e)
            break

        identifiers = page.get("identifiers", [])
        all_members.extend(identifiers)
        cursor = page.get("next", "")
        if not cursor or not identifiers:
            break

    logger.info("CIO segment sync: fetched %d members from segment %d", len(all_members), segment_id)

    # Store each member as a touchpoint in Supabase
    from integrations.supabase import store_cio_email_event, fetch_contact_by_email, get_supabase_client

    stored = 0
    skipped = 0
    client = get_supabase_client()
    now_iso = datetime.now(timezone.utc).isoformat()

    for member in all_members:
        email = member.get("email", "")
        if not email:
            skipped += 1
            continue

        # Check if contact exists in Supabase
        contact = await fetch_contact_by_email(email)
        if not contact:
            skipped += 1
            continue

        # Store as touchpoint
        try:
            await client._post("touchpoints", {
                "contact_id": contact["id"],
                "channel": "direct",
                "source": "customerio",
                "medium": "segment",
                "touchpoint_type": "form_submit",
                "content": f"CIO Segment {segment_id}: MC Launchcall",
                "campaign": f"segment_{segment_id}",
                "created_at": now_iso,
                "is_first_touch": False,
                "is_last_touch": False,
            })
            stored += 1
        except Exception as e:
            logger.warning("CIO segment sync: store failed for %s: %s", email, e)

    logger.info("CIO segment sync: stored %d, skipped %d", stored, skipped)
    return {
        "status": "ok",
        "segment_id": segment_id,
        "total_members": len(all_members),
        "stored": stored,
        "skipped": skipped,
    }
