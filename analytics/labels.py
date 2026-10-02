"""
Canonical conversion label — the single source of truth for "did this lead convert?".

CANONICAL DATA DEFINITION (do not redefine elsewhere; import from here):

  Conversion (primary):  HubSpot "Deal Won" in the Vertrieb pipeline.
      pipeline  id = 168455110  (Vertrieb)
      dealstage id = 311698367  (Won, 747 wins as of 2026-06-20)
      A contact is converted if any associated deal sits in that stage.

  Conversion (secondary / entry-level):  Whyros purchase with
      purchases.payment_status = 'completed'  (Supabase kugjoikxhdsueddbbeyu, RO).
      Matched by lowercased contact email.

  is_converted() combines both: True if the contact id is in the Won set
  OR the (normalized) email is in the completed-purchase set.

NEVER use total_purchases / total_revenue / lead_score as a conversion label —
those are derived/mutable fields, not ground truth. Revenue truth = HubSpot
deal amount + Bexio (owned by the Tracking-Crew), out of scope here.

Both fetches are READ-ONLY.
"""

import datetime
import enum
import os
import logging

import httpx

from integrations.supabase import get_supabase_client

logger = logging.getLogger(__name__)

# --- Canonical IDs (HubSpot Vertrieb pipeline, verified) ---
WON_DEAL_PIPELINE_ID = "168455110"
WON_DEAL_STAGE_ID = "311698367"

HUBSPOT_BASE = "https://api.hubapi.com"
ACCESS_TOKEN = os.environ.get("HUBSPOT_ACCESS_TOKEN", "")


def _headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }


def is_converted(
    contact_id: str | int | None,
    email: str | None,
    won_set: set[str],
    completed_set: set[str],
) -> bool:
    """
    Pure canonical label. True if the contact converted, by either source:
      - contact_id is in won_set        (HubSpot Deal Won), or
      - normalized email in completed_set (Whyros completed purchase).

    Email is matched case-insensitively and whitespace-trimmed. contact_id is
    coerced to str. Missing/empty inputs are simply skipped (never crash).
    """
    if contact_id is not None and str(contact_id) and str(contact_id) in won_set:
        return True
    if email:
        if email.strip().lower() in completed_set:
            return True
    return False


class ConversionTiming(enum.Enum):
    """Did the purchase happen after the signal, before it, or not at all?

    `is_converted` answers "did they ever buy", which is right for baseline and
    point calibration. It is wrong for signal calibration: a customer who bought
    two years ago and revisits the offer page today shows up as a hit for whatever
    signal that visit produced. Measured 19.08.2026 on the PostHog signal
    population — 22 of 26 resolvable conversions had bought BEFORE the signal, a
    median of 456 days before. That alone flattened every bucket to within four
    points of the others and inverted the ranking.

    PRIOR_BUYER is deliberately its own outcome and NOT a negative example. Those
    contacts already bought and are no longer in the market; scoring them as
    "did not convert" would understate every signal. Callers drop them from the
    population instead.
    """
    CONVERTED_AFTER = "converted_after"
    PRIOR_BUYER = "prior_buyer"
    NOT_CONVERTED = "not_converted"
    UNDECIDABLE = "undecidable"


def _as_utc(dt: datetime.datetime | None) -> datetime.datetime | None:
    """HubSpot and Supabase disagree about tzinfo; normalise instead of crashing."""
    if dt is None:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=datetime.timezone.utc)


def converted_after(
    contact_id: str | int | None,
    email: str | None,
    won_dates: dict[str, datetime.datetime],
    purchase_dates: dict[str, datetime.datetime],
    anchor: datetime.datetime | None,
) -> ConversionTiming:
    """Time-ordered counterpart to `is_converted`.

    `anchor` is the signal timestamp. A purchase strictly AFTER it is the only
    thing that can be called a conversion of that signal. A purchase at exactly
    the anchor counts as prior: identical timestamps cannot show which caused
    which, and claiming a hit there would be the same wishful reading this
    function exists to prevent.

    Without an anchor there is no ordering, so the answer is UNDECIDABLE — the
    caller drops the row rather than guessing.
    """
    if anchor is None:
        return ConversionTiming.UNDECIDABLE
    anchor = _as_utc(anchor)

    dates: list[datetime.datetime] = []
    if contact_id is not None and str(contact_id) in won_dates:
        dates.append(won_dates[str(contact_id)])
    if email:
        d = purchase_dates.get(email.strip().lower())
        if d is not None:
            dates.append(d)

    if not dates:
        return ConversionTiming.NOT_CONVERTED
    if any(_as_utc(d) > anchor for d in dates):
        return ConversionTiming.CONVERTED_AFTER
    return ConversionTiming.PRIOR_BUYER


async def fetch_won_contacts(*, timeout: float = 30.0) -> set[str]:
    """
    Return the set of HubSpot contact IDs with at least one Won deal in the
    Vertrieb pipeline (canonical conversion label, primary source).

    Paginates `/crm/v3/objects/deals/search` (pipeline + dealstage), then for
    each won deal reads `/associations/contacts`. READ-ONLY.
    """
    if not ACCESS_TOKEN:
        logger.warning("fetch_won_contacts: HUBSPOT_ACCESS_TOKEN not set — returning empty set")
        return set()

    contact_ids: set[str] = set()

    async with httpx.AsyncClient(timeout=timeout) as client:
        # Step 1: page through all Won deals (search API caps at 100/page)
        deal_ids: list[str] = []
        after: str | None = None
        while True:
            body: dict = {
                "filterGroups": [{
                    "filters": [
                        {"propertyName": "pipeline",  "operator": "EQ", "value": WON_DEAL_PIPELINE_ID},
                        {"propertyName": "dealstage", "operator": "EQ", "value": WON_DEAL_STAGE_ID},
                    ]
                }],
                "properties": ["hs_object_id"],
                "limit": 100,
            }
            if after:
                body["after"] = after

            resp = await client.post(
                f"{HUBSPOT_BASE}/crm/v3/objects/deals/search",
                headers=_headers(),
                json=body,
            )
            if resp.status_code != 200:
                logger.error(
                    "fetch_won_contacts: deal search failed %s %s",
                    resp.status_code, resp.text[:300],
                )
                break

            data = resp.json()
            deal_ids.extend(d["id"] for d in data.get("results", []))

            after = data.get("paging", {}).get("next", {}).get("after")
            if not after:
                break

        logger.info("fetch_won_contacts: %d won deals in pipeline %s", len(deal_ids), WON_DEAL_PIPELINE_ID)

        # Step 2: deal → associated contacts
        for deal_id in deal_ids:
            assoc = await client.get(
                f"{HUBSPOT_BASE}/crm/v3/objects/deals/{deal_id}/associations/contacts",
                headers=_headers(),
            )
            if assoc.status_code != 200:
                logger.debug("fetch_won_contacts: assoc fetch failed for deal %s: %s", deal_id, assoc.status_code)
                continue
            for row in assoc.json().get("results", []):
                cid = row.get("id")
                if cid:
                    contact_ids.add(str(cid))

    logger.info("fetch_won_contacts: %d distinct won contacts", len(contact_ids))
    return contact_ids


def _parse_ts(value) -> datetime.datetime | None:
    """HubSpot sends epoch millis or ISO; Supabase sends ISO. Never raise."""
    if not value:
        return None
    try:
        s = str(value)
        if s.isdigit():
            return datetime.datetime.fromtimestamp(int(s) / 1000, datetime.timezone.utc)
        return _as_utc(datetime.datetime.fromisoformat(s.replace("Z", "+00:00")))
    except (ValueError, OverflowError, OSError):
        return None


async def fetch_won_contact_dates(*, timeout: float = 30.0) -> dict[str, datetime.datetime]:
    """Contact ID → date of their most recent Won deal. READ-ONLY.

    Deliberately NOT sharing an implementation with `fetch_won_contacts`, even
    though both walk the same two endpoints. That function feeds baseline.py and
    calibrate_points.py and has no test coverage of its own; refactoring it into
    a wrapper would risk a silent change to the population those reports use
    (a deal without a closedate would have to be represented somehow). Twenty
    duplicated lines are the cheaper mistake here. If either gains tests, merge them.

    Contacts whose won deals all lack a closedate are omitted — an undated
    purchase cannot be ordered against a signal, and a guess would defeat the point.
    """
    if not ACCESS_TOKEN:
        logger.warning("fetch_won_contact_dates: HUBSPOT_ACCESS_TOKEN not set — returning empty")
        return {}

    deal_dates: dict[str, datetime.datetime] = {}
    contact_dates: dict[str, datetime.datetime] = {}

    async with httpx.AsyncClient(timeout=timeout) as client:
        after: str | None = None
        while True:
            body: dict = {
                "filterGroups": [{
                    "filters": [
                        {"propertyName": "pipeline",  "operator": "EQ", "value": WON_DEAL_PIPELINE_ID},
                        {"propertyName": "dealstage", "operator": "EQ", "value": WON_DEAL_STAGE_ID},
                    ]
                }],
                "properties": ["closedate"],
                "limit": 100,
            }
            if after:
                body["after"] = after
            resp = await client.post(
                f"{HUBSPOT_BASE}/crm/v3/objects/deals/search", headers=_headers(), json=body,
            )
            if resp.status_code != 200:
                logger.error(
                    "fetch_won_contact_dates: deal search failed %s %s",
                    resp.status_code, resp.text[:300],
                )
                break
            data = resp.json()
            for d in data.get("results", []):
                ts = _parse_ts(d.get("properties", {}).get("closedate"))
                if ts:
                    deal_dates[d["id"]] = ts
            after = data.get("paging", {}).get("next", {}).get("after")
            if not after:
                break

        undated = 0
        for deal_id, ts in deal_dates.items():
            assoc = await client.get(
                f"{HUBSPOT_BASE}/crm/v3/objects/deals/{deal_id}/associations/contacts",
                headers=_headers(),
            )
            if assoc.status_code != 200:
                undated += 1
                continue
            for row in assoc.json().get("results", []):
                cid = row.get("id")
                if not cid:
                    continue
                cid = str(cid)
                # Most recent win wins: a repeat customer's latest purchase is the
                # one a later signal has to be ordered against.
                if cid not in contact_dates or ts > contact_dates[cid]:
                    contact_dates[cid] = ts

    logger.info(
        "fetch_won_contact_dates: %d dated won deals → %d contacts (%d assoc lookups failed)",
        len(deal_dates), len(contact_dates), undated,
    )
    return contact_dates


async def fetch_completed_purchase_dates() -> dict[str, datetime.datetime]:
    """Lowercased email → date of their most recent completed Whyros purchase.

    READ-ONLY on Andre's Supabase — we never write there.

    Refunded purchases are excluded (`refunded_at` set). A refund is not a
    conversion, and `fetch_completed_purchase_emails` counts them today because
    payment_status stays 'completed' after a refund.

    ⚠️ `purchased_at` is the invoice date, not the checkout moment. For instalment
    and invoice purchases the browsing session can sit days to weeks earlier. That
    is tolerable here — the distortion this function exists to remove is measured
    in months — but it makes any single near-anchor case unreliable.
    """
    client = get_supabase_client()

    purchases = await client._get("purchases", {
        "select": "contact_id,purchased_at,refunded_at",
        "payment_status": "eq.completed",
    })

    by_contact: dict[str, datetime.datetime] = {}
    refunded = 0
    undated = 0
    for p in purchases:
        cid = p.get("contact_id")
        if not cid:
            continue
        if p.get("refunded_at"):
            refunded += 1
            continue
        ts = _parse_ts(p.get("purchased_at"))
        if ts is None:
            undated += 1
            continue
        cid = str(cid)
        if cid not in by_contact or ts > by_contact[cid]:
            by_contact[cid] = ts

    if not by_contact:
        logger.info("fetch_completed_purchase_dates: 0 usable completed purchases")
        return {}

    emails: dict[str, datetime.datetime] = {}
    contact_ids = list(by_contact)
    _CHUNK = 100
    for i in range(0, len(contact_ids), _CHUNK):
        chunk = contact_ids[i:i + _CHUNK]
        contacts = await client._get("contacts", {
            "select": "id,email",
            "id": f"in.({','.join(chunk)})",
        })
        for c in contacts:
            e = (c.get("email") or "").strip().lower()
            ts = by_contact.get(str(c.get("id")))
            if e and ts and (e not in emails or ts > emails[e]):
                emails[e] = ts

    logger.info(
        "fetch_completed_purchase_dates: %d emails dated (%d refunded skipped, %d undated skipped)",
        len(emails), refunded, undated,
    )
    return emails


async def fetch_completed_purchase_emails() -> set[str]:
    """
    Return the set of distinct, lowercased contact emails with at least one
    Whyros purchase where payment_status='completed' (secondary entry-level
    label). READ-ONLY via the shared Supabase client.

    purchases has no email column — it links to contacts via contact_id, so we
    fetch completed purchases, then resolve their contact_ids to emails.
    """
    client = get_supabase_client()

    purchases = await client._get("purchases", {
        "select": "contact_id",
        "payment_status": "eq.completed",
    })
    contact_ids = list({str(p["contact_id"]) for p in purchases if p.get("contact_id")})
    if not contact_ids:
        logger.info("fetch_completed_purchase_emails: 0 completed purchases")
        return set()

    # Resolve contact_ids → emails (chunked to stay within PostgREST URL limits)
    emails: set[str] = set()
    _CHUNK = 100
    for i in range(0, len(contact_ids), _CHUNK):
        chunk = contact_ids[i:i + _CHUNK]
        ids_csv = ",".join(chunk)
        contacts = await client._get("contacts", {
            "select": "id,email",
            "id": f"in.({ids_csv})",
        })
        for c in contacts:
            e = c.get("email")
            if e:
                emails.add(e.strip().lower())

    logger.info(
        "fetch_completed_purchase_emails: %d completed-purchase emails (from %d contacts)",
        len(emails), len(contact_ids),
    )
    return emails
