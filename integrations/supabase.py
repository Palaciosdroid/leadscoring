"""
Supabase REST API Client (READ-ONLY)

Fetches contacts, touchpoints, events, and purchases from Supabase via PostgREST.
Uses service_role key for full read access — no RLS restrictions.

Tables:
  - contacts: id, email, visitor_id, customerio_id, lead_score, status,
              first_purchase_at, total_purchases, funnel_id, audience_tag
  - touchpoints: id, contact_id, channel, source, medium, touchpoint_type,
                 content, campaign, is_first_touch, is_last_touch, created_at
  - events: id, visitor_id, event_type, event_name, page_url, page_title,
            event_properties, utm_source, utm_campaign, created_at
  - purchases: id, contact_id, product_name, product_key, product_category,
               amount, currency, amount_chf, purchased_at, refunded_at
"""

import asyncio
import os
import logging
from datetime import datetime, timezone, timedelta

import httpx

logger = logging.getLogger(__name__)

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")

# Contact fields to select by default
_CONTACT_FIELDS = (
    "id,email,visitor_id,customerio_id,lead_score,status,"
    "first_purchase_at,total_purchases,funnel_id,audience_tag"
)

# Touchpoint fields to select by default
_TOUCHPOINT_FIELDS = (
    "id,contact_id,channel,source,medium,touchpoint_type,"
    "content,campaign,is_first_touch,is_last_touch,created_at"
)

# Event fields to select
_EVENT_FIELDS = (
    "id,visitor_id,event_type,event_name,event_properties,"
    "page_url,page_title,utm_source,utm_campaign,created_at"
)

# Purchase fields to select
_PURCHASE_FIELDS = (
    "id,contact_id,product_name,product_key,product_category,"
    "amount,currency,amount_chf,purchased_at,refunded_at,funnel_id"
)


# PostgREST URL length limit: chunk large IN queries
_CHUNK_SIZE = 200  # max emails per IN clause to stay within URL limits


class SupabaseClient:
    """Async Supabase REST client — create once via get_supabase_client(), reuse."""

    def __init__(self) -> None:
        if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
            raise EnvironmentError(
                "SUPABASE_URL and SUPABASE_SERVICE_KEY must be set"
            )
        self._base = f"{SUPABASE_URL}/rest/v1"
        self._client = httpx.AsyncClient(
            timeout=15.0,
            headers={
                "apikey": SUPABASE_SERVICE_KEY,
                "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
                "Content-Type": "application/json",
                "Prefer": "return=representation",
            },
        )

    async def close(self) -> None:
        """Close the underlying HTTP client."""
        await self._client.aclose()

    async def _get(self, table: str, params: dict | None = None) -> list[dict]:
        """Generic GET against PostgREST. Returns parsed JSON list."""
        url = f"{self._base}/{table}"
        response = await self._client.get(url, params=params or {})
        if response.status_code != 200:
            logger.error(
                "Supabase GET %s failed: %s %s",
                table, response.status_code, response.text[:500],
            )
            response.raise_for_status()
        return response.json()


# ---------------------------------------------------------------------------
# Singleton
# ---------------------------------------------------------------------------

_instance: SupabaseClient | None = None


def get_supabase_client() -> SupabaseClient:
    """Return a module-level singleton SupabaseClient."""
    global _instance
    if _instance is None:
        _instance = SupabaseClient()
    return _instance


# ---------------------------------------------------------------------------
# Public query functions
# ---------------------------------------------------------------------------


async def fetch_contacts_with_touchpoints(days: int = 30) -> list[dict]:
    """
    Fetch all contacts that have touchpoints in the last N days.

    Uses two queries:
      1. Touchpoints created in window -> collect unique contact_ids
      2. Contacts by those IDs + their touchpoints

    Returns list of contact dicts, each with a nested 'touchpoints' list.
    """
    client = get_supabase_client()
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

    # Step 1: Get recent touchpoints (only need contact_id to find relevant contacts)
    touchpoints = await client._get("touchpoints", {
        "select": _TOUCHPOINT_FIELDS,
        "created_at": f"gte.{cutoff}",
        "order": "created_at.desc",
    })

    if not touchpoints:
        logger.info("fetch_contacts_with_touchpoints: 0 touchpoints in last %d days", days)
        return []

    # Collect unique contact IDs
    contact_ids = list({tp["contact_id"] for tp in touchpoints if tp.get("contact_id")})
    if not contact_ids:
        return []

    # Step 2: Fetch contacts by IDs
    # PostgREST IN filter: id=in.(uuid1,uuid2,...)
    ids_csv = ",".join(str(cid) for cid in contact_ids)
    contacts = await client._get("contacts", {
        "select": _CONTACT_FIELDS,
        "id": f"in.({ids_csv})",
    })

    # Build lookup: contact_id -> touchpoints list
    tp_by_contact: dict[str, list[dict]] = {}
    for tp in touchpoints:
        cid = tp.get("contact_id")
        if cid:
            tp_by_contact.setdefault(str(cid), []).append(tp)

    # Merge touchpoints into each contact
    for contact in contacts:
        contact["touchpoints"] = tp_by_contact.get(str(contact["id"]), [])

    logger.info(
        "fetch_contacts_with_touchpoints: %d contacts with %d touchpoints (last %dd)",
        len(contacts), len(touchpoints), days,
    )
    return contacts


async def fetch_touchpoints_for_emails(
    emails: list[str],
    days: int = 30,
) -> dict[str, list[dict]]:
    """
    Given email addresses, fetch all their touchpoints from the last N days.

    Resolves emails -> contact_ids first, then bulk-fetches touchpoints.
    Chunks large email lists to avoid PostgREST URL length limits.
    Returns {email: [touchpoint_dicts]}.
    """
    if not emails:
        return {}

    client = get_supabase_client()
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

    # Step 1: Resolve emails to contacts (chunked)
    all_contacts: list[dict] = []
    for i in range(0, len(emails), _CHUNK_SIZE):
        chunk = emails[i:i + _CHUNK_SIZE]
        emails_csv = ",".join(chunk)
        contacts = await client._get("contacts", {
            "select": "id,email",
            "email": f"in.({emails_csv})",
        })
        all_contacts.extend(contacts)

    if not all_contacts:
        logger.info("fetch_touchpoints_for_emails: no contacts found for %d emails", len(emails))
        return {e: [] for e in emails}

    # Build mappings
    email_by_id: dict[str, str] = {}
    contact_ids: list[str] = []
    for c in all_contacts:
        cid = str(c["id"])
        contact_ids.append(cid)
        email_by_id[cid] = c["email"]

    # Step 2: Bulk fetch touchpoints (chunked by contact_ids)
    all_touchpoints: list[dict] = []
    for i in range(0, len(contact_ids), _CHUNK_SIZE):
        chunk_ids = contact_ids[i:i + _CHUNK_SIZE]
        ids_csv = ",".join(chunk_ids)
        tps = await client._get("touchpoints", {
            "select": _TOUCHPOINT_FIELDS,
            "contact_id": f"in.({ids_csv})",
            "created_at": f"gte.{cutoff}",
            "order": "created_at.desc",
        })
        all_touchpoints.extend(tps)

    # Group by email
    result: dict[str, list[dict]] = {e: [] for e in emails}
    for tp in all_touchpoints:
        cid = str(tp.get("contact_id", ""))
        email = email_by_id.get(cid)
        if email and email in result:
            result[email].append(tp)

    logger.info(
        "fetch_touchpoints_for_emails: %d touchpoints for %d/%d emails (last %dd)",
        len(all_touchpoints), sum(1 for v in result.values() if v), len(emails), days,
    )
    return result


async def _resolve_contacts_for_emails(
    emails: list[str],
) -> list[dict]:
    """
    Shared contact lookup — chunked to avoid PostgREST URL limits.

    Returns list of contact dicts with id, email, visitor_id, customerio_id.
    """
    if not emails:
        return []

    client = get_supabase_client()
    all_contacts: list[dict] = []
    for i in range(0, len(emails), _CHUNK_SIZE):
        chunk = emails[i:i + _CHUNK_SIZE]
        emails_csv = ",".join(chunk)
        contacts = await client._get("contacts", {
            "select": "id,email,visitor_id,customerio_id",
            "email": f"in.({emails_csv})",
        })
        all_contacts.extend(contacts)
    return all_contacts


async def fetch_all_lead_data(
    emails: list[str],
    days: int = 30,
) -> dict[str, dict[str, list[dict] | str | None]]:
    """
    Single-call contact resolution + parallel data fetch for all lead data.

    Returns {email: {"events": [...], "purchases": [...], "meetings": [...],
                      "customerio_id": str|None}}.
    Uses ONE contacts query instead of 3 separate ones.
    """
    _empty = {"events": [], "purchases": [], "meetings": [], "customerio_id": None}
    if not emails:
        return {e: {**_empty} for e in emails}

    client = get_supabase_client()
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

    # Single contacts lookup (was 3 separate queries before)
    contacts = await _resolve_contacts_for_emails(emails)

    if not contacts:
        return {e: {**_empty} for e in emails}

    # Build all mappings from one contacts result
    email_by_id: dict[str, str] = {}
    email_by_visitor: dict[str, str] = {}
    cio_id_by_email: dict[str, str | None] = {}
    contact_ids: list[str] = []

    for c in contacts:
        cid = str(c["id"])
        contact_ids.append(cid)
        email_by_id[cid] = c["email"]
        cio_id_by_email[c["email"]] = c.get("customerio_id")
        vid = c.get("visitor_id")
        if vid:
            email_by_visitor[str(vid)] = c["email"]

    # Fetch events, purchases, meetings (chunked to avoid URL limits)

    async def _fetch_events_chunked() -> list[dict]:
        if not email_by_visitor:
            return []
        visitor_ids = list(email_by_visitor.keys())
        all_events: list[dict] = []
        for i in range(0, len(visitor_ids), _CHUNK_SIZE):
            chunk = visitor_ids[i:i + _CHUNK_SIZE]
            vids_csv = ",".join(chunk)
            evs = await client._get("events", {
                "select": _EVENT_FIELDS,
                "visitor_id": f"in.({vids_csv})",
                "created_at": f"gte.{cutoff}",
                "order": "created_at.desc",
            })
            all_events.extend(evs)
        return all_events

    async def _fetch_purchases_chunked() -> list[dict]:
        all_purchases: list[dict] = []
        for i in range(0, len(contact_ids), _CHUNK_SIZE):
            chunk = contact_ids[i:i + _CHUNK_SIZE]
            ids_csv = ",".join(chunk)
            ps = await client._get("purchases", {
                "select": _PURCHASE_FIELDS,
                "contact_id": f"in.({ids_csv})",
                "refunded_at": "is.null",
                "order": "purchased_at.desc",
            })
            all_purchases.extend(ps)
        return all_purchases

    async def _fetch_meetings_chunked() -> list[dict]:
        all_meetings: list[dict] = []
        for i in range(0, len(contact_ids), _CHUNK_SIZE):
            chunk = contact_ids[i:i + _CHUNK_SIZE]
            ids_csv = ",".join(chunk)
            ms = await client._get("meetings", {
                "select": "id,contact_id,meeting_type,scheduled_at,status,outcome,created_at",
                "contact_id": f"in.({ids_csv})",
                "order": "scheduled_at.desc",
            })
            all_meetings.extend(ms)
        return all_meetings

    events, purchases, meetings = await asyncio.gather(
        _fetch_events_chunked(), _fetch_purchases_chunked(), _fetch_meetings_chunked(),
    )

    # Initialize result — include customerio_id from the contacts lookup
    result: dict[str, dict] = {
        e: {"events": [], "purchases": [], "meetings": [],
            "customerio_id": cio_id_by_email.get(e)}
        for e in emails
    }

    # Group events by email (via visitor_id)
    for ev in events:
        vid = str(ev.get("visitor_id", ""))
        email = email_by_visitor.get(vid)
        if email and email in result:
            result[email]["events"].append(ev)

    # Group purchases by email (via contact_id)
    for p in purchases:
        cid = str(p.get("contact_id", ""))
        email = email_by_id.get(cid)
        if email and email in result:
            result[email]["purchases"].append(p)

    # Group meetings by email (via contact_id)
    for m in meetings:
        cid = str(m.get("contact_id", ""))
        email = email_by_id.get(cid)
        if email and email in result:
            result[email]["meetings"].append(m)

    logger.info(
        "fetch_all_lead_data: %d events, %d purchases, %d meetings for %d emails (last %dd)",
        len(events), len(purchases), len(meetings), len(emails), days,
    )
    return result


# Legacy wrappers (keep for backward compatibility)

async def fetch_events_for_emails(
    emails: list[str],
    days: int = 30,
) -> dict[str, list[dict]]:
    """Fetch browser events for emails. Prefer fetch_all_lead_data() for batch use."""
    data = await fetch_all_lead_data(emails, days=days)
    return {e: d["events"] for e, d in data.items()}


async def fetch_purchases_for_emails(
    emails: list[str],
) -> dict[str, list[dict]]:
    """Fetch purchases for emails. Prefer fetch_all_lead_data() for batch use."""
    data = await fetch_all_lead_data(emails, days=9999)
    return {e: d["purchases"] for e, d in data.items()}


async def fetch_meetings_for_emails(
    emails: list[str],
) -> dict[str, list[dict]]:
    """Fetch meetings for emails. Prefer fetch_all_lead_data() for batch use."""
    data = await fetch_all_lead_data(emails, days=9999)
    return {e: d["meetings"] for e, d in data.items()}


async def fetch_contact_by_email(email: str) -> dict | None:
    """
    Look up a single contact by email address.

    Returns contact dict with fields: id, email, customerio_id, lead_score,
    status, first_purchase_at, total_purchases, funnel_id, audience_tag.
    Returns None if not found.
    """
    if not email:
        return None

    client = get_supabase_client()
    results = await client._get("contacts", {
        "select": _CONTACT_FIELDS,
        "email": f"eq.{email}",
        "limit": "1",
    })

    if not results:
        logger.debug("fetch_contact_by_email: no contact for %s", email)
        return None

    return results[0]
