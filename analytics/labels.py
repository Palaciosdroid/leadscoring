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
