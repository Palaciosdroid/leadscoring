"""
HubSpot API Integration
Writes sbc_* custom properties back to a HubSpot contact.
Uses HubSpot Private App token (HUBSPOT_ACCESS_TOKEN env var).
"""

import os
import logging
from datetime import datetime, timezone
from typing import Any

import httpx

logger = logging.getLogger(__name__)

HUBSPOT_BASE = "https://api.hubapi.com"
ACCESS_TOKEN = os.environ.get("HUBSPOT_ACCESS_TOKEN", "")

# HubSpot call disposition UUIDs → readable labels
# (defined here — single source of truth for both webhook handler and call poller)
HS_DISPOSITION_MAP: dict[str, str] = {
    "f240bbac-87c9-4f6e-bf70-924b57d47db7": "Kontakt aufgenommen",
    "b2cf5968-551e-4856-9783-52b3da59a7d0": "Voicemail hinterlassen",
    "a4c4c377-d246-4b32-a13b-75a56a4cd0ff": "Live-Nachricht hinterlassen",
    "73a0d17f-1163-4015-bdd5-ec830791da20": "Keine Antwort",
    "9d9162e7-6cf3-4944-bf63-4dff82258764": "Besetzt",
    "17b47fee-58de-441e-a44c-c6300d46f273": "Falsche Nummer",
}

# Dispositions where a human actually picked up — these count as "Meetings".
# Everything else (Keine Antwort, Voicemail, Besetzt, Falsche Nummer) = Anschlag.
CONNECTED_DISPOSITIONS: frozenset[str] = frozenset({
    "f240bbac-87c9-4f6e-bf70-924b57d47db7",  # Kontakt aufgenommen
    "a4c4c377-d246-4b32-a13b-75a56a4cd0ff",  # Live-Nachricht hinterlassen
})


def _headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }


def _contact_url(contact_id: str) -> str:
    return f"{HUBSPOT_BASE}/crm/v3/objects/contacts/{contact_id}"


def _is_email(value: str) -> bool:
    return "@" in value and "." in value


async def _resolve_hubspot_id(email: str, client: httpx.AsyncClient) -> str | None:
    """Look up a HubSpot contact ID by email address."""
    response = await client.post(
        f"{HUBSPOT_BASE}/crm/v3/objects/contacts/search",
        headers=_headers(),
        json={
            "filterGroups": [{
                "filters": [{
                    "propertyName": "email",
                    "operator": "EQ",
                    "value": email,
                }]
            }],
            "limit": 1,
        },
    )
    if response.status_code != 200:
        logger.warning("HubSpot search failed: %s %s", response.status_code, response.text)
        return None
    results = response.json().get("results", [])
    if not results:
        logger.info("No HubSpot contact found for email=%s", email)
        return None
    return results[0]["id"]


async def _create_contact(
    lead_data: dict[str, Any],
    properties: dict[str, Any],
    client: httpx.AsyncClient,
) -> str:
    """
    Create a new HubSpot contact for a CIO-only lead.
    Includes basic identity fields + all score properties + funnel source.
    Returns the new HubSpot record ID.
    """
    create_props = {
        "email":              lead_data.get("email", ""),
        "firstname":          lead_data.get("firstname", ""),
        "lastname":           lead_data.get("lastname", ""),
        "phone":              lead_data.get("phone", ""),
        "lead_funnel_source": lead_data.get("funnel_source", ""),
        **properties,
    }
    # Strip empty strings so HubSpot doesn't store blank values
    create_props = {k: v for k, v in create_props.items() if v not in (None, "")}

    response = await client.post(
        f"{HUBSPOT_BASE}/crm/v3/objects/contacts",
        headers=_headers(),
        json={"properties": create_props},
    )
    if response.status_code not in (200, 201):
        logger.error("HubSpot CREATE failed: %s %s", response.status_code, response.text)
        response.raise_for_status()

    hs_id = str(response.json()["id"])
    logger.info(
        "HubSpot created new contact %s (hs_id=%s) tier=%s",
        lead_data.get("email"), hs_id, properties.get("lead_tier"),
    )
    return hs_id


async def upsert_contact_score(
    contact_id: str,
    payload: dict[str, Any],
    *,
    lead_data: dict[str, Any] | None = None,
    timeout: float = 10.0,
) -> dict[str, Any]:
    """
    Write lead_* score properties onto a HubSpot contact.
    - If contact_id is an email and found: PATCH existing contact.
    - If not found: CREATE new contact (requires lead_data with email/phone/name).
    Returns the HubSpot API response body.
    """
    if not ACCESS_TOKEN:
        raise EnvironmentError("HUBSPOT_ACCESS_TOKEN is not set")

    # Remove None values — HubSpot ignores nulls, but keep payload clean
    properties = {k: v for k, v in payload.items() if v is not None}

    async with httpx.AsyncClient(timeout=timeout) as client:
        hs_id = contact_id

        if _is_email(contact_id):
            resolved = await _resolve_hubspot_id(contact_id, client)
            if not resolved:
                # Contact exists in CIO but not HubSpot — create it
                if not lead_data:
                    raise ValueError(f"Contact not in HubSpot and no lead_data to create: {contact_id}")
                hs_id = await _create_contact(lead_data, properties, client)
                return {}   # Already wrote all props during create — nothing more to PATCH
            hs_id = resolved

        response = await client.patch(
            _contact_url(hs_id),
            headers=_headers(),
            json={"properties": properties},
        )

    if response.status_code not in (200, 204):
        logger.error(
            "HubSpot PATCH failed for contact %s (hs_id=%s): %s %s",
            contact_id, hs_id, response.status_code, response.text,
        )
        response.raise_for_status()

    logger.info("HubSpot updated contact %s (hs_id=%s) → tier=%s", contact_id, hs_id, payload.get("lead_tier"))
    return response.json() if response.content else {}


async def get_call_stats(*, timeout: float = 10.0) -> tuple[int, int, int]:
    """
    Return total call counts for the last 7 / 30 / 365 days (week, month, year).
    Runs 3 HubSpot CRM search queries in parallel.
    """
    if not ACCESS_TOKEN:
        return (0, 0, 0)

    now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    day_ms = 86_400_000  # milliseconds per day

    def _call_filter(days: int) -> dict:
        return {
            "filterGroups": [{
                "filters": [{
                    "propertyName": "hs_createdate",
                    "operator": "GTE",
                    "value": str(now_ms - days * day_ms),
                }]
            }],
            "properties": ["hs_createdate"],
            "limit": 1,
        }

    async with httpx.AsyncClient(timeout=timeout) as client:
        import asyncio
        responses = await asyncio.gather(
            client.post(f"{HUBSPOT_BASE}/crm/v3/objects/calls/search", headers=_headers(), json=_call_filter(7)),
            client.post(f"{HUBSPOT_BASE}/crm/v3/objects/calls/search", headers=_headers(), json=_call_filter(30)),
            client.post(f"{HUBSPOT_BASE}/crm/v3/objects/calls/search", headers=_headers(), json=_call_filter(365)),
            return_exceptions=True,
        )

    def _parse(r: Any) -> int:
        if isinstance(r, Exception):
            return 0
        try:
            return r.json().get("total", 0)
        except Exception:
            return 0

    return (_parse(responses[0]), _parse(responses[1]), _parse(responses[2]))


async def get_daily_call_stats(*, timeout: float = 15.0) -> tuple[int, int, int, int]:
    """
    Return today's call activity: (outbound_total, outbound_connected, inbound_connected, inbound_duration_sec).

    - outbound_total:     ALL COMPLETED outbound calls today (= Anschläge, from server-side total)
    - outbound_connected: subset where disposition in CONNECTED_DISPOSITIONS (= outbound Meetings)
    - inbound_connected:  COMPLETED inbound calls with connected disposition (= inbound Meetings)
    - inbound_duration_sec: total talk-time for inbound connected calls

    "Today" = since midnight UTC. Post-processes disposition in Python — avoids extra API queries.
    Accurate for up to 200 calls per direction per day (sufficient for this team size).
    """
    if not ACCESS_TOKEN:
        return (0, 0, 0, 0)

    import asyncio

    today = datetime.now(tz=timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    today_ms = int(today.timestamp() * 1000)

    def _daily_filter(direction: str) -> dict:
        return {
            "filterGroups": [{"filters": [
                {"propertyName": "hs_call_direction", "operator": "EQ",  "value": direction},
                {"propertyName": "hs_call_status",    "operator": "EQ",  "value": "COMPLETED"},
                {"propertyName": "hs_createdate",     "operator": "GTE", "value": str(today_ms)},
            ]}],
            # Fetch disposition + duration for all calls so we can post-process in Python
            "properties": ["hs_call_disposition", "hs_call_duration"],
            "limit": 200,
        }

    async with httpx.AsyncClient(timeout=timeout) as client:
        outbound_r, inbound_r = await asyncio.gather(
            client.post(f"{HUBSPOT_BASE}/crm/v3/objects/calls/search", headers=_headers(), json=_daily_filter("OUTBOUND")),
            client.post(f"{HUBSPOT_BASE}/crm/v3/objects/calls/search", headers=_headers(), json=_daily_filter("INBOUND")),
            return_exceptions=True,
        )

    outbound_total = outbound_connected = inbound_connected = inbound_duration_sec = 0

    if not isinstance(outbound_r, Exception) and outbound_r.status_code == 200:
        data = outbound_r.json()
        outbound_total = data.get("total", 0)  # server-side count — accurate even if >200
        for call in data.get("results", []):
            if call.get("properties", {}).get("hs_call_disposition") in CONNECTED_DISPOSITIONS:
                outbound_connected += 1
    elif isinstance(outbound_r, Exception):
        logger.warning("get_daily_call_stats outbound query failed: %s", outbound_r)

    if not isinstance(inbound_r, Exception) and inbound_r.status_code == 200:
        for call in inbound_r.json().get("results", []):
            props = call.get("properties", {})
            if props.get("hs_call_disposition") in CONNECTED_DISPOSITIONS:
                inbound_connected += 1
                dur = props.get("hs_call_duration")
                if dur:
                    try:
                        inbound_duration_sec += int(dur) // 1000
                    except (ValueError, TypeError):
                        pass
    elif isinstance(inbound_r, Exception):
        logger.warning("get_daily_call_stats inbound query failed: %s", inbound_r)

    logger.debug(
        "get_daily_call_stats: outbound_total=%d outbound_connected=%d inbound_connected=%d inbound_dur=%ds",
        outbound_total, outbound_connected, inbound_connected, inbound_duration_sec,
    )
    return (outbound_total, outbound_connected, inbound_connected, inbound_duration_sec)


async def get_latest_call_for_contact(
    contact_id: str,
    *,
    max_age_minutes: int = 60,
    timeout: float = 10.0,
) -> dict[str, Any] | None:
    """
    Fetch the most recently created call associated with a HubSpot contact.
    Returns call properties dict or None if no recent call found.

    max_age_minutes: only return calls created within this window (avoids
    processing stale calls when a workflow fires due to an email/meeting).
    """
    if not ACCESS_TOKEN:
        return None

    async with httpx.AsyncClient(timeout=timeout) as client:
        # Step 1: Get call associations for this contact
        assoc_resp = await client.get(
            f"{HUBSPOT_BASE}/crm/v3/objects/contacts/{contact_id}/associations/calls",
            headers=_headers(),
        )
        if assoc_resp.status_code != 200:
            logger.warning("associations fetch failed for contact %s: %s", contact_id, assoc_resp.status_code)
            return None

        call_ids = [r["id"] for r in assoc_resp.json().get("results", [])]
        if not call_ids:
            return None

        # Sort descending by ID (higher ID = more recently created)
        latest_call_id = sorted(call_ids, key=int, reverse=True)[0]

        # Step 2: Fetch call object with properties
        call_resp = await client.get(
            f"{HUBSPOT_BASE}/crm/v3/objects/calls/{latest_call_id}",
            headers=_headers(),
            params={
                "properties": (
                    "hs_call_direction,hs_call_disposition,hs_call_duration,"
                    "hs_call_status,hs_createdate,hs_timestamp"
                )
            },
        )
        if call_resp.status_code != 200:
            logger.warning("call fetch failed for id=%s: %s", latest_call_id, call_resp.status_code)
            return None

    props = call_resp.json().get("properties", {})

    # Guard: skip if call is older than max_age_minutes
    created_str = props.get("hs_createdate") or props.get("hs_timestamp")
    if created_str:
        try:
            from datetime import timedelta
            created_dt = datetime.fromisoformat(created_str.replace("Z", "+00:00"))
            if datetime.now(tz=timezone.utc) - created_dt > timedelta(minutes=max_age_minutes):
                logger.info("Latest call for contact %s is older than %dm — skipping", contact_id, max_age_minutes)
                return None
        except (ValueError, TypeError):
            pass  # if we can't parse, proceed anyway

    logger.info("Fetched latest call id=%s for contact %s", latest_call_id, contact_id)
    return props


async def write_call_outcome(
    contact_id: str,
    outcome: str,
    *,
    timeout: float = 10.0,
) -> None:
    """
    Write call outcome + timestamp back to HubSpot contact after each call.
    Properties: lead_last_call_date, lead_last_call_outcome.
    """
    if not ACCESS_TOKEN or not contact_id:
        return

    now_iso = datetime.now(tz=timezone.utc).isoformat()

    async with httpx.AsyncClient(timeout=timeout) as client:
        hs_id = contact_id
        if _is_email(contact_id):
            resolved = await _resolve_hubspot_id(contact_id, client)
            if not resolved:
                logger.warning("write_call_outcome: contact not found: %s", contact_id)
                return
            hs_id = resolved

        response = await client.patch(
            _contact_url(hs_id),
            headers=_headers(),
            json={"properties": {
                "lead_last_call_date":    now_iso,
                "lead_last_call_outcome": outcome,
            }},
        )

    if response.status_code not in (200, 204):
        logger.error("write_call_outcome PATCH failed for %s: %s %s", hs_id, response.status_code, response.text)
    else:
        logger.info("Call outcome written: hs_id=%s outcome=%s", hs_id, outcome)


async def get_prioritized_contacts(
    limit: int = 200,
    *,
    timeout: float = 30.0,
) -> list[dict[str, Any]]:
    """
    Return contacts sorted for outbound calling:
      1. Hot  (lead_tier=1_hot)  — not called in last 24h
      2. Warm (lead_tier=2_warm) — not called in last 48h
      3. Cold (lead_tier=3_cold) — not called in last 7d
    Only includes contacts with a phone number.
    Contacts never called are always included (no lead_last_call_date).
    """
    if not ACCESS_TOKEN:
        return []

    import asyncio

    now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    props = [
        "firstname", "lastname", "email", "phone",
        "lead_tier", "lead_combined_score", "lead_interest_category",
        "lead_last_call_date", "lead_last_call_outcome",
    ]

    async def _fetch_tier(tier: str, cooldown_ms: int) -> list[dict]:
        cutoff_ms = now_ms - cooldown_ms
        body = {
            # OR: (called before cutoff) OR (never called)
            "filterGroups": [
                {
                    "filters": [
                        {"propertyName": "lead_tier",           "operator": "EQ",               "value": tier},
                        {"propertyName": "phone",               "operator": "HAS_PROPERTY"},
                        {"propertyName": "lead_last_call_date", "operator": "LT",               "value": str(cutoff_ms)},
                    ]
                },
                {
                    "filters": [
                        {"propertyName": "lead_tier",           "operator": "EQ",               "value": tier},
                        {"propertyName": "phone",               "operator": "HAS_PROPERTY"},
                        {"propertyName": "lead_last_call_date", "operator": "NOT_HAS_PROPERTY"},
                    ]
                },
            ],
            "properties": props,
            "limit": min(limit, 100),
            "sorts": [{"propertyName": "lead_combined_score", "direction": "DESCENDING"}],
        }

        async with httpx.AsyncClient(timeout=timeout) as client:
            r = await client.post(
                f"{HUBSPOT_BASE}/crm/v3/objects/contacts/search",
                headers=_headers(),
                json=body,
            )

        if r.status_code != 200:
            logger.warning("get_prioritized_contacts tier=%s failed: %s %s", tier, r.status_code, r.text)
            return []

        results = r.json().get("results", [])
        for c in results:
            c["_tier"] = tier
        return results

    h, w, c = await asyncio.gather(
        _fetch_tier("1_hot",  24 * 3_600_000),    # Hot:  24h cooldown
        _fetch_tier("2_warm", 48 * 3_600_000),    # Warm: 48h cooldown
        _fetch_tier("3_cold",  7 * 86_400_000),   # Cold:  7d cooldown
        return_exceptions=True,
    )

    def _safe(r: Any) -> list:
        return r if isinstance(r, list) else []

    return [*_safe(h), *_safe(w), *_safe(c)]


async def poll_completed_calls(
    since_minutes: int = 10,
    *,
    timeout: float = 20.0,
) -> list[dict[str, Any]]:
    """
    Query HubSpot for completed calls created in the last `since_minutes` minutes.

    Returns a list of dicts with contact + call properties — same shape as
    HubSpotCallPayload so call_poller.py can reuse the exact same processing logic.

    Replaces the HubSpot Workflow → "Webhook senden" action (Operations Hub Pro
    required) with a free polling approach via APScheduler.
    """
    if not ACCESS_TOKEN:
        return []

    cutoff_ms = int(
        (datetime.now(tz=timezone.utc).timestamp() - since_minutes * 60) * 1000
    )

    search_body = {
        "filterGroups": [{
            "filters": [
                {"propertyName": "hs_call_status",  "operator": "EQ",  "value": "COMPLETED"},
                {"propertyName": "hs_createdate",   "operator": "GTE", "value": str(cutoff_ms)},
            ]
        }],
        "properties": [
            "hs_call_direction", "hs_call_disposition",
            "hs_call_duration", "hs_timestamp", "hs_createdate",
        ],
        "sorts": [{"propertyName": "hs_createdate", "direction": "DESCENDING"}],
        "limit": 50,
    }

    async with httpx.AsyncClient(timeout=timeout) as client:
        calls_resp = await client.post(
            f"{HUBSPOT_BASE}/crm/v3/objects/calls/search",
            headers=_headers(),
            json=search_body,
        )
        if calls_resp.status_code != 200:
            logger.warning(
                "poll_completed_calls: search failed %s %s",
                calls_resp.status_code, calls_resp.text,
            )
            return []

        calls = calls_resp.json().get("results", [])
        if not calls:
            logger.debug("poll_completed_calls: 0 completed calls in last %dm", since_minutes)
            return []

        import asyncio

        async def _enrich_call(call: dict) -> dict | None:
            """Attach associated contact_id + name to a call object."""
            call_id = call["id"]

            # Get associated contact
            assoc = await client.get(
                f"{HUBSPOT_BASE}/crm/v3/objects/calls/{call_id}/associations/contacts",
                headers=_headers(),
            )
            if assoc.status_code != 200 or not assoc.json().get("results"):
                logger.debug("poll_completed_calls: no contact for call %s", call_id)
                return None

            contact_id = assoc.json()["results"][0]["id"]

            # Get contact name + phone + lead tier + call stats for Slack reporting
            firstname, lastname, phone, lead_tier = "", "", "", ""
            calls_7d, calls_30d, calls_365d = 0, 0, 0
            c_resp = await client.get(
                f"{HUBSPOT_BASE}/crm/v3/objects/contacts/{contact_id}",
                headers=_headers(),
                params={"properties": "firstname,lastname,phone,lead_tier,hs_call_count_7d,hs_call_count_30d,hs_call_count_365d"},
            )
            if c_resp.status_code == 200:
                p = c_resp.json().get("properties", {})
                firstname = p.get("firstname", "") or ""
                lastname  = p.get("lastname",  "") or ""
                phone     = p.get("phone",     "") or ""
                lead_tier = p.get("lead_tier", "") or ""
                try:
                    calls_7d = int(p.get("hs_call_count_7d", "0") or 0)
                    calls_30d = int(p.get("hs_call_count_30d", "0") or 0)
                    calls_365d = int(p.get("hs_call_count_365d", "0") or 0)
                except (ValueError, TypeError):
                    pass

            props = call.get("properties", {})
            return {
                "call_id":            call_id,
                "contact_id":         contact_id,
                "contact_firstname":  firstname,
                "contact_lastname":   lastname,
                "contact_phone":      phone,
                "lead_tier":          lead_tier,
                "calls_7d":           calls_7d,
                "calls_30d":          calls_30d,
                "calls_365d":         calls_365d,
                "hs_call_direction":  props.get("hs_call_direction",  "OUTBOUND"),
                "hs_call_disposition": props.get("hs_call_disposition", ""),
                "hs_call_duration":   int(props.get("hs_call_duration") or 0),
                "hs_timestamp":       props.get("hs_timestamp") or props.get("hs_createdate") or "",
            }

        enriched = await asyncio.gather(
            *[_enrich_call(c) for c in calls],
            return_exceptions=True,
        )

    results = [r for r in enriched if isinstance(r, dict)]
    logger.info("poll_completed_calls: %d/%d calls enriched with contact", len(results), len(calls))
    return results


async def get_contact_events(contact_id: str) -> list[dict[str, Any]]:
    """
    Fetch the latest known sbc_* scores from HubSpot for a contact.
    Useful for batch re-scoring without re-fetching all events.
    """
    properties = [
        "lead_engagement_score", "lead_combined_score",
        "lead_tier", "lead_interest_category", "lead_score_updated_at",
    ]
    params = "&".join(f"properties={p}" for p in properties)

    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.get(
            f"{_contact_url(contact_id)}?{params}",
            headers=_headers(),
        )
    response.raise_for_status()
    return response.json().get("properties", {})


async def remove_from_lists(
    contact_id: str,
    *,
    timeout: float = 10.0,
) -> bool:
    """
    Remove a contact from all active HubSpot lists (triggered on unsubscribe).

    Queries list memberships, then removes the contact from each list.
    Returns True if successful, False otherwise.

    Used in unsubscribe automation workflow.
    """
    if not ACCESS_TOKEN or not contact_id:
        logger.warning("remove_from_lists: missing token or contact_id")
        return False

    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            # Step 1: Get all list memberships for this contact
            list_response = await client.get(
                f"{HUBSPOT_BASE}/crm/v3/objects/contacts/{contact_id}/associations/lists",
                headers=_headers(),
            )

            if list_response.status_code != 200:
                logger.warning(
                    "remove_from_lists: failed to fetch list memberships for %s: %s",
                    contact_id, list_response.status_code,
                )
                return False

            associations = list_response.json().get("results", [])
            list_ids = [a.get("id") for a in associations if a.get("id")]

            if not list_ids:
                logger.info("remove_from_lists: %s not in any lists", contact_id)
                return True  # Not in any lists, so mission accomplished

            # Step 2: Remove contact from each list
            removed_count = 0
            for list_id in list_ids:
                remove_response = await client.delete(
                    f"{HUBSPOT_BASE}/crm/v3/objects/lists/{list_id}/memberships",
                    headers=_headers(),
                    json={"inputs": [{"id": contact_id}]},
                )

                if remove_response.status_code in (200, 204):
                    removed_count += 1
                    logger.debug("remove_from_lists: removed %s from list %s", contact_id, list_id)
                else:
                    logger.warning(
                        "remove_from_lists: failed to remove %s from list %s: %s",
                        contact_id, list_id, remove_response.status_code,
                    )

            success = removed_count == len(list_ids)
            if success:
                logger.info("remove_from_lists: successfully removed %s from %d list(s)", contact_id, removed_count)
            else:
                logger.warning("remove_from_lists: removed from %d/%d lists for %s", removed_count, len(list_ids), contact_id)

            return success

    except Exception as e:
        logger.error("remove_from_lists: exception for %s: %s", contact_id, e)
        return False
