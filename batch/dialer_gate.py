"""Shared dialer suppression gate.

The realtime + WhatsApp webhooks (main.py) push leads straight into Kevin's
Aircall Power Dialer. Unlike the batch path (scorer.py), they did not consult
the lead's stored HubSpot lifecycle state, so an unsubscribed / paused / booked /
wrong-number / not-interested contact got re-pushed whenever a new event fired
(e.g. a phone edit or an inbound WhatsApp message on an existing contact).

dialer_suppressed() reproduces the batch exclusion truth from a HubSpot props
dict: check_do_not_call (unsubscribed, phone_dnc, booked, recently-called,
purchased, not_interested, permanent outcome) plus the pause/removed window.
Webhooks carry no scored-events history, so intent-reactivation is intentionally
NOT applied here — that errs on the safe side (suppress rather than dial).
"""

import logging
from datetime import datetime, timezone

from batch.do_not_call import check_do_not_call
from integrations.hubspot import (
    get_contact_id,
    get_contact_properties,
    has_upcoming_hubspot_meeting,
)

logger = logging.getLogger(__name__)

# HubSpot properties needed to reproduce the batch exclusion decision.
_GATE_PROPS = [
    "lead_pause_until",
    "lead_dialer_removed",
    "lead_phone_dnc",
    "lead_call_booked",
    "lead_not_interested",
    "lead_last_call_outcome",
    "lead_last_call_date",
    "hs_email_optout",
]


def _truthy(value) -> bool:
    return str(value).strip().lower() in ("true", "1", "yes", "y", "ja")


def _is_paused(props: dict, now: datetime) -> bool:
    """Mirror of scorer._is_paused_or_removed, minus intent-reactivation.

    Webhooks have no scored-events history, so an active pause is honoured
    unconditionally (safe direction). Returns True when the lead is removed or
    inside an active pause window.
    """
    if _truthy(props.get("lead_dialer_removed")):
        return True
    raw = (props.get("lead_pause_until") or "").strip()
    if not raw:
        return False
    try:
        pause_until = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return False
    return now < pause_until


async def dialer_suppressed(
    *,
    email: str | None = None,
    phone: str | None = None,
    contact_id: str | None = None,
    funnel: str = "",
) -> tuple[bool, str]:
    """Return (suppressed, reason). suppressed=True → do NOT push to the dialer.

    Resolves the HubSpot contact (by id, then email, then phone), loads its
    lifecycle props, and applies the same hard exclusions the batch enforces.
    A contact that cannot be found returns (False, "new") — a brand-new lead
    with no HubSpot record yet is dialable.
    """
    hs_id = contact_id if (contact_id and str(contact_id).isdigit()) else None
    if not hs_id:
        hs_id = await get_contact_id(email=email, phone=phone)
    if not hs_id:
        return (False, "new")  # not in HubSpot yet → treat as a clean new lead

    # Empty dict on a failed/minimal fetch — fail OPEN on missing props (a new
    # fresh lead must still get through) but ALWAYS run the independent booked
    # check below, so a booked lead is caught even if the props fetch hiccuped.
    props = await get_contact_properties(hs_id, _GATE_PROPS) or {}

    now = datetime.now(timezone.utc)
    if _is_paused(props, now):
        return (True, "paused_or_removed")

    # Booked: explicit property OR an upcoming HubSpot meeting (closes the
    # Calendly/HubSpot-only booking gap the webhooks had no view into).
    booked = _truthy(props.get("lead_call_booked")) or await has_upcoming_hubspot_meeting(hs_id)

    dnc = await check_do_not_call(
        email=email or "",
        funnel=funnel,
        hubspot_contact_id=hs_id,
        last_call_date=props.get("lead_last_call_date"),
        call_booked=booked,
        not_interested=_truthy(props.get("lead_not_interested")),
        unsubscribed=_truthy(props.get("hs_email_optout")),
        phone_dnc=_truthy(props.get("lead_phone_dnc")),
        call_outcome=props.get("lead_last_call_outcome"),
    )
    if dnc.should_skip:
        return (True, dnc.reason)

    return (False, "")
