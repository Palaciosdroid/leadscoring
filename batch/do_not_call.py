"""
Do Not Call filter — checks whether a lead should be called or skipped.

All checks are async-compatible so they integrate cleanly with the
batch scoring pipeline.
"""

from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any
import logging

logger = logging.getLogger(__name__)

# Cooldown is handled by _is_in_cooldown() in scorer.py (7d answered, 3d no-answer).
# DNC only checks the hard 24h minimum to prevent double-calling on the same day.
CALL_COOLDOWN = timedelta(hours=24)


@dataclass
class DoNotCallResult:
    """Result of a Do Not Call check."""

    should_skip: bool
    reason: str  # empty if should_skip is False


async def check_do_not_call(
    email: str,
    funnel: str,
    *,
    cio_id: str | None = None,
    hubspot_contact_id: str | None = None,
    last_call_date: str | None = None,
    call_booked: bool = False,
    not_interested: bool = False,
    unsubscribed: bool = False,
    purchased_funnels: list[str] | None = None,
    call_outcome: str | None = None,
    **kwargs: Any,
) -> DoNotCallResult:
    """
    Master Do Not Call check.  Returns whether to skip and why.

    Checks (in order):
    1. Unsubscribed                              -> skip
    2. Inbound call already booked (HubSpot)     -> skip
    3. Called within last 24 h                    -> skip
    4. Already purchased same funnel (CIO)       -> skip
    5. Marked as "not interested" (Aircall)      -> skip
    """

    # 1. Unsubscribed (from CIO attributes or explicit parameter)
    if unsubscribed:
        logger.info("DNC skip [unsubscribed]: %s", email)
        return DoNotCallResult(should_skip=True, reason="unsubscribed")

    # 2. Call already booked in HubSpot calendar
    if call_booked:
        logger.info("DNC skip [call_booked]: %s", email)
        return DoNotCallResult(should_skip=True, reason="call_booked")

    # 3. Called within the last 24 hours
    if last_call_date:
        try:
            last_call_dt = datetime.fromisoformat(last_call_date)
            # Ensure timezone-aware comparison
            if last_call_dt.tzinfo is None:
                last_call_dt = last_call_dt.replace(tzinfo=timezone.utc)
            if datetime.now(timezone.utc) - last_call_dt < CALL_COOLDOWN:
                logger.info("DNC skip [called_recently]: %s (last: %s)", email, last_call_date)
                return DoNotCallResult(should_skip=True, reason="called_recently")
        except (ValueError, TypeError) as exc:
            logger.warning(
                "DNC: could not parse last_call_date '%s' for %s: %s",
                last_call_date, email, exc,
            )

    # 4. Already purchased the same funnel
    if purchased_funnels and funnel in purchased_funnels:
        logger.info("DNC skip [already_purchased]: %s (funnel=%s)", email, funnel)
        return DoNotCallResult(should_skip=True, reason="already_purchased")

    # 5. Marked as not interested (Aircall disposition / HubSpot property)
    if not_interested:
        logger.info("DNC skip [not_interested]: %s", email)
        return DoNotCallResult(should_skip=True, reason="not_interested")

    # 6. Disqualified / not qualified / cancelled — permanent removal outcomes
    # NOTE: call_outcome comes from the function parameter (line 39), NOT kwargs.
    if call_outcome:
        _PERMANENT_OUTCOMES = {
            "falsche nummer", "nicht_qualifiziert", "nicht qualifiziert",
            "disqualified", "abgesagt", "beratungsgespräch abgesagt",
        }
        if call_outcome.lower().strip() in _PERMANENT_OUTCOMES:
            logger.info("DNC skip [permanent_outcome]: %s — %s", email, call_outcome)
            return DoNotCallResult(should_skip=True, reason=f"permanent:{call_outcome}")

    return DoNotCallResult(should_skip=False, reason="")


async def filter_callable_leads(
    leads: list[dict[str, Any]],
    purchased_funnels_map: dict[str, list[str]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """
    Filter a list of leads through Do Not Call checks.

    Args:
        leads: List of lead dicts. Each must contain at least 'email' and
               'funnel'.  Optional keys: 'cio_id', 'hubspot_contact_id',
               'last_call_date', 'call_booked', 'not_interested',
               'unsubscribed'.
        purchased_funnels_map: Mapping of email -> list of purchased funnel
                               slugs (from Customer.io segments).

    Returns:
        (callable_leads, skipped_leads) — skipped leads get a
        '_skip_reason' key added.
    """
    callable_leads: list[dict[str, Any]] = []
    skipped_leads: list[dict[str, Any]] = []

    for lead in leads:
        email = lead.get("email", "")
        funnel = lead.get("funnel", "")

        result = await check_do_not_call(
            email=email,
            funnel=funnel,
            cio_id=lead.get("cio_id"),
            hubspot_contact_id=lead.get("hubspot_contact_id"),
            last_call_date=lead.get("last_call_date"),
            call_booked=bool(lead.get("call_booked", False)),
            not_interested=bool(lead.get("not_interested", False)),
            unsubscribed=bool(lead.get("unsubscribed", False)),
            purchased_funnels=purchased_funnels_map.get(email),
        )

        if result.should_skip:
            lead["_skip_reason"] = result.reason
            skipped_leads.append(lead)
        else:
            callable_leads.append(lead)

    logger.info(
        "DNC filter: %d callable, %d skipped out of %d total",
        len(callable_leads), len(skipped_leads), len(leads),
    )
    return callable_leads, skipped_leads
