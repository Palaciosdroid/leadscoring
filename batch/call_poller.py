"""
Call Polling Job — polls HubSpot every 5 min for completed calls.

Replaces the HubSpot Workflow → "Webhook senden" action which requires
Operations Hub Professional. This job runs via APScheduler (already live
for batch scoring) and needs zero extra infrastructure or paid plan.

Dedup strategy: in-memory set of processed call_ids. Resets on app restart
(acceptable — worst case: one extra Slack message after a Railway redeploy).
"""

import asyncio
import logging
from datetime import datetime, timezone

from integrations.aircall import (
    log_call_outcome as aircall_log_outcome,
    remove_from_power_dialer,
)
from integrations.hubspot import (
    CONNECTED_DISPOSITIONS,
    HS_DISPOSITION_MAP,
    poll_completed_calls,
    write_call_outcome,
    get_contact_properties,
    update_contact_properties,
)
from batch.lifecycle import (
    classify_outcome,
    apply_call_outcome,
    state_from_props,
    state_to_props,
)

logger = logging.getLogger(__name__)

# In-memory dedup set — prevents duplicate processing of the same call
# when the 10-min window overlaps two consecutive 5-min poll cycles.
_processed_call_ids: set[str] = set()


_LIFECYCLE_PROPS = [
    "lead_no_answer_streak",
    "lead_no_answer_cycles",
    "lead_pause_until",
    "lead_dialer_removed",
]


async def record_call_outcome(
    contact_id: str, outcome: str, now: datetime, phone: str = ""
) -> None:
    """SINGLE writer for call outcomes — used by the 5-min poller AND the
    HubSpot call webhook (main.py), so both paths apply identical rules:

      1. Apply the lifecycle state machine (reached -> 90d pause, no-answer
         streaks, wrong number -> removed).
      2. Write lifecycle props + last_call_date + last_call_outcome to HubSpot.
      3. If the outcome pauses or removes the lead, IMMEDIATELY pull them from
         Kevin's Aircall queue — don't wait for the next batch run. (RCA 03.07:
         reached leads stayed dialable because removal only happened in the
         batch, and the webhook path never wrote a pause at all.)

    Runs for EVERY disposition. Best-effort: logs and returns on any error.
    """
    if not contact_id:
        return
    outcome_class = classify_outcome(outcome)
    try:
        props = await get_contact_properties(contact_id, _LIFECYCLE_PROPS + ["phone"])
        state = state_from_props(props)
        new_state = apply_call_outcome(state, outcome_class, now)

        update = state_to_props(new_state)
        update["lead_last_call_date"] = now.isoformat()
        update["lead_last_call_outcome"] = outcome
        await update_contact_properties(contact_id, update)
        logger.info(
            "call_poller: lifecycle %s outcome=%s class=%s streak=%d cycles=%d removed=%s pause_until=%s",
            contact_id, outcome, outcome_class,
            new_state.no_answer_streak, new_state.no_answer_cycles,
            new_state.removed, update["lead_pause_until"],
        )

        # Step 3: enforce at the source — paused/removed leads leave the live
        # queue NOW, not on the next batch cycle.
        if new_state.removed or (new_state.pause_until and new_state.pause_until > now):
            target_phone = phone or props.get("phone", "")
            if target_phone:
                try:
                    gone = await remove_from_power_dialer(target_phone)
                    logger.info(
                        "call_poller: immediate dialer removal %s (%s) -> %s",
                        target_phone, outcome_class, gone,
                    )
                except Exception as rm_err:
                    logger.warning(
                        "call_poller: immediate removal failed for %s: %s",
                        target_phone, rm_err,
                    )
    except Exception as e:
        logger.error("call_poller: lifecycle persist failed for %s: %s", contact_id, e)


# Backwards-compatible alias (poller-internal name before the webhook shared it)
_persist_lifecycle = record_call_outcome


async def run_call_polling(since_minutes: int = 10) -> None:
    """
    Poll HubSpot for completed calls in the last `since_minutes` minutes.
    For each new (not-yet-seen) call: write outcome back to the contact + send Slack.

    Called every 5 min via APScheduler in main.py.
    """
    calls = await poll_completed_calls(since_minutes=since_minutes)

    # Filter out already-processed calls
    new_calls = [c for c in calls if c["call_id"] not in _processed_call_ids]

    if not new_calls:
        logger.debug("call_poller: 0 new calls (all %d already processed)", len(calls))
        return

    # Apply lifecycle state for EVERY new call (reached + no-answer + wrong number).
    # This is the single writer of lead_last_call_* and lifecycle properties.
    now = datetime.now(timezone.utc)
    for c in new_calls:
        outcome = HS_DISPOSITION_MAP.get(c.get("hs_call_disposition", ""), "Unknown")
        await record_call_outcome(
            c.get("contact_id", ""), outcome, now, phone=c.get("contact_phone", "")
        )

    # Split: Anschläge (no Slack) vs Meetings (connected → send Slack)
    connected_calls = [c for c in new_calls if c.get("hs_call_disposition") in CONNECTED_DISPOSITIONS]
    anschlaege      = [c for c in new_calls if c.get("hs_call_disposition") not in CONNECTED_DISPOSITIONS]

    # Silently dedup Anschläge — they count in the EOD report but get no individual Slack card
    for c in anschlaege:
        _processed_call_ids.add(c["call_id"])
    if anschlaege:
        logger.info("call_poller: %d Anschlag/-schläge (no Slack): %s", len(anschlaege),
                    [HS_DISPOSITION_MAP.get(c.get("hs_call_disposition",""),"?") for c in anschlaege[:5]])

    if not connected_calls:
        logger.debug("call_poller: 0 connected calls to report this cycle")
        return

    logger.info("call_poller: processing %d connected call(s) (Meetings)", len(connected_calls))

    async def _process(call: dict) -> None:
        call_id     = call["call_id"]
        contact_id  = call["contact_id"]
        disposition = call.get("hs_call_disposition", "")
        direction   = call.get("hs_call_direction") or "OUTBOUND"
        duration_ms = call.get("hs_call_duration", 0) or 0

        outcome      = HS_DISPOSITION_MAP.get(disposition, disposition or "Unknown")
        duration_sec = duration_ms // 1000
        contact_name = (
            f"{call.get('contact_firstname') or ''} {call.get('contact_lastname') or ''}".strip()
            or "Unknown"
        )

        phone = call.get("contact_phone", "")

        # 1b. Snapshot tier + score at first call (feedback loop for scoring calibration)
        try:
            from integrations.hubspot import get_contact_properties
            props = await get_contact_properties(contact_id, [
                "lead_tier_at_first_call", "lead_tier", "lead_combined_score",
            ])
            if not props.get("lead_tier_at_first_call"):
                from integrations.hubspot import update_contact_properties
                await update_contact_properties(contact_id, {
                    "lead_tier_at_first_call": props.get("lead_tier", ""),
                    "lead_score_at_first_call": props.get("lead_combined_score", "0"),
                })
                logger.info("call_poller: snapshotted tier=%s score=%s at first call for %s",
                            props.get("lead_tier"), props.get("lead_combined_score"), contact_name)
        except Exception as e:
            logger.debug("call_poller: tier snapshot failed for %s: %s", contact_name, e)

        # 2. Log outcome to Aircall contact (information field) — silently skips if not in Aircall
        if phone:
            try:
                await aircall_log_outcome(phone, outcome, contact_name)
            except Exception as ac_err:
                logger.warning("call_poller: Aircall outcome log failed for %s: %s", contact_name, ac_err)

        # No individual Slack card per call — meetings are summarised in the EOD report (18:00 CET)

        # Mark as processed AFTER successful handling
        _processed_call_ids.add(call_id)

        logger.info(
            "call_poller processed call=%s contact=%s direction=%s outcome=%s duration=%ds",
            call_id, contact_name, direction, outcome, duration_sec,
        )

    # Process all connected calls in parallel, log errors but don't crash the job
    results = await asyncio.gather(*[_process(c) for c in connected_calls], return_exceptions=True)
    errors = [r for r in results if isinstance(r, Exception)]
    if errors:
        for err in errors:
            logger.error("call_poller: error processing call: %s", err)
