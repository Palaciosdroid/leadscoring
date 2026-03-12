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

from integrations.hubspot import (
    HS_DISPOSITION_MAP,
    get_call_stats,
    poll_completed_calls,
    write_call_outcome,
)
from integrations.slack import send_call_report

logger = logging.getLogger(__name__)

# In-memory dedup set — prevents duplicate Slack messages for the same call
# when the 10-min window overlaps two consecutive 5-min poll cycles.
_processed_call_ids: set[str] = set()


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

    logger.info("call_poller: processing %d new call(s)", len(new_calls))

    # Fetch stats once, reused across all calls in this batch
    (calls_7d, calls_30d, calls_365d) = await get_call_stats()

    async def _process(call: dict) -> None:
        call_id     = call["call_id"]
        contact_id  = call["contact_id"]
        disposition = call.get("hs_call_disposition", "")
        direction   = call.get("hs_call_direction", "OUTBOUND")
        duration_ms = call.get("hs_call_duration", 0) or 0
        ts_raw      = call.get("hs_timestamp", "")

        outcome      = HS_DISPOSITION_MAP.get(disposition, disposition or "Unknown")
        duration_sec = duration_ms // 1000
        contact_name = (
            f"{call.get('contact_firstname', '')} {call.get('contact_lastname', '')}".strip()
            or "Unknown"
        )

        # Parse timestamp → human-readable string for Slack
        try:
            if isinstance(ts_raw, str) and ts_raw:
                ts = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
            elif ts_raw:
                ts = datetime.fromtimestamp(int(ts_raw) / 1000, tz=timezone.utc)
            else:
                ts = datetime.now(tz=timezone.utc)
            ts_str = ts.strftime("%Y-%m-%d %H:%M UTC")
        except (ValueError, TypeError, OSError):
            ts_str = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

        # 1. Write outcome back to HubSpot contact
        await write_call_outcome(contact_id, outcome)

        # 2. Send Slack call report
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

        # Mark as processed AFTER successful handling
        _processed_call_ids.add(call_id)

        logger.info(
            "call_poller processed call=%s contact=%s direction=%s outcome=%s duration=%ds",
            call_id, contact_name, direction, outcome, duration_sec,
        )

    # Process all new calls in parallel, log errors but don't crash the job
    results = await asyncio.gather(*[_process(c) for c in new_calls], return_exceptions=True)
    errors = [r for r in results if isinstance(r, Exception)]
    if errors:
        for err in errors:
            logger.error("call_poller: error processing call: %s", err)
