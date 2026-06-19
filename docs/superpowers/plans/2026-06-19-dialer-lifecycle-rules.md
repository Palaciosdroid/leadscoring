# Dialer Lifecycle State-Machine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Pause leads in the Aircall Power Dialer based on call outcome (reached → 3 months, 3× no-answer → 2 months), auto re-enter after the pause or on a fresh high-intent signal, and stop after 2 no-answer cycles.

**Architecture:** A pure state machine (`batch/lifecycle.py`) decides the next dialer state from each call outcome. `call_poller.py` persists that state to HubSpot properties after every call. `batch/scorer.py` reads `lead_pause_until` / `lead_dialer_removed` to gate the dialer queue and lifts the pause early when a high-intent event arrives after the last call.

**Tech Stack:** Python 3.12 (Railway), FastAPI, httpx, pytest. No new runtime dependency (phone validation is Plan 2).

**Companion plan:** `2026-06-19-dialer-phone-hygiene.md` (phonenumbers, invalid-flagging, dedupe-by-phone, call-window, telephone-DNC) — written separately.

**Spec:** `docs/superpowers/specs/2026-06-19-dialer-lifecycle-rules-design.md`

---

## Background the engineer must know

- The batch scorer (`batch/scorer.py:run_batch_scoring`) runs 3×/day (cron 08/12/16 CET) and pushes qualified leads to Aircall. The call poller (`batch/call_poller.py:run_call_polling`) runs every 5 min and writes call results back to HubSpot.
- Call outcomes are German disposition labels resolved from `integrations/hubspot.py:HS_DISPOSITION_MAP`:
  - reached: `Kontakt aufgenommen`, `Live-Nachricht hinterlassen`
  - no_answer: `Keine Antwort`, `Besetzt`, `Voicemail hinterlassen`
  - wrong_number: `Falsche Nummer`
- TWO existing gaps this plan fixes:
  1. `lead_call_attempts` is read in `scorer.py` but never written → counter is dead.
  2. `call_poller.py` only calls `write_call_outcome()` for *connected* calls; no-answer dispositions ("Anschläge") write nothing. The state machine needs every outcome.
- HubSpot helpers already available in `integrations/hubspot.py`:
  - `get_contact_properties(contact_id, properties) -> dict[str,str]`
  - `update_contact_properties(contact_id, properties: dict[str,str]) -> bool`
- `_truthy(value)` lives in `batch/scorer.py` and converts HubSpot `"true"/"false"` strings to bool.
- Tests run on Railway Python 3.12. The pure modules in this plan have no httpx dependency, so they also run under local Python 3.14.

---

### Task 1: Create the 4 lifecycle HubSpot properties

**Files:**
- Modify: `create_hs_properties.py` (append to `PROPERTIES` list, ends at line 80)

- [ ] **Step 1: Add the property definitions**

In `create_hs_properties.py`, insert these 4 dicts into the `PROPERTIES` list (before the closing `]` at line 80):

```python
    {
        "name": "lead_pause_until",
        "label": "Lead Pause Until",
        "type": "string",
        "fieldType": "text",
        "groupName": "contactinformation",
        "description": "ISO 8601 timestamp until which the lead is paused from the Aircall dialer",
    },
    {
        "name": "lead_no_answer_streak",
        "label": "Lead No-Answer Streak",
        "type": "number",
        "fieldType": "number",
        "groupName": "contactinformation",
        "description": "Consecutive no-answer calls; reset to 0 when the lead is reached",
    },
    {
        "name": "lead_no_answer_cycles",
        "label": "Lead No-Answer Cycles",
        "type": "number",
        "fieldType": "number",
        "groupName": "contactinformation",
        "description": "Completed 2-month no-answer pause cycles; at 2 the lead is removed",
    },
    {
        "name": "lead_dialer_removed",
        "label": "Lead Dialer Removed",
        "type": "enumeration",
        "fieldType": "booleancheckbox",
        "groupName": "contactinformation",
        "description": "True when the lead is permanently removed from the dialer (cycle cap or wrong number)",
        "options": [
            {"label": "true",  "value": "true",  "displayOrder": 0},
            {"label": "false", "value": "false", "displayOrder": 1},
        ],
    },
```

- [ ] **Step 2: Run the script (idempotent — prints SKIP if a property exists)**

Run: `python create_hs_properties.py`
Expected: four lines ending in `OK: lead_pause_until`, `OK: lead_no_answer_streak`, `OK: lead_no_answer_cycles`, `OK: lead_dialer_removed` (or `SKIP (already exists)` on re-runs).

- [ ] **Step 3: Commit**

```bash
git add create_hs_properties.py
git commit -m "feat: add dialer lifecycle HubSpot properties"
```

---

### Task 2: Build the pure lifecycle state machine

**Files:**
- Create: `batch/lifecycle.py`
- Test: `tests/test_lifecycle.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_lifecycle.py`:

```python
from datetime import datetime, timezone, timedelta

from batch.lifecycle import (
    LifecycleState,
    classify_outcome,
    apply_call_outcome,
    state_from_props,
    state_to_props,
)

NOW = datetime(2026, 6, 19, 12, 0, tzinfo=timezone.utc)


def test_classify_outcome_maps_known_labels():
    assert classify_outcome("Kontakt aufgenommen") == "reached"
    assert classify_outcome("Live-Nachricht hinterlassen") == "reached"
    assert classify_outcome("Keine Antwort") == "no_answer"
    assert classify_outcome("Besetzt") == "no_answer"
    assert classify_outcome("Voicemail hinterlassen") == "no_answer"
    assert classify_outcome("Falsche Nummer") == "wrong_number"
    assert classify_outcome("Etwas Unbekanntes") is None
    assert classify_outcome("") is None


def test_reached_sets_3_month_pause_and_resets_counters():
    state = LifecycleState(no_answer_streak=2, no_answer_cycles=1)
    new = apply_call_outcome(state, "reached", NOW)
    assert new.pause_until == NOW + timedelta(days=90)
    assert new.no_answer_streak == 0
    assert new.no_answer_cycles == 0
    assert new.removed is False


def test_no_answer_increments_streak_below_limit():
    state = LifecycleState(no_answer_streak=1)
    new = apply_call_outcome(state, "no_answer", NOW)
    assert new.no_answer_streak == 2
    assert new.pause_until is None
    assert new.no_answer_cycles == 0


def test_third_no_answer_triggers_2_month_pause_and_cycle():
    state = LifecycleState(no_answer_streak=2, no_answer_cycles=0)
    new = apply_call_outcome(state, "no_answer", NOW)
    assert new.pause_until == NOW + timedelta(days=60)
    assert new.no_answer_streak == 0
    assert new.no_answer_cycles == 1
    assert new.removed is False


def test_cycle_cap_removes_lead():
    # 2 cycles already done; the next 3rd-no-answer removes instead of pausing
    state = LifecycleState(no_answer_streak=2, no_answer_cycles=2)
    new = apply_call_outcome(state, "no_answer", NOW)
    assert new.removed is True
    assert new.no_answer_streak == 0


def test_wrong_number_removes_lead():
    new = apply_call_outcome(LifecycleState(), "wrong_number", NOW)
    assert new.removed is True


def test_unknown_outcome_is_noop():
    state = LifecycleState(no_answer_streak=1, no_answer_cycles=1)
    new = apply_call_outcome(state, None, NOW)
    assert new.no_answer_streak == 1
    assert new.no_answer_cycles == 1
    assert new.removed is False


def test_apply_does_not_mutate_input():
    state = LifecycleState(no_answer_streak=2)
    apply_call_outcome(state, "no_answer", NOW)
    assert state.no_answer_streak == 2  # original unchanged


def test_state_round_trips_through_props():
    state = LifecycleState(
        no_answer_streak=2,
        no_answer_cycles=1,
        pause_until=NOW + timedelta(days=60),
        removed=False,
    )
    props = state_to_props(state)
    assert props["lead_no_answer_streak"] == "2"
    assert props["lead_no_answer_cycles"] == "1"
    assert props["lead_dialer_removed"] == "false"
    back = state_from_props(props)
    assert back.no_answer_streak == 2
    assert back.no_answer_cycles == 1
    assert back.pause_until == NOW + timedelta(days=60)
    assert back.removed is False


def test_state_from_props_handles_missing_and_blank():
    back = state_from_props({})
    assert back.no_answer_streak == 0
    assert back.no_answer_cycles == 0
    assert back.pause_until is None
    assert back.removed is False
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `python -m pytest tests/test_lifecycle.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'batch.lifecycle'`.

- [ ] **Step 3: Write the implementation**

Create `batch/lifecycle.py`:

```python
"""
Lead lifecycle state machine for the Aircall Power Dialer.

Pure, side-effect-free functions deciding how a lead's dialer state changes
after each call outcome. State is persisted via HubSpot properties; the batch
scorer reads `pause_until` / `removed` to gate the dialer queue.

Outcome classes (from HS_DISPOSITION_MAP in integrations/hubspot.py):
  reached      -> Kontakt aufgenommen, Live-Nachricht hinterlassen
  no_answer    -> Keine Antwort, Besetzt, Voicemail hinterlassen
  wrong_number -> Falsche Nummer
"""
from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timedelta

# Pause durations (calendar months simplified to fixed days)
REACHED_PAUSE_DAYS = 90       # 3 months after a reached call
NO_ANSWER_PAUSE_DAYS = 60     # 2 months after 3 consecutive no-answers
NO_ANSWER_STREAK_LIMIT = 3    # consecutive no-answers that trigger a pause
MAX_NO_ANSWER_CYCLES = 2      # after this many no-answer pauses -> removed

REACHED_OUTCOMES = frozenset({"Kontakt aufgenommen", "Live-Nachricht hinterlassen"})
NO_ANSWER_OUTCOMES = frozenset({"Keine Antwort", "Besetzt", "Voicemail hinterlassen"})
WRONG_NUMBER_OUTCOMES = frozenset({"Falsche Nummer"})


def classify_outcome(outcome: str) -> str | None:
    """Map a raw HubSpot disposition label to an outcome class.

    Returns 'reached' | 'no_answer' | 'wrong_number' | None (unknown).
    """
    label = (outcome or "").strip()
    if label in REACHED_OUTCOMES:
        return "reached"
    if label in NO_ANSWER_OUTCOMES:
        return "no_answer"
    if label in WRONG_NUMBER_OUTCOMES:
        return "wrong_number"
    return None


@dataclass(frozen=True)
class LifecycleState:
    no_answer_streak: int = 0
    no_answer_cycles: int = 0
    pause_until: datetime | None = None
    removed: bool = False


def apply_call_outcome(
    state: LifecycleState, outcome_class: str | None, now: datetime
) -> LifecycleState:
    """Return a NEW LifecycleState after a call with the given outcome class.

    Pure — does not mutate `state`. Unknown outcome returns an equal copy.
    """
    if outcome_class == "reached":
        return replace(
            state,
            pause_until=now + timedelta(days=REACHED_PAUSE_DAYS),
            no_answer_streak=0,
            no_answer_cycles=0,
        )

    if outcome_class == "no_answer":
        streak = state.no_answer_streak + 1
        if streak >= NO_ANSWER_STREAK_LIMIT:
            if state.no_answer_cycles >= MAX_NO_ANSWER_CYCLES:
                return replace(state, removed=True, no_answer_streak=0)
            return replace(
                state,
                pause_until=now + timedelta(days=NO_ANSWER_PAUSE_DAYS),
                no_answer_streak=0,
                no_answer_cycles=state.no_answer_cycles + 1,
            )
        return replace(state, no_answer_streak=streak)

    if outcome_class == "wrong_number":
        return replace(state, removed=True)

    # Unknown outcome -> unchanged
    return state


def _to_int(value: str | None) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def state_from_props(props: dict[str, str]) -> LifecycleState:
    """Build a LifecycleState from raw HubSpot string properties."""
    pause_raw = (props.get("lead_pause_until") or "").strip()
    pause_until: datetime | None = None
    if pause_raw:
        try:
            pause_until = datetime.fromisoformat(pause_raw.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            pause_until = None
    removed = str(props.get("lead_dialer_removed", "")).lower() in ("true", "1", "yes")
    return LifecycleState(
        no_answer_streak=_to_int(props.get("lead_no_answer_streak")),
        no_answer_cycles=_to_int(props.get("lead_no_answer_cycles")),
        pause_until=pause_until,
        removed=removed,
    )


def state_to_props(state: LifecycleState) -> dict[str, str]:
    """Serialize a LifecycleState to HubSpot string properties."""
    return {
        "lead_no_answer_streak": str(state.no_answer_streak),
        "lead_no_answer_cycles": str(state.no_answer_cycles),
        "lead_dialer_removed": "true" if state.removed else "false",
        "lead_pause_until": state.pause_until.isoformat() if state.pause_until else "",
    }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `python -m pytest tests/test_lifecycle.py -v`
Expected: PASS (10 tests).

- [ ] **Step 5: Commit**

```bash
git add batch/lifecycle.py tests/test_lifecycle.py
git commit -m "feat: lead lifecycle state machine (pause/cycle/remove)"
```

---

### Task 3: Persist lifecycle state from the call poller (every disposition)

**Files:**
- Modify: `batch/call_poller.py` (imports at top; `run_call_polling` body; `_process` helper)

This makes the poller write call outcome + lifecycle state for ALL dispositions (today only connected calls write anything), and removes the now-duplicated `write_call_outcome` from the connected-only path.

- [ ] **Step 1: Add imports**

In `batch/call_poller.py`, replace the existing import block (lines 12-22) with:

```python
import asyncio
import logging
from datetime import datetime, timezone

from integrations.aircall import log_call_outcome as aircall_log_outcome
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
```

- [ ] **Step 2: Add the lifecycle-persist helper**

In `batch/call_poller.py`, add this function directly below the `_processed_call_ids` definition (after line 26):

```python
_LIFECYCLE_PROPS = [
    "lead_no_answer_streak",
    "lead_no_answer_cycles",
    "lead_pause_until",
    "lead_dialer_removed",
]


async def _persist_lifecycle(contact_id: str, outcome: str, now: datetime) -> None:
    """Load the lead's lifecycle state, apply the call outcome, and write it
    back to HubSpot together with last_call_date + last_call_outcome.

    Runs for EVERY disposition (reached AND no-answer) so the state machine
    sees no-answer streaks. Best-effort: logs and returns on any error.
    """
    if not contact_id:
        return
    outcome_class = classify_outcome(outcome)
    try:
        props = await get_contact_properties(contact_id, _LIFECYCLE_PROPS)
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
    except Exception as e:
        logger.error("call_poller: lifecycle persist failed for %s: %s", contact_id, e)
```

- [ ] **Step 3: Apply lifecycle to all new calls in `run_call_polling`**

In `batch/call_poller.py:run_call_polling`, insert this block immediately after the `new_calls` list is built (after line 39, before the connected/anschlaege split at line 45):

```python
    # Apply lifecycle state for EVERY new call (reached + no-answer + wrong number).
    # This is the single writer of lead_last_call_* and lifecycle properties.
    now = datetime.now(timezone.utc)
    for c in new_calls:
        outcome = HS_DISPOSITION_MAP.get(c.get("hs_call_disposition", ""), "Unknown")
        await _persist_lifecycle(c.get("contact_id", ""), outcome, now)
```

- [ ] **Step 4: Remove the now-duplicated outcome write from `_process`**

In `batch/call_poller.py:_process`, delete the line that re-writes the outcome (line 79):

```python
        # 1. Write outcome back to HubSpot contact
        await write_call_outcome(contact_id, outcome)
```

Leave the tier snapshot (step 1b) and the Aircall outcome log (step 2) untouched. `write_call_outcome` stays imported for other callers but is no longer used here.

- [ ] **Step 5: Verify the module imports cleanly**

Run: `python -c "import batch.call_poller"`
Expected: no output, exit code 0 (no import/syntax errors).

- [ ] **Step 6: Commit**

```bash
git add batch/call_poller.py
git commit -m "feat: persist lifecycle state for every call disposition"
```

---

### Task 4: Gate the dialer queue on pause/removed + intent reactivation

**Files:**
- Modify: `batch/scorer.py` (`_fetch_active_hubspot_leads` properties list ~line 143; add helpers; replace `_should_exclude_from_queue` call site ~line 1040)
- Test: `tests/test_dialer_pause.py`

- [ ] **Step 1: Write the failing tests for the pure helpers**

Create `tests/test_dialer_pause.py`:

```python
from datetime import datetime, timezone, timedelta

from batch.scorer import _is_intent_reactivated, _is_paused_or_removed

NOW = datetime(2026, 6, 19, 12, 0, tzinfo=timezone.utc)


def _iso(dt):
    return dt.isoformat()


def test_not_paused_when_no_pause_property():
    assert _is_paused_or_removed({}, NOW, []) is False


def test_paused_when_pause_until_in_future():
    props = {"lead_pause_until": _iso(NOW + timedelta(days=30))}
    assert _is_paused_or_removed(props, NOW, []) is True


def test_not_paused_when_pause_expired():
    props = {"lead_pause_until": _iso(NOW - timedelta(days=1))}
    assert _is_paused_or_removed(props, NOW, []) is False


def test_removed_is_always_excluded():
    props = {"lead_dialer_removed": "true"}
    assert _is_paused_or_removed(props, NOW, []) is True


def test_intent_event_after_last_call_lifts_pause():
    props = {
        "lead_pause_until": _iso(NOW + timedelta(days=30)),
        "lead_last_call_date": _iso(NOW - timedelta(days=10)),
    }
    events = [{"event_type": "checkout_visited", "timestamp": _iso(NOW - timedelta(days=2))}]
    assert _is_paused_or_removed(props, NOW, events) is False


def test_intent_event_before_last_call_does_not_lift_pause():
    props = {
        "lead_pause_until": _iso(NOW + timedelta(days=30)),
        "lead_last_call_date": _iso(NOW - timedelta(days=1)),
    }
    events = [{"event_type": "checkout_visited", "timestamp": _iso(NOW - timedelta(days=10))}]
    assert _is_paused_or_removed(props, NOW, events) is True


def test_low_intent_event_does_not_lift_pause():
    props = {
        "lead_pause_until": _iso(NOW + timedelta(days=30)),
        "lead_last_call_date": _iso(NOW - timedelta(days=10)),
    }
    events = [{"event_type": "page_visited", "timestamp": _iso(NOW - timedelta(days=2))}]
    assert _is_paused_or_removed(props, NOW, events) is True


def test_intent_reactivation_requires_last_call_date():
    events = [{"event_type": "checkout_visited", "timestamp": _iso(NOW)}]
    assert _is_intent_reactivated(events, None) is False
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_dialer_pause.py -v`
Expected: FAIL with `ImportError: cannot import name '_is_intent_reactivated'`.

- [ ] **Step 3: Add the helpers to `batch/scorer.py`**

In `batch/scorer.py`, add near the other module-level helpers (e.g. right after `_truthy` at the end of the file, or below `_should_exclude_from_queue`):

```python
# High-intent events that lift a pause early (intent reactivation).
HIGH_INTENT_EVENTS: frozenset[str] = frozenset({
    "checkout_visited",
    "price_info_viewed",
    "cta_clicked",
    "email_link_clicked",
})


def _is_intent_reactivated(
    scored_events: list[dict], last_call_date: str | None,
) -> bool:
    """True if a high-intent event occurred AFTER the last call (the pause anchor)."""
    if not last_call_date:
        return False
    try:
        anchor = datetime.fromisoformat(last_call_date.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return False
    for ev in scored_events:
        if ev.get("event_type") not in HIGH_INTENT_EVENTS:
            continue
        ts_raw = ev.get("timestamp", "")
        try:
            ts = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            continue
        if ts > anchor:
            return True
    return False


def _is_paused_or_removed(
    props: dict, now: datetime, scored_events: list[dict],
) -> bool:
    """Decide if a lead is currently excluded from the Aircall dialer.

    Excluded when removed (cycle cap / wrong number) or inside an active pause
    window — unless a high-intent event arrived after the last call.
    """
    if _truthy(props.get("lead_dialer_removed")):
        return True
    pause_raw = (props.get("lead_pause_until") or "").strip()
    if not pause_raw:
        return False
    try:
        pause_until = datetime.fromisoformat(pause_raw.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return False
    if now >= pause_until:
        return False  # pause expired -> re-enter
    if _is_intent_reactivated(scored_events, props.get("lead_last_call_date")):
        return False  # high-intent signal lifts the pause
    return True
```

- [ ] **Step 4: Fetch the new properties in `_fetch_active_hubspot_leads`**

In `batch/scorer.py:_fetch_active_hubspot_leads`, add the four properties to the `"properties"` list (currently lines 143-150):

```python
        "properties": [
            "email", "firstname", "lastname", "phone", "mobilephone",
            "lead_engagement_score", "lead_tier",
            "lead_interest_category",
            "lead_last_call_date", "lead_last_call_outcome",
            "lead_call_attempts", "lead_not_interested", "lead_call_booked",
            "hs_email_open_count", "hs_email_click_count",
            "lead_pause_until", "lead_no_answer_streak",
            "lead_no_answer_cycles", "lead_dialer_removed",
        ],
```

- [ ] **Step 5: Replace the cooldown exclusion with the pause check**

In `batch/scorer.py:run_batch_scoring`, find the exclusion block that currently calls `_should_exclude_from_queue` (around lines 1038-1054) and replace it with the pause/removed check. The new block:

```python
            # Exclusion: lifecycle pause / removed (replaces the old day-cooldowns).
            # Intent reactivation lifts an active pause when a high-intent event
            # arrived after the last call.
            if should_push and _is_paused_or_removed(props, now_utc, scored_events):
                logger.debug(
                    "Batch: dialer-pause exclude %s (pause_until=%s removed=%s)",
                    email, props.get("lead_pause_until"), props.get("lead_dialer_removed"),
                )
                should_push = False
```

Note: `now_utc` is already defined at the top of `run_batch_scoring` (`now_utc = datetime.now(timezone.utc)`). The old `_should_exclude_from_queue` function and its constants (`COOLDOWN_*`, `MAX_CALL_ATTEMPTS`, `PERMANENT_REMOVE_OUTCOMES`) become dead code — leave them in place for this task (removed in a follow-up cleanup), do NOT delete them now to keep the diff focused.

- [ ] **Step 6: Run the new tests to verify they pass**

Run: `python -m pytest tests/test_dialer_pause.py -v`
Expected: PASS (8 tests).

- [ ] **Step 7: Run the full suite to confirm nothing else broke**

Run: `python -m pytest tests/ -q`
Expected: existing tests still pass (any pre-existing httpx-related local failures are unrelated to this change; on Railway Python 3.12 all pass).

- [ ] **Step 8: Commit**

```bash
git add batch/scorer.py tests/test_dialer_pause.py
git commit -m "feat: gate dialer queue on lifecycle pause + intent reactivation"
```

---

## Self-Review (completed during planning)

**Spec coverage:**
- Pause-Loop with cap (max 2 cycles → removed): Task 2 (`apply_call_outcome`), verified by `test_cycle_cap_removes_lead`.
- Reached → 3-month pause: Task 2, `test_reached_sets_3_month_pause_and_resets_counters`.
- 3× no-answer → 2-month pause: Task 2, `test_third_no_answer_triggers_2_month_pause_and_cycle`.
- Dead attempt counter / no-answer not recorded: fixed in Task 3 (`_persist_lifecycle` runs for every disposition).
- Re-entry after pause: Task 4, `test_not_paused_when_pause_expired`.
- Intent reactivation: Task 4, `_is_intent_reactivated` + tests.
- `removed` terminal (no resurrect in v1): no un-remove path exists — matches spec decision.

**Deferred to companion plan (`dialer-phone-hygiene`):** phone validation/correction + `lead_phone_status`, invalid-flagging, dedupe-by-phone, call-window/timezone, `lead_phone_dnc`. Out of scope here on purpose.

**Type consistency:** `LifecycleState` fields (`no_answer_streak`, `no_answer_cycles`, `pause_until`, `removed`) are used identically across Tasks 2-4. Property names (`lead_pause_until`, `lead_no_answer_streak`, `lead_no_answer_cycles`, `lead_dialer_removed`) match across Tasks 1, 3, 4.

**Open confirmation:** `lead_dialer_removed` is a 4th lifecycle property beyond the spec's named set — it gives the `removed` state a persistent home (the spec described the state but not the field). Flag at review if a different representation (e.g. `lead_tier=4_disqualified`) is preferred.
