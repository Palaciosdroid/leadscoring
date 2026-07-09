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
# 7th disposition — created in the HubSpot UI (no create API, 405). Matched by
# LABEL so the new GUID is picked up via the dynamic disposition map without a
# deploy. Kevin selects it when a lead says "kein Interesse" -> permanent stop.
NOT_INTERESTED_OUTCOMES = frozenset({"Nicht interessiert"})


def classify_outcome(outcome: str) -> str | None:
    """Map a raw HubSpot disposition label to an outcome class.

    Returns 'reached' | 'no_answer' | 'wrong_number' | 'not_interested'
    | None (unknown).
    """
    label = (outcome or "").strip()
    if label in REACHED_OUTCOMES:
        return "reached"
    if label in NO_ANSWER_OUTCOMES:
        return "no_answer"
    if label in WRONG_NUMBER_OUTCOMES:
        return "wrong_number"
    if label in NOT_INTERESTED_OUTCOMES:
        return "not_interested"
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

    if outcome_class in ("wrong_number", "not_interested"):
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
