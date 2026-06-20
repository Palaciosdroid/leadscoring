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
