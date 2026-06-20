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
