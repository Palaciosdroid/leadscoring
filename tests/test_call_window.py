from datetime import datetime, timezone

from batch.call_window import is_within_call_window

# June 2026 = CEST (UTC+2). 2026-06-22 is a Monday, 2026-06-21 a Sunday.
def _utc(y, m, d, h):
    return datetime(y, m, d, h, 0, tzinfo=timezone.utc)


def test_within_business_hours_ch():
    # 08:00 UTC -> 10:00 CH local, Monday
    assert is_within_call_window("CH", _utc(2026, 6, 22, 8)) is True


def test_before_business_hours():
    # 05:00 UTC -> 07:00 CH local, Monday
    assert is_within_call_window("CH", _utc(2026, 6, 22, 5)) is False


def test_after_business_hours():
    # 19:00 UTC -> 21:00 CH local, Monday
    assert is_within_call_window("CH", _utc(2026, 6, 22, 19)) is False


def test_sunday_blocked():
    # 10:00 UTC -> 12:00 CH local, Sunday
    assert is_within_call_window("CH", _utc(2026, 6, 21, 10)) is False


def test_unknown_region_uses_default_tz():
    assert is_within_call_window(None, _utc(2026, 6, 22, 8)) is True
