"""
Call-window gate: only push a number into the dialer when it's a sensible
local time to call (business hours, no Sunday). Region is derived from the
phone number; unknown regions fall back to Central European time.
"""
from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

# ISO region code -> representative timezone
REGION_TZ: dict[str, str] = {
    "CH": "Europe/Zurich",
    "DE": "Europe/Berlin",
    "AT": "Europe/Vienna",
}
DEFAULT_TZ = "Europe/Berlin"

WINDOW_START_HOUR = 9    # inclusive
WINDOW_END_HOUR = 20     # exclusive


def is_within_call_window(region: str | None, now_utc: datetime) -> bool:
    """True if `now_utc` falls within local business hours for `region`.

    Rule: 09:00 <= local_hour < 20:00 and weekday is not Sunday.
    """
    tz_name = REGION_TZ.get(region or "", DEFAULT_TZ)
    local = now_utc.astimezone(ZoneInfo(tz_name))
    if local.weekday() == 6:  # Sunday
        return False
    return WINDOW_START_HOUR <= local.hour < WINDOW_END_HOUR
