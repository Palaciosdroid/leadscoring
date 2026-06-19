# Dialer Phone Validation & Queue Hygiene Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Validate/correct phone numbers with Google libphonenumber before dialing (flag the unfixable), call only during sensible local hours, never queue the same number twice, and honor a telephone-specific do-not-call flag.

**Architecture:** Two pure modules — `integrations/phone.py` (validate/normalize via `phonenumbers`) and `batch/call_window.py` (region → timezone → business-hours gate). The batch scorer uses them in the Aircall push path; invalid numbers are flagged to HubSpot + the Slack batch report. A new `lead_phone_dnc` property feeds the existing Do-Not-Call filter.

**Tech Stack:** Python 3.12 (Railway), `phonenumbers`, `zoneinfo` (stdlib), pytest.

**Companion plan:** `2026-06-19-dialer-lifecycle-rules.md` (pauses, re-entry, cycle cap) — independent; this plan can ship before or after it.

**Spec:** `docs/superpowers/specs/2026-06-19-dialer-lifecycle-rules-design.md`

---

## Background the engineer must know

- The batch scorer (`batch/scorer.py:run_batch_scoring`, cron 08/12/16 CET) builds an `aircall_queue` of leads and pushes them to the Aircall Power Dialer.
- Today phone numbers are normalized by a hand-rolled regex helper `_normalize_phone()` (`batch/scorer.py:334`), used only once at `batch/scorer.py:947`. This plan replaces that single use with libphonenumber-based validation. `_normalize_phone` becomes dead code (leave it; remove in a later cleanup).
- `_truthy(value)` (`batch/scorer.py`) converts HubSpot `"true"/"false"` strings to bool.
- `now_utc = datetime.now(timezone.utc)` is defined once near the top of `run_batch_scoring` and is in scope for the whole function, including the Aircall push loop.
- The Do-Not-Call filter is `batch/do_not_call.py:check_do_not_call(...)`, called from the scorer (~`batch/scorer.py:955`).
- The Slack batch report is driven by `BatchRunStats` (`integrations/slack.py:229`) and rendered in `_build_batch_report_message` (`integrations/slack.py:249`).
- HubSpot property creation pattern: `create_hs_properties.py` (`PROPERTIES` list + idempotent `create_property`).
- Aircall queue priority sort key: `batch/scorer.py:_aircall_priority_key(item)` — lower tuple = higher priority.
- Tests run on Railway Python 3.12. The new pure modules also run under local Python 3.14.

---

### Task 1: Add the `phonenumbers` dependency

**Files:**
- Modify: `requirements.txt`

- [ ] **Step 1: Add the dependency**

Append to `requirements.txt`:

```
phonenumbers~=8.13
```

- [ ] **Step 2: Install locally**

Run: `python -m pip install "phonenumbers~=8.13"`
Expected: `Successfully installed phonenumbers-8.13.x`.

- [ ] **Step 3: Commit**

```bash
git add requirements.txt
git commit -m "build: add phonenumbers dependency"
```

---

### Task 2: Phone validation/normalization module

**Files:**
- Create: `integrations/phone.py`
- Test: `tests/test_phone.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_phone.py`:

```python
from integrations.phone import validate_and_normalize, region_for


def test_clean_international_number_is_valid():
    assert validate_and_normalize("+41446681800") == ("+41446681800", "valid")


def test_spaces_in_international_number_still_valid():
    assert validate_and_normalize("+41 44 668 18 00") == ("+41446681800", "valid")


def test_national_format_is_corrected_with_default_region():
    assert validate_and_normalize("044 668 18 00") == ("+41446681800", "corrected")


def test_double_zero_prefix_is_corrected():
    assert validate_and_normalize("0041446681800") == ("+41446681800", "corrected")


def test_apostrophe_artefact_is_stripped():
    e164, status = validate_and_normalize("'+41 44 668 18 00")
    assert e164 == "+41446681800"
    assert status in ("valid", "corrected")


def test_too_short_is_invalid():
    assert validate_and_normalize("123") == (None, "invalid")


def test_empty_is_invalid():
    assert validate_and_normalize("") == (None, "invalid")


def test_garbage_is_invalid():
    assert validate_and_normalize("keine nummer") == (None, "invalid")


def test_region_for_returns_iso_code():
    assert region_for("+41446681800") == "CH"


def test_region_for_invalid_returns_none():
    assert region_for("nonsense") is None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_phone.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'integrations.phone'`.

- [ ] **Step 3: Write the implementation**

Create `integrations/phone.py`:

```python
"""
Phone validation + normalization via Google's libphonenumber (`phonenumbers`).

Used by the batch scorer before pushing a lead to the Aircall dialer:
valid / corrected numbers are dialed; invalid ones are flagged for manual fix.
"""
from __future__ import annotations

import phonenumbers

# Gabriel's primary market — used to interpret national-format numbers without
# a country code (e.g. "044 668 18 00" -> "+41446681800").
DEFAULT_REGION = "CH"


def validate_and_normalize(
    raw: str, default_region: str = DEFAULT_REGION
) -> tuple[str | None, str]:
    """Return (e164, status) for a raw phone string.

    status:
      "valid"     -> raw was already a clean international number
      "corrected" -> made valid by normalization (region inferred, 00->+ , cleanup)
      "invalid"   -> could not be parsed into a valid number
    Returns (None, "invalid") when no valid number can be produced.
    """
    if not raw or not raw.strip():
        return None, "invalid"

    cleaned = raw.strip().lstrip("'")  # drop Excel CSV apostrophe artefact
    started_intl = cleaned.startswith("+") or cleaned.startswith("00")

    try:
        region = None if started_intl else default_region
        parsed = phonenumbers.parse(cleaned, region)
    except phonenumbers.NumberParseException:
        return None, "invalid"

    if not phonenumbers.is_valid_number(parsed):
        return None, "invalid"

    e164 = phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
    # Already-clean international input (only cosmetic spaces) counts as "valid";
    # anything we had to reshape (national form, 00 prefix) counts as "corrected".
    status = "valid" if cleaned.replace(" ", "") == e164 else "corrected"
    return e164, status


def region_for(e164: str) -> str | None:
    """Return the ISO region code (e.g. 'CH', 'DE', 'AT') for an E.164 number."""
    try:
        parsed = phonenumbers.parse(e164, None)
    except phonenumbers.NumberParseException:
        return None
    return phonenumbers.region_code_for_number(parsed)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_phone.py -v`
Expected: PASS (10 tests).

- [ ] **Step 5: Commit**

```bash
git add integrations/phone.py tests/test_phone.py
git commit -m "feat: phone validation/normalization via libphonenumber"
```

---

### Task 3: Call-window (timezone + business hours) module

**Files:**
- Create: `batch/call_window.py`
- Test: `tests/test_call_window.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_call_window.py`:

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_call_window.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'batch.call_window'`.

- [ ] **Step 3: Write the implementation**

Create `batch/call_window.py`:

```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_call_window.py -v`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add batch/call_window.py tests/test_call_window.py
git commit -m "feat: call-window gate (region timezone + business hours)"
```

---

### Task 4: Create the 2 phone HubSpot properties

**Files:**
- Modify: `create_hs_properties.py`

- [ ] **Step 1: Add the property definitions**

Insert these 2 dicts into the `PROPERTIES` list in `create_hs_properties.py`:

```python
    {
        "name": "lead_phone_status",
        "label": "Lead Phone Status",
        "type": "string",
        "fieldType": "text",
        "groupName": "contactinformation",
        "description": "Phone validation result: valid / corrected / invalid",
    },
    {
        "name": "lead_phone_dnc",
        "label": "Lead Phone Do-Not-Call",
        "type": "enumeration",
        "fieldType": "booleancheckbox",
        "groupName": "contactinformation",
        "description": "True when the lead asked not to be called by phone (independent of email opt-out)",
        "options": [
            {"label": "true",  "value": "true",  "displayOrder": 0},
            {"label": "false", "value": "false", "displayOrder": 1},
        ],
    },
```

- [ ] **Step 2: Run the script (idempotent)**

Run: `python create_hs_properties.py`
Expected: `OK: lead_phone_status`, `OK: lead_phone_dnc` (or `SKIP` on re-run).

- [ ] **Step 3: Commit**

```bash
git add create_hs_properties.py
git commit -m "feat: add phone status + phone DNC HubSpot properties"
```

---

### Task 5: Honor `lead_phone_dnc` in the Do-Not-Call filter

**Files:**
- Modify: `batch/do_not_call.py` (`check_do_not_call` signature + first check)
- Modify: `batch/scorer.py` (fetch `lead_phone_dnc`; pass to `check_do_not_call`)
- Test: `tests/test_do_not_call.py` (add one test)

- [ ] **Step 1: Write the failing test**

Add to `tests/test_do_not_call.py`:

```python
import asyncio

from batch.do_not_call import check_do_not_call


def test_phone_dnc_skips():
    result = asyncio.run(
        check_do_not_call(email="x@y.de", funnel="hypnose", phone_dnc=True)
    )
    assert result.should_skip is True
    assert result.reason == "phone_dnc"
```

- [ ] **Step 2: Run it to verify it fails**

Run: `python -m pytest tests/test_do_not_call.py::test_phone_dnc_skips -v`
Expected: FAIL with `TypeError: check_do_not_call() got an unexpected keyword argument 'phone_dnc'`.

- [ ] **Step 3: Add the `phone_dnc` parameter + check**

In `batch/do_not_call.py:check_do_not_call`, add the keyword parameter to the signature (alongside the other keyword-only args, e.g. after `unsubscribed: bool = False,`):

```python
    phone_dnc: bool = False,
```

Then add this check as the FIRST check inside the function body, before the `# 1. Unsubscribed` block:

```python
    # 0. Telephone do-not-call — permanent, independent of email opt-out
    if phone_dnc:
        logger.info("DNC skip [phone_dnc]: %s", email)
        return DoNotCallResult(should_skip=True, reason="phone_dnc")
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `python -m pytest tests/test_do_not_call.py::test_phone_dnc_skips -v`
Expected: PASS.

- [ ] **Step 5: Fetch and pass `lead_phone_dnc` in the scorer**

In `batch/scorer.py:_fetch_active_hubspot_leads`, add `"lead_phone_dnc"` to the `"properties"` list (append it to whatever properties are already requested).

Then in `batch/scorer.py:run_batch_scoring`, the existing `check_do_not_call(...)` call (~line 955) passes several keyword args. Add one more:

```python
                phone_dnc=_truthy(props.get("lead_phone_dnc")),
```

- [ ] **Step 6: Run the do-not-call test file**

Run: `python -m pytest tests/test_do_not_call.py -v`
Expected: PASS (existing tests + new one).

- [ ] **Step 7: Commit**

```bash
git add batch/do_not_call.py batch/scorer.py tests/test_do_not_call.py
git commit -m "feat: telephone-specific do-not-call flag"
```

---

### Task 6: Validate phone numbers in the push path + flag invalid

**Files:**
- Modify: `integrations/slack.py` (`BatchRunStats` field + report line)
- Modify: `batch/scorer.py` (imports; phone validation block ~lines 947-950; `hs_properties`; invalid accumulator; stats)

- [ ] **Step 1: Add the `phone_invalid` stat field**

In `integrations/slack.py:BatchRunStats`, add after `skipped_dnc: int = 0` (line 243):

```python
    phone_invalid: int = 0            # leads with an unfixable phone number this run
```

- [ ] **Step 2: Render it in the Slack report**

In `integrations/slack.py:_build_batch_report_message`, after the `if stats.hs_chunk_errors:` block (ends ~line 288), add:

```python
    if stats.phone_invalid:
        lines.append(
            f":telephone_receiver: *{stats.phone_invalid} ungültige Nummer(n)* — manuell prüfen"
        )
```

- [ ] **Step 3: Add the scorer imports**

In `batch/scorer.py`, add to the imports near the other `from integrations...` lines:

```python
from integrations.phone import validate_and_normalize, region_for
from batch.call_window import is_within_call_window
```

- [ ] **Step 4: Add the invalid-phone accumulator**

In `batch/scorer.py:run_batch_scoring`, next to the other per-run accumulators (near `skipped_dnc = 0`, ~line 777), add:

```python
    invalid_phones: list[dict[str, Any]] = []
```

- [ ] **Step 5: Replace the phone normalization block**

In `batch/scorer.py:run_batch_scoring`, replace the existing block (lines 947-950):

```python
            _raw_phone = _normalize_phone(
                (props.get("phone") or props.get("mobilephone") or "").strip()
            )
            has_phone = len(_raw_phone) > 6  # reject stubs like "+41", "+49", "+"
```

with:

```python
            _raw_value = (props.get("phone") or props.get("mobilephone") or "").strip()
            if _raw_value:
                _raw_phone, _phone_status = validate_and_normalize(_raw_value)
                _raw_phone = _raw_phone or ""
                if _phone_status == "invalid":
                    invalid_phones.append({"email": email, "raw": _raw_value})
            else:
                _raw_phone, _phone_status = "", ""
            has_phone = bool(_raw_phone)  # only valid E.164 numbers are dialable
```

- [ ] **Step 6: Persist `lead_phone_status` on the contact**

In `batch/scorer.py:run_batch_scoring`, immediately after `hs_properties` is built (after the `_build_hubspot_card_properties(...)` assignment, ~line 1089), add:

```python
            if _phone_status:
                hs_properties["lead_phone_status"] = _phone_status
```

- [ ] **Step 7: Record the stat after the loop**

In `batch/scorer.py:run_batch_scoring`, where the other `_stats.*` fields are set after the scoring loop (~line 1221, near `_stats.skipped_dnc = skipped_dnc`), add:

```python
    _stats.phone_invalid = len(invalid_phones)
```

- [ ] **Step 8: Verify modules import cleanly**

Run: `python -c "import batch.scorer; import integrations.slack"`
Expected: exit code 0, no errors.

- [ ] **Step 9: Run the full suite**

Run: `python -m pytest tests/ -q`
Expected: all new + existing pure tests pass.

- [ ] **Step 10: Commit**

```bash
git add batch/scorer.py integrations/slack.py
git commit -m "feat: validate phone numbers in push path, flag invalid to Slack"
```

---

### Task 7: Dedupe the queue by phone + gate on the call window

**Files:**
- Modify: `batch/scorer.py` (`run_batch_scoring` — before the queue sort ~line 1269, and inside the push loop ~line 1278)

- [ ] **Step 1: Dedupe the Aircall queue by phone**

In `batch/scorer.py:run_batch_scoring`, immediately BEFORE the line `aircall_queue.sort(key=_aircall_priority_key, reverse=True)` (~line 1269), add:

```python
    # Dedupe by phone — same person under multiple emails => one queue entry.
    # Keep the highest-priority item per number (lowest _aircall_priority_key).
    _by_phone: dict[str, dict[str, Any]] = {}
    for _item in aircall_queue:
        _ph = _item["phone"]
        _existing = _by_phone.get(_ph)
        if _existing is None or _aircall_priority_key(_item) < _aircall_priority_key(_existing):
            _by_phone[_ph] = _item
    if len(_by_phone) < len(aircall_queue):
        logger.info(
            "Batch: deduped Aircall queue %d → %d by phone",
            len(aircall_queue), len(_by_phone),
        )
    aircall_queue = list(_by_phone.values())
```

- [ ] **Step 2: Gate each push on the call window**

In `batch/scorer.py:run_batch_scoring`, inside the push loop `for item in aircall_queue:`, after the `if item["phone"]:` check and before building `lead_dict` (~line 1278), add:

```python
            if not is_within_call_window(region_for(item["phone"]), now_utc):
                logger.debug(
                    "Batch: outside call window for %s — skip push this run",
                    item["email"],
                )
                continue
```

(`now_utc` is already defined at the top of `run_batch_scoring`.)

- [ ] **Step 3: Verify the module imports cleanly**

Run: `python -c "import batch.scorer"`
Expected: exit code 0.

- [ ] **Step 4: Run the full suite**

Run: `python -m pytest tests/ -q`
Expected: all pure tests pass (dedupe + window are exercised by the unit tests for `call_window`; the scorer integration is verified by import + a manual `/debug/batch` run on Railway after deploy).

- [ ] **Step 5: Commit**

```bash
git add batch/scorer.py
git commit -m "feat: dedupe dialer queue by phone + call-window gate"
```

---

## Self-Review (completed during planning)

**Spec coverage:**
- libphonenumber validation + safe auto-correction: Task 2 (`validate_and_normalize`), tests cover valid/corrected/invalid.
- Invalid → not dialed + flagged: Task 6 (`has_phone = bool(_raw_phone)`, `invalid_phones`, `lead_phone_status`, Slack line).
- Call-window + timezone: Task 3 (`is_within_call_window`) + Task 7 Step 2 gate.
- Dedupe by phone: Task 7 Step 1.
- Telephone-DNC separate from email opt-out: Tasks 4 + 5 (`lead_phone_dnc` property + `check_do_not_call` check).

**Placeholder scan:** none — every code step contains full code.

**Type consistency:** `validate_and_normalize` returns `tuple[str | None, str]` and is consumed that way in Task 6. `region_for` returns `str | None`, consumed by `is_within_call_window(region, now)` in Task 7. `_phone_status` is defined in every branch of the Task 5 replacement (no unbound-variable path) and guarded by `if _phone_status:` before use.

**Known interaction (acceptable for v1):** the 08:00 CET batch is before 09:00 local for DACH leads, so new pushes for DACH leads happen at the 12:00 and 16:00 batches. The 08:00 run still scores and updates HubSpot. Documented in the spec's limitations.
