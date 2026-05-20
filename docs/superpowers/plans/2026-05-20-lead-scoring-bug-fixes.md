# Lead-Scoring Bug Fixes + High-ROI Optimizations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix 6 confirmed bugs in the scoring engine, expand the funnel taxonomy, and close the funnel-attribution gap via `events.page_url` and a CIO-ID sync write-back.

**Architecture:** Behaviour-only changes inside the existing modules — no new files, no API changes. Four sequential phases, each independently testable and deployable.

**Tech Stack:** Python 3.14, pytest, httpx, Supabase PostgREST, HubSpot v3 API, Customer.io EU App API. Linked spec: `docs/superpowers/specs/2026-05-20-lead-scoring-bug-fixes-design.md`.

---

## File Structure

| File | Phase | Responsibility |
|------|-------|----------------|
| `scoring/engagement.py` | 1 | Engagement math: score clamp, counter ordering, recency curve |
| `scoring/touchpoint_mapper.py` | 2 | Drop noisy email-NULL fallback |
| `scoring/interest.py` | 3 | Token-match purchase keywords, new funnel categories, `page_url` parsing |
| `integrations/supabase.py` | 4 | New helpers: `_patch()` + `update_contact_customerio_id(contact_id, cio_id)` |
| `scripts/backfill_customerio_ids.py` | 4 | **New file** — one-shot CIO-ID backfill |
| `tests/test_engagement.py` | 1 | New tests B1, B2, B6 |
| `tests/test_touchpoint_mapper.py` | 2 | **New file** — covers B3 + existing mapper behaviour |
| `tests/test_interest.py` | 3 | New tests B4, B5, O1 |
| `tests/test_supabase_writeback.py` | 4 | **New file** — covers `update_contact_customerio_id` |

---

## Phase 1 — Engagement Math (B1, B2, B6)

All in `scoring/engagement.py`. Lowest risk first: pure math, fully unit-tested.

### Task 1.1: B6 — Replace step-function recency with exponential decay

**Files:**
- Modify: `scoring/engagement.py:64-76`
- Modify: `tests/test_engagement.py:13-32` (replace existing `TestRecencyMultiplier` class)

- [ ] **Step 1: Update the failing test**

Replace `TestRecencyMultiplier` in `tests/test_engagement.py` with:

```python
import math
import pytest


class TestRecencyMultiplier:
    def test_today(self):
        assert recency_multiplier(0) == pytest.approx(1.0, abs=0.001)

    def test_one_week(self):
        # exp(-7/14) ≈ 0.606
        assert recency_multiplier(7) == pytest.approx(0.606, abs=0.005)

    def test_two_weeks(self):
        # exp(-14/14) ≈ 0.368
        assert recency_multiplier(14) == pytest.approx(0.368, abs=0.005)

    def test_one_month(self):
        # exp(-30/14) ≈ 0.117
        assert recency_multiplier(30) == pytest.approx(0.117, abs=0.005)

    def test_floor_at_very_old(self):
        # Floor prevents negligible weights — assert >= 0.05 at 90 days
        assert recency_multiplier(90) >= 0.05

    def test_monotonically_decreasing(self):
        prev = recency_multiplier(0)
        for d in (1, 3, 7, 14, 30, 60, 90):
            cur = recency_multiplier(d)
            assert cur <= prev, f"non-monotonic at day {d}: {cur} > {prev}"
            prev = cur
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_engagement.py::TestRecencyMultiplier -v`
Expected: FAIL — current step-function returns 1.5 for day 0, not 1.0.

- [ ] **Step 3: Implement the exponential curve**

Replace lines 64-76 in `scoring/engagement.py` with:

```python
import math  # add at top of file if not already imported


# ---------------------------------------------------------------------------
# Recency multiplier — smooth exponential decay
# ---------------------------------------------------------------------------
# exp(-days/14) gives: day 0 = 1.0, day 7 ≈ 0.61, day 14 ≈ 0.37, day 30 ≈ 0.12.
# Floored at 0.05 to avoid negligible-weight scoring noise from very old events.
def recency_multiplier(days_ago: float) -> float:
    return max(0.05, math.exp(-days_ago / 14.0))
```

(Add `import math` to the imports block at the top of the file.)

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_engagement.py::TestRecencyMultiplier -v`
Expected: PASS (6 tests).

- [ ] **Step 5: Run the full engagement test file**

Run: `pytest tests/test_engagement.py -v`
Expected: PASS. If any pre-existing test relied on a hard-coded multiplier (e.g. 1.3), update its expected value to match the new curve. Show the failing test before changing it — no silent edits.

- [ ] **Step 6: Commit**

```bash
git add scoring/engagement.py tests/test_engagement.py
git commit -m "fix(scoring): B6 smooth exp recency decay (was step function)"
```

---

### Task 1.2: B2 — Move type counter increment after timestamp parse

**Files:**
- Modify: `scoring/engagement.py:153-168`
- Modify: `tests/test_engagement.py` (append new test class)

- [ ] **Step 1: Write the failing test**

Append to `tests/test_engagement.py`:

```python
class TestEventCounterOrdering:
    """B2: malformed events must not consume MAX_EVENTS_PER_TYPE slots."""

    def test_malformed_timestamps_do_not_consume_cap(self):
        # 6 events, type=email_opened, but the first one has a broken timestamp.
        # If the counter increments before parse, only 4 valid events get scored
        # (1 malformed + 4 valid hits the cap of 5).
        # After the fix, all 5 valid events should be scored.
        now = datetime.now(timezone.utc)
        events = [{"event_type": "email_opened", "timestamp": "not-a-date"}]
        for i in range(5):
            ts = (now - timedelta(hours=i + 1)).isoformat()
            events.append({"event_type": "email_opened", "timestamp": ts})

        result = calculate_engagement_score(events)

        # 5 valid email_opened events × 5 base points × ~1.0 multiplier (≤ 1 day)
        # should each be scored — confirm by counting breakdown entries.
        scored_count = sum(
            1 for b in result["event_breakdown"] if b["event_type"] == "email_opened"
        )
        assert scored_count == 5
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_engagement.py::TestEventCounterOrdering -v`
Expected: FAIL — current code increments counter before parse, so the malformed event consumes a slot and only 4 valid events get scored.

- [ ] **Step 3: Reorder the loop**

In `scoring/engagement.py` replace lines 153-168 (the for-loop opening) with:

```python
    for event in events:
        event_type = event.get("event_type", "")
        ts_str = event.get("timestamp", "")

        if event_type not in BASE_POINTS:
            continue

        # Parse timestamp BEFORE consuming a cap slot — malformed events
        # must not displace valid ones (B2 fix).
        try:
            ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            continue

        # Cap: max N events of same type to prevent score inflation
        type_counts[event_type] += 1
        if type_counts[event_type] > MAX_EVENTS_PER_TYPE:
            continue

        days_ago = (now - ts).total_seconds() / 86400
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_engagement.py::TestEventCounterOrdering -v`
Expected: PASS.

- [ ] **Step 5: Run the full engagement test file**

Run: `pytest tests/test_engagement.py -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add scoring/engagement.py tests/test_engagement.py
git commit -m "fix(scoring): B2 count slot only after timestamp parse"
```

---

### Task 1.3: B1 — Clamp engagement score to 0-100

**Files:**
- Modify: `scoring/engagement.py:205`
- Modify: `tests/test_engagement.py` (append new test class)

- [ ] **Step 1: Write the failing test**

Append to `tests/test_engagement.py`:

```python
class TestScoreClamp:
    """B1: engagement score must clamp to 0-100, not 0-200."""

    def test_high_score_clamped_at_100(self):
        # Build events that would otherwise produce > 100 raw points.
        # 5× checkout_visited (40pts each, ×1.0 recency) = 200 raw → clamp to 100.
        now = datetime.now(timezone.utc)
        events = [
            {"event_type": "checkout_visited",
             "timestamp": (now - timedelta(hours=i)).isoformat()}
            for i in range(5)
        ]
        result = calculate_engagement_score(events)
        assert result["score"] <= 100
        assert result["score"] == 100  # capped exactly at 100

    def test_negative_clamp_unchanged_at_minus_100(self):
        # Unsubscribed adds -50 malus, no positive events. Final must be 0.
        # (Bug fix raises the floor from -100 to 0 — negative scores are
        # meaningless downstream; tier mapping treats < 0 as disqualified anyway.)
        result = calculate_engagement_score([
            {"event_type": "email_unsubscribed", "timestamp": "2026-05-01T00:00:00Z"},
        ])
        assert result["score"] >= 0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_engagement.py::TestScoreClamp -v`
Expected: FAIL — current code clamps to 200, so 5× checkout = 200 stays at 200.

- [ ] **Step 3: Update the clamp**

In `scoring/engagement.py`, replace line 205:

```python
    # Clamp to 0-100 — tier thresholds (combined.py) expect 0-100 scale.
    # Negative raw scores (unsubscribed -50) floor at 0; disqualification is
    # determined separately via the `unsubscribed` flag on the result.
    score = max(min(round(raw_score), 100), 0)  # B1: was max(min(..., 200), -100)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_engagement.py::TestScoreClamp -v`
Expected: PASS.

- [ ] **Step 5: Run the entire test suite — catch any reliance on the old range**

Run: `pytest tests/ -v`
Expected: PASS. If any pre-existing test asserted a score > 100 (e.g. `test_optimizations.py`, `test_s1_calibration.py`), the assertion needs to be updated to the new clamp. Show each failing assertion before changing it.

- [ ] **Step 6: Commit**

```bash
git add scoring/engagement.py tests/test_engagement.py
git commit -m "fix(scoring): B1 clamp engagement score to 0-100 (was 0-200)"
```

---

### Task 1.4: Phase 1 deploy + verify

- [ ] **Step 1: Push to Railway**

```bash
git push origin palacios-sync
```

Watch Railway logs for green deploy.

- [ ] **Step 2: Trigger one batch run + capture report**

Wait for the next scheduled batch (08/12/16 CET) OR call the `/debug/batch` endpoint manually. Read the Slack `#batch-health` message it posts.

- [ ] **Step 3: Compare tier distribution**

Note in the deployment PR (or `_HOT_MEMORY.md`): the pre-fix and post-fix counts of Hot / Warm / Cold leads. Expected drift: some leads previously scoring 100-200 collapse to 100 — no tier change. The exp recency (Task 1.1) shifts mid-range scores slightly. If > 5% of Hot leads drop to Warm, stop and reconsider Task 1.3 — may need to recalibrate thresholds in a separate spec (O4) first.

---

## Phase 2 — Touchpoint Mapping (B3)

### Task 2.1: B3 — Drop the email/NULL-type fallback rule

**Files:**
- Modify: `scoring/touchpoint_mapper.py:34`
- Create: `tests/test_touchpoint_mapper.py` (new file — current tests live in the legacy `lead-scoring/tests/` path)

- [ ] **Step 1: Create the new test file**

Create `tests/test_touchpoint_mapper.py`:

```python
"""Tests for scoring/touchpoint_mapper.py — Supabase touchpoint mapping."""

from scoring.touchpoint_mapper import map_touchpoint_to_event


class TestEmailNullTypeFallback:
    """B3: email touchpoints with NULL touchpoint_type must NOT be scored.

    407 touchpoints in production carry channel=email, touchpoint_type=NULL.
    Previously these counted as `email_opened` (5pts) which inflated scores
    for inactive recipients.
    """

    def test_email_null_type_is_dropped(self):
        tp = {"channel": "email", "source": "customerio", "touchpoint_type": None}
        assert map_touchpoint_to_event(tp) is None

    def test_email_opened_still_maps(self):
        tp = {"channel": "email", "source": "customerio", "touchpoint_type": "opened"}
        result = map_touchpoint_to_event(tp)
        assert result is not None
        assert result["event_type"] == "email_opened"

    def test_email_clicked_still_maps(self):
        tp = {"channel": "email", "source": "customerio", "touchpoint_type": "clicked"}
        result = map_touchpoint_to_event(tp)
        assert result is not None
        assert result["event_type"] == "email_link_clicked"

    def test_ad_click_still_maps(self):
        tp = {"channel": "meta_ads", "source": "Meta", "touchpoint_type": None}
        result = map_touchpoint_to_event(tp)
        assert result is not None
        assert result["event_type"] == "cta_clicked"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_touchpoint_mapper.py -v`
Expected: `test_email_null_type_is_dropped` FAILS — current rule maps it to `email_opened`. The other three should pass.

- [ ] **Step 3: Drop the fallback rule**

In `scoring/touchpoint_mapper.py` delete lines 31-34 (the rule + comment block):

```python
    # Catch-all: email touchpoints with NULL type → count as email_opened
    # 407 touchpoints in Supabase have channel=email but touchpoint_type=None.
    # Conservative: count as open (5pts) rather than ignoring entirely.
    ("email", "*", None, "email_opened"),
```

Replace with a single comment line:

```python
    # B3 (2026-05-20): email/NULL fallback removed — these are inactive
    # recipients, not opens. 407 historical touchpoints affected.
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_touchpoint_mapper.py -v`
Expected: PASS (4 tests).

- [ ] **Step 5: Run the full test suite**

Run: `pytest tests/ -v`
Expected: PASS.

- [ ] **Step 6: Commit + push + verify**

```bash
git add scoring/touchpoint_mapper.py tests/test_touchpoint_mapper.py
git commit -m "fix(scoring): B3 drop email/NULL touchpoint fallback (407 noise rows)"
git push origin palacios-sync
```

Watch the next batch report — affected leads lose ≤25pts each. Confirm aggregate Cold-count rises slightly (expected), no Hot/Warm leads disappear unexpectedly.

---

## Phase 3 — Funnel Detection (B4, B5, O1)

All in `scoring/interest.py`. The new funnel codes stay confined to `interest.py` — `batch/scorer.py`'s `VALID_FUNNELS` (which gates Aircall list assignment) is **deliberately not changed** here. New funnels become visible in the `lead_interest_category` HubSpot property without auto-creating calling lists. Routing to lists for BF / Ebook is a separate decision.

### Task 3.1: B4 — Token-match purchase keywords (replace substring)

**Files:**
- Modify: `scoring/interest.py:28-40` (the `_infer_from_purchased` function)
- Modify: `tests/test_interest.py` (append new test class)

- [ ] **Step 1: Write the failing test**

Append to `tests/test_interest.py`:

```python
from scoring.interest import _infer_from_purchased


class TestInferFromPurchased:
    """B4: keyword match must be token-based, not substring."""

    def test_hc_exact_match(self):
        assert _infer_from_purchased(["hc"]) == "hypnose"

    def test_hypnose_in_product_name(self):
        assert _infer_from_purchased(["Hypnose-Ausbildung"]) == "hypnose"

    def test_hc_not_in_unrelated_word(self):
        # "ch" inside "achtsamkeit" / "such" must NOT trigger "hc"->hypnose.
        # Token-match treats "hc" as a standalone word, not a substring.
        assert _infer_from_purchased(["Achtsamkeits-Coaching"]) != "hypnose"

    def test_mc_token_match(self):
        assert _infer_from_purchased(["MC"]) == "meditation"

    def test_no_match_returns_none(self):
        assert _infer_from_purchased(["random-product"]) is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_interest.py::TestInferFromPurchased -v`
Expected: `test_hc_not_in_unrelated_word` FAILS — current substring check would currently NOT match "Achtsamkeits-Coaching" against "hc" actually (no "hc" substring there), so check with a known false-positive case. Update the test if needed to pick a string where the bug manifests, e.g. `["check"]` contains "hc" → false-matches hypnose. Use that instead:

```python
    def test_hc_not_in_unrelated_word(self):
        # "check" contains the substring "hc" — must NOT trigger "hc"->hypnose.
        assert _infer_from_purchased(["checkout"]) != "hypnose"
        assert _infer_from_purchased(["check"]) != "hypnose"
```

Re-run; expected: FAIL on these.

- [ ] **Step 3: Rewrite `_infer_from_purchased` with token matching**

Replace lines 28-40 in `scoring/interest.py`:

```python
import re


def _infer_from_purchased(purchased_products: list[str]) -> str | None:
    """
    Infer interest category from purchased product keys.

    Token-based: the keyword must appear as a standalone word (split on
    non-word characters), not as a substring. Prevents "check" matching
    "hc" → "hypnose" (B4 fix).
    """
    for product in purchased_products:
        tokens = {t.lower() for t in re.split(r"[^a-zA-Z0-9]+", product) if t}
        for key, category in _PURCHASE_CATEGORY_MAP.items():
            if key.lower() in tokens:
                return category
    return None
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_interest.py::TestInferFromPurchased -v`
Expected: PASS (5 tests).

- [ ] **Step 5: Run the full interest test file**

Run: `pytest tests/test_interest.py -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add scoring/interest.py tests/test_interest.py
git commit -m "fix(scoring): B4 token-match purchase keywords (was substring)"
```

---

### Task 3.2: B5 — Expand funnel taxonomy

**Files:**
- Modify: `scoring/interest.py:16-25` (`_PURCHASE_CATEGORY_MAP`)
- Modify: `scoring/interest.py:45-72` (`CATEGORY_SIGNALS`)
- Modify: `tests/test_interest.py` (append new test class)

- [ ] **Step 1: Write the failing test**

Append to `tests/test_interest.py`:

```python
class TestExpandedFunnels:
    """B5: add bewusstseinsformel, ebook, eignungscheck as recognised funnels."""

    def test_bewusstseinsformel_from_url(self):
        events = [{"event_type": "page_visited",
                   "url": "https://sbc.de/bewusstseinsformel-optin"}]
        result = detect_interest_category(events)
        assert result["category"] == "bewusstseinsformel"

    def test_ebook_from_url(self):
        events = [{"event_type": "free_resource_downloaded",
                   "url": "https://sbc.de/kostenloses-ebook"}]
        result = detect_interest_category(events)
        assert result["category"] == "ebook"

    def test_eignungscheck_from_url(self):
        # Use a URL with ONLY the eignungscheck keyword — no other category
        # in the path, otherwise first-match-wins dict order returns
        # whichever category is declared first in CATEGORY_SIGNALS.
        events = [{"event_type": "application_submitted",
                   "url": "https://sbc.de/eignungscheck-anmeldung"}]
        result = detect_interest_category(events)
        assert result["category"] == "eignungscheck"

    def test_bf_purchase_inference(self):
        # Lead bought BF (Bewusstseinsformel) — should infer that funnel.
        result = detect_interest_category([], purchased_products=["bf"])
        assert result["category"] == "bewusstseinsformel"
        assert result["inferred_from_purchase"] is True
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_interest.py::TestExpandedFunnels -v`
Expected: all 4 FAIL — categories don't exist yet.

- [ ] **Step 3: Add new categories to `CATEGORY_SIGNALS`**

In `scoring/interest.py`, replace the `CATEGORY_SIGNALS` dict (lines 45-72) with:

```python
CATEGORY_SIGNALS: dict[str, list[str]] = {
    "hypnose": [
        "hypnose", "hypnosecoach", "hypnotherapy", "hypnosis", "trance", "nlp",
    ],
    "lifecoach": [
        "lifecoach", "life-coach", "life_coach",
        "coaching-ausbildung", "coaching_ausbildung",
        "persoenlichkeit", "persoenlichkeitsentwicklung", "lebenscoach",
    ],
    "meditation": [
        "meditation", "meditationscoach", "achtsamkeit",
        "mindfulness", "breathwork", "yoga",
    ],
    "bewusstseinsformel": [
        "bewusstseinsformel", "bewusstseins-formel",
    ],
    "ebook": [
        "ebook", "e-book", "kostenloses-ebook", "kostenloses_ebook",
        "free-pdf", "kostenloses-pdf",
    ],
    "eignungscheck": [
        "eignungscheck", "eignungs-check", "eignungs_check", "eignungs-test",
    ],
}
```

- [ ] **Step 4: Add new categories to `_PURCHASE_CATEGORY_MAP`**

Replace lines 16-25 with:

```python
_PURCHASE_CATEGORY_MAP: dict[str, str] = {
    "hc":         "hypnose",
    "hypnose":    "hypnose",
    "mc":         "meditation",
    "meditation": "meditation",
    "gc":         "lifecoach",
    "lifecoach":  "lifecoach",
    "life-coach": "lifecoach",
    "life_coach": "lifecoach",
    "bf":         "bewusstseinsformel",
    "ebook":      "ebook",
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `pytest tests/test_interest.py::TestExpandedFunnels -v`
Expected: PASS (4 tests).

- [ ] **Step 6: Run the full interest test file + scorer/list-key tests**

Run: `pytest tests/ -v`
Expected: PASS. The new funnel codes will appear in `lead_interest_category` for new batches; downstream `_determine_list_key` (in `batch/scorer.py`) returns `None` for unknown funnels which is the correct no-Aircall-list behaviour. No code change needed in scorer.py.

- [ ] **Step 7: Commit**

```bash
git add scoring/interest.py tests/test_interest.py
git commit -m "feat(scoring): B5 add bewusstseinsformel/ebook/eignungscheck funnels"
```

---

### Task 3.3: O1 — Verify page_url patterns against live data

**Files:**
- Read-only query against Supabase.

- [ ] **Step 1: Pull 30 distinct page_url samples from the 269 setting-call leads**

Use the Supabase SQL editor or MCP:

```sql
WITH setting_leads AS (
  SELECT DISTINCT m.contact_id, c.visitor_id
  FROM meetings m
  JOIN contacts c ON c.id = m.contact_id
  WHERE m.scheduled_at >= '2026-01-01'
    AND m.meeting_type = 'setting_call'
)
SELECT DISTINCT e.page_url
FROM events e
JOIN setting_leads sl ON sl.visitor_id = e.visitor_id
WHERE e.page_url IS NOT NULL
ORDER BY random()
LIMIT 30;
```

- [ ] **Step 2: Manually classify each URL by funnel**

For each URL, decide which funnel it represents (HC/MC/GC/BF/Ebook/Eignungscheck/none). Compare against the keyword lists added in Task 3.2.

- [ ] **Step 3: Update `CATEGORY_SIGNALS` if gaps found**

If any URLs reveal a missing keyword (e.g. `gb_e=mc-launch`), add it to the matching category in `CATEGORY_SIGNALS`. Otherwise no change.

- [ ] **Step 4: Commit any keyword updates**

```bash
git add scoring/interest.py
git commit -m "feat(scoring): O1-verify expand CATEGORY_SIGNALS for live URL patterns"
```

If no changes were needed, skip the commit and note in the plan tracker that verification passed.

---

### Task 3.4: O1 — Parse page_url in detect_interest_category

**Files:**
- Modify: `scoring/interest.py:100-166` (`detect_interest_category`)
- Modify: `tests/test_interest.py` (append new test class)

- [ ] **Step 1: Write the failing test**

Append to `tests/test_interest.py`:

```python
class TestPageUrlParsing:
    """O1: detect_interest_category must use event.page_url, not just event.url."""

    def test_page_url_classified(self):
        # Supabase events use `page_url`; CIO touchpoints use `url`.
        # Both must be honoured.
        events = [{"event_type": "page_visited",
                   "page_url": "https://sbc.de/hypnose-ausbildung"}]
        result = detect_interest_category(events)
        assert result["category"] == "hypnose"

    def test_url_field_still_works(self):
        events = [{"event_type": "page_visited",
                   "url": "https://sbc.de/meditation-kurs"}]
        result = detect_interest_category(events)
        assert result["category"] == "meditation"

    def test_both_fields_present(self):
        # If both `url` and `page_url` exist, count both (additive, not
        # exclusive — they're separate sources of the same signal).
        events = [
            {"event_type": "page_visited",
             "page_url": "https://sbc.de/hypnose-ausbildung"},
            {"event_type": "sales_page_visited",
             "url": "https://sbc.de/hypnose-ausbildung-offer"},
        ]
        result = detect_interest_category(events)
        assert result["category"] == "hypnose"
        # Two hypnose hits → confidence = 1.0
        assert result["confidence"] == 1.0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_interest.py::TestPageUrlParsing -v`
Expected: `test_page_url_classified` FAILS — current code only reads `event.url`, not `event.page_url`.

- [ ] **Step 3: Update `detect_interest_category` to read both fields**

In `scoring/interest.py`, inside the `detect_interest_category` function loop (currently line ~123-142), replace the URL extraction block with:

```python
    for event in events:
        event_type = event.get("event_type", "")
        # Read BOTH `url` (CIO touchpoints) and `page_url` (Supabase events).
        # O1 fix: page_url was previously ignored — closes funnel-attribution
        # gap for ~191 leads.
        urls = [u for u in (event.get("url"), event.get("page_url")) if u]
        metadata = event.get("metadata", {}) or {}

        weight = SIGNAL_WEIGHTS.get(event_type, 1)

        # Detect via URL(s)
        for url in urls:
            cat = _extract_category_from_url(url)
            if cat:
                category_scores[cat] += weight

        # Detect via metadata fields (e.g. video_title, resource_name)
        for field in ("video_title", "resource_name", "webinar_title", "page_title"):
            value = metadata.get(field, "")
            if value:
                cat = _extract_category_from_url(value)
                if cat:
                    category_scores[cat] += weight * 0.5  # metadata = half weight
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_interest.py::TestPageUrlParsing -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Run the full test suite**

Run: `pytest tests/ -v`
Expected: PASS.

- [ ] **Step 6: Commit + push + verify**

```bash
git add scoring/interest.py tests/test_interest.py
git commit -m "feat(scoring): O1 parse events.page_url for funnel attribution"
git push origin palacios-sync
```

After deploy, run a manual batch and check the HubSpot list-membership query:

```
HubSpot: filter contacts where lead_interest_category HAS_PROPERTY → expected > 144
```

Note the new count in the commit message of the next deploy or `_HOT_MEMORY.md`.

---

## Phase 4 — CIO-ID Write-Back (O2)

Closes the 124-lead sync gap: when the batch looks up a contact in CIO and finds them, but Supabase's `customerio_id` is NULL, write the resolved `cio_id` back.

### Task 4.1: Add `update_contact_customerio_id` to the Supabase client

**Files:**
- Modify: `integrations/supabase.py` (append new function near the other writers, around line 484)
- Create: `tests/test_supabase_writeback.py` (new file)

- [ ] **Step 1: Write the failing test**

Create `tests/test_supabase_writeback.py`:

```python
"""Tests for CIO-ID write-back in integrations/supabase.py (O2)."""

from unittest.mock import AsyncMock, patch

import pytest


@pytest.mark.asyncio
async def test_update_contact_customerio_id_calls_patch():
    """O2: helper must PATCH the contact row with the new cio_id."""
    from integrations.supabase import update_contact_customerio_id

    with patch("integrations.supabase.get_supabase_client") as get_client:
        client = AsyncMock()
        get_client.return_value = client
        client._patch = AsyncMock(return_value={"id": "uuid-1", "customerio_id": "cio_123"})

        result = await update_contact_customerio_id("uuid-1", "cio_123")

        client._patch.assert_called_once_with(
            "contacts",
            {"id": "eq.uuid-1", "customerio_id": "is.null"},
            {"customerio_id": "cio_123"},
        )
        assert result is True


@pytest.mark.asyncio
async def test_update_contact_customerio_id_handles_missing_client_method():
    """If the client lacks a _patch method (test stub), raise clearly."""
    from integrations.supabase import update_contact_customerio_id

    with patch("integrations.supabase.get_supabase_client") as get_client:
        client = AsyncMock(spec=[])  # no _patch attribute
        get_client.return_value = client
        with pytest.raises(AttributeError):
            await update_contact_customerio_id("uuid-1", "cio_123")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_supabase_writeback.py -v`
Expected: FAIL — `update_contact_customerio_id` does not exist.

- [ ] **Step 3: Add `_patch` helper to `SupabaseClient`**

In `integrations/supabase.py`, inside the `SupabaseClient` class (after `_post` around line 119), add:

```python
    async def _patch(
        self, table: str, filters: dict, data: dict
    ) -> dict | None:
        """Generic PATCH (update) against PostgREST.

        `filters` keys translate to PostgREST query params (e.g.
        {"id": "eq.uuid-1"}). `data` is the new column values.
        """
        params = "&".join(f"{k}={v}" for k, v in filters.items())
        url = f"{self._base}/{table}?{params}"
        response = await self._client.patch(url, json=data)
        if response.status_code not in (200, 204):
            logger.error(
                "Supabase PATCH %s failed: %s %s",
                table, response.status_code, response.text[:500],
            )
            response.raise_for_status()
        if response.status_code == 204 or not response.text:
            return None
        rows = response.json()
        return rows[0] if isinstance(rows, list) and rows else rows
```

- [ ] **Step 4: Add the public helper**

Below the singleton block (around line 150), add:

```python
async def update_contact_customerio_id(contact_id: str, cio_id: str) -> bool:
    """Write `cio_id` back to Supabase contacts WHERE customerio_id IS NULL.

    The `is.null` filter prevents overwriting existing IDs — only fills
    the sync gap. Returns True on success.

    Closes the O2 gap: 124 of 188 CIO-linked setting-call leads currently
    have customerio_id = NULL because the original sync missed them.
    """
    if not contact_id or not cio_id:
        return False
    client = get_supabase_client()
    await client._patch(
        "contacts",
        {"id": f"eq.{contact_id}", "customerio_id": "is.null"},
        {"customerio_id": cio_id},
    )
    return True
```

- [ ] **Step 5: Run test to verify it passes**

Run: `pytest tests/test_supabase_writeback.py -v`
Expected: PASS (2 tests).

- [ ] **Step 6: Commit**

```bash
git add integrations/supabase.py tests/test_supabase_writeback.py
git commit -m "feat(supabase): add update_contact_customerio_id for O2 write-back"
```

---

### Task 4.2: One-shot backfill script for missing CIO IDs

The current batch reads `contact.customerio_id` from Supabase but doesn't perform email-based CIO lookups itself, so the cleanest fix is a standalone backfill script rather than threading new behaviour through the hot batch loop.

**Files:**
- Create: `scripts/backfill_customerio_ids.py` (new)

- [ ] **Step 1: Create the backfill script**

Create `scripts/backfill_customerio_ids.py`:

```python
"""
One-shot backfill: resolve Supabase contacts whose customerio_id is NULL
by looking them up against the Customer.io App API by email.

Safe to re-run: the underlying update_contact_customerio_id helper filters
on `customerio_id IS NULL`, so existing IDs are never overwritten.

Usage:
    python scripts/backfill_customerio_ids.py [--dry-run] [--limit N]
"""

import argparse
import asyncio
import json
import os
import sys
import urllib.parse
import urllib.request
import urllib.error

# Allow running this script without installing the package
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from integrations.supabase import (
    get_supabase_client,
    update_contact_customerio_id,
)


def _cio_lookup(email: str, headers: dict) -> str | None:
    url = (
        "https://api-eu.customer.io/v1/customers?email="
        + urllib.parse.quote(email)
    )
    try:
        req = urllib.request.Request(url, headers=headers)
        data = json.loads(urllib.request.urlopen(req).read())
        results = data.get("results") or []
        return results[0].get("cio_id") if results else None
    except urllib.error.HTTPError as e:
        if e.code in (400, 404):
            return None
        raise


async def backfill(dry_run: bool, limit: int | None) -> None:
    # 1. Load CIO credentials
    with open(os.path.expanduser(r"~/.claude/secrets/SECRETS.json"),
              encoding="utf-8") as f:
        cio = json.load(f)["apiKeys"]["customerio"]
    headers = {"Authorization": f"Bearer {cio['app_api_key']}"}

    # 2. Fetch all contacts with NULL customerio_id and a non-null email
    client = get_supabase_client()
    params = {
        "select": "id,email",
        "customerio_id": "is.null",
        "email": "not.is.null",
    }
    if limit:
        params["limit"] = str(limit)
    rows = await client._get("contacts", params)

    print(f"Candidates: {len(rows)} contacts with NULL customerio_id")

    found = 0
    written = 0
    for i, row in enumerate(rows):
        cio_id = _cio_lookup(row["email"], headers)
        if cio_id:
            found += 1
            if dry_run:
                print(f"  [DRY] {row['email']} -> {cio_id}")
            else:
                ok = await update_contact_customerio_id(row["id"], cio_id)
                if ok:
                    written += 1
        if (i + 1) % 100 == 0:
            print(f"  ...{i + 1}/{len(rows)}  found={found}  written={written}")

    print(f"\nDone. Candidates={len(rows)}  matched in CIO={found}  "
          f"written={written}  dry_run={dry_run}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()
    asyncio.run(backfill(args.dry_run, args.limit))
```

- [ ] **Step 2: Smoke-test with --dry-run --limit 5**

Run: `python scripts/backfill_customerio_ids.py --dry-run --limit 5`
Expected output: lists up to 5 candidate emails and their CIO matches (or none) without writing to Supabase.

- [ ] **Step 3: Real run for 20 contacts as canary**

Run: `python scripts/backfill_customerio_ids.py --limit 20`
Expected: a few `written` updates (depends on how many of those 20 happen to be in CIO).

- [ ] **Step 4: Verify in Supabase**

```sql
SELECT COUNT(*) FILTER (WHERE customerio_id IS NOT NULL) AS synced,
       COUNT(*) FILTER (WHERE customerio_id IS NULL)     AS not_synced
FROM contacts;
```

Compare before/after — `synced` rose by the canary number, `not_synced` dropped by the same. If anything else moved, stop and investigate.

- [ ] **Step 5: Full backfill**

Run: `python scripts/backfill_customerio_ids.py` (no limit)
Expected: walks every NULL-customerio_id contact, looks each up in CIO, writes back matches. Watch the progress output; this will take roughly `candidates × 0.12s` minutes.

- [ ] **Step 6: Commit**

```bash
git add scripts/backfill_customerio_ids.py
git commit -m "feat(scripts): O2 one-shot backfill for customerio_id sync gap"
```

- [ ] **Step 7: Document the new total in `_HOT_MEMORY.md`**

Append a line capturing `synced` before vs after the full run, so future investigations don't waste time re-discovering the gap.

---

## Phase 5 — Final Verification

### Task 5.1: Full regression sweep

- [ ] **Step 1: Run all tests**

Run: `pytest tests/ -v --tb=short`
Expected: All tests pass — pre-existing 145 + new ones from this plan (≈ 15+ new tests across Phases 1–4).

- [ ] **Step 2: Compare lead-tier distribution before/after**

Query HubSpot before and after the full deployment chain:

```
filter: lead_tier HAS_PROPERTY
group by: lead_tier
```

Document the deltas in the commit message of the final commit or in `_HOT_MEMORY.md`. Expected:

| Tier   | Before | After  | Drift acceptable? |
|--------|--------|--------|-------------------|
| Hot    | 257    | ?      | -5% to +5%        |
| Warm   | 249    | ?      | -10% to +10%      |
| Cold   | 6362   | ?      | +0% to +5%        |

A larger drift than these bands is a signal that thresholds need separate recalibration (O4 spec).

- [ ] **Step 3: Confirm funnel-attribution coverage**

Query: count of contacts where `lead_interest_category HAS_PROPERTY`.

Before: ≈ 144 (from earlier diagnostic).
Target after Phase 3 deploy: ≥ 200 (≥ 85% of the 269 setting-call leads, plus broader coverage from O1 page_url parsing on the whole population).

- [ ] **Step 4: Update `_HOT_MEMORY.md` with the change summary**

Append a session entry capturing the 6 bug fixes + 2 optimisations + funnel taxonomy expansion, the deltas measured, and any follow-up items (e.g. if Phase 4 revealed CIO contacts that need additional work, log them).

---

## Out-of-Plan Follow-ups (for future specs)

These remain explicitly NOT in this plan and are listed only as a reminder:

- **O3** — Train Logistic-Regression AI model from the 24 buyers
- **O4** — Recalibrate Hot/Warm thresholds against actual conversion rates (requires Phase 1–4 stabilised first)
- **O5** — Split `batch/scorer.py` (1.512 lines → 5–6 modules)
- **O8** — Score versioning + A/B harness
- Multi-Signal Funnel Attribution one-time report (`docs/superpowers/specs/2026-05-20-funnel-attribution-multi-signal-design.md`)
