# Lead-Scoring — Bug Fixes + High-ROI Optimizations

**Date:** 2026-05-20
**Status:** Approved scope (verbal)
**Scope:** Fix 6 confirmed bugs in the scoring engine, expand funnel taxonomy, and close the funnel-attribution gap via `events.page_url` and the CIO-ID sync — in one cohesive code change.

---

## 1. Problem

Diagnostic review of `scoring/`, `batch/scorer.py`, `integrations/` and recent commit history surfaced six concrete bugs plus three high-ROI improvements that share the same files and risk surface. Shipping them as one cohesive change avoids overlapping rework.

Out of scope (separate spec cycles):

- AI/ML model training (O3) — new infrastructure
- Tier-threshold recalibration (O4) — depends on this fix + live conversion data
- `batch/scorer.py` module split (O5) — large architectural refactor
- Score versioning + A/B hooks (O8)
- Multi-Signal Funnel Attribution (existing spec from earlier today)

## 2. Goal

After this change the scoring engine produces correctly-scaled scores, classifies leads from the BF / E-Book / Eignungscheck funnels, attributes a funnel to ~191 leads currently marked `unknown`, and brings the 124 CIO-ID-out-of-sync leads back into the engagement signal.

Success criteria:

- Engagement score range: 0–100 (down from 0–200).
- All bug paths covered by new unit tests; existing 145 tests still green.
- Funnel-coverage for the 269 setting-call leads ≥ 85% (up from current ~70%).
- No regression in the per-tier lead counts beyond the expected B1 recalibration.

## 3. Changes

### Bugs

| ID | File | Change |
|----|------|--------|
| **B1** | `scoring/engagement.py` | Clamp `score` to `0–100` (was `0–200`). Keep `MAX_EVENTS_PER_TYPE = 5` — re-verify it's still reasonable against a slice of real leads after the change. |
| **B2** | `scoring/engagement.py` | Move `type_counts[event_type] += 1` to AFTER successful timestamp parse, so malformed events don't consume cap slots. |
| **B3** | `scoring/touchpoint_mapper.py` | Drop the `email`/NULL-type fallback rule entirely (currently counts 407 noise touchpoints as `email_opened`). Affected leads lose ≤25pts each — measured against the slice in Section 6. |
| **B4** | `scoring/interest.py` | Replace substring `if "hc" in product_lower` with token match (split on non-word chars, compare to `{"hc", "hypnose", ...}`). |
| **B5** | `scoring/interest.py` | Add funnels `bewusstseinsformel`, `ebook`, `eignungscheck` to `CATEGORY_SIGNALS` + `_PURCHASE_CATEGORY_MAP`. Update `VALID_FUNNELS` in `batch/scorer.py` accordingly. |
| **B6** | `scoring/engagement.py` | Replace step-function `recency_multiplier` with smooth exponential: `max(0.1, exp(-days/14))`. Behaviour at day 0 ≈ 1.0, day 7 ≈ 0.6, day 30 ≈ 0.12. |

### Optimizations

| ID | File | Change |
|----|------|--------|
| **O1** | `scoring/interest.py` | Parse `event.page_url` in `detect_interest_category` using regex patterns per funnel (`/hypnose`, `/meditation`, `/bewusstseinsformel`, `gb_e=gc-`, etc.). Verify patterns against real URL sample first. |
| **O2** | `batch/scorer.py` (new helper) | After CIO lookup by email, write the resolved `cio_id` back to Supabase `contacts.customerio_id` when it was NULL. Closes the 124-lead sync gap permanently. |
| **O6** | (covered by B5) | Funnel taxonomy expansion. |

## 4. Architecture

No new modules. All changes live in existing files. Behaviour-level changes only — no API surface changes for callers.

```
scoring/engagement.py        ← B1, B2, B6
scoring/interest.py          ← B4, B5, O1
scoring/touchpoint_mapper.py ← B3
batch/scorer.py              ← B5 (VALID_FUNNELS), O2 (CIO write-back)
tests/                       ← new tests for each bug + smoke test for O1
```

`batch/scorer.py` stays a monolith for this change — the split is a separate refactor.

## 5. Risk + Mitigation

| Risk | Mitigation |
|------|-----------|
| B1 lowers all current Hot/Warm scores → leads flip Cold | Hold tier thresholds constant for now; flag any tier-flip in the batch report. Recalibration (O4) is a separate spec. |
| B3 drops scoring for 407 touchpoints → some Cold leads get even colder | Expected — these were noise. Verify aggregate score delta after the change. |
| B5 introduces new funnel codes that downstream code doesn't recognise | Update `VALID_FUNNELS`, list mappings, and Aircall tags atomically. Run E2E test before deploy. |
| O1 URL patterns mis-classify | Pre-build verification step: pull 30 distinct `page_url` values from current events and confirm coverage. |
| O2 writes to Supabase contacts | Use UPDATE WHERE customerio_id IS NULL — never overwrite existing IDs. Dry-run mode first. |

## 6. Test Strategy

- **Unit:** one new test per bug fix (B1–B6) verifying the corrected behaviour and a regression-guard for the old behaviour.
- **Integration:** existing batch dry-run with a representative slice (~50 leads from each tier); compare scoring delta before/after each commit.
- **E2E:** Aircall queue contents + HubSpot batch update payload diff before/after, surfaced in the existing post-batch Slack health report.
- **All 145 existing tests must stay green.**

## 7. Rollout

1. Land bugs B1, B2, B6 first (pure scoring math, lowest risk).
2. Land B3 (touchpoint mapping) — measure impact on score distribution.
3. Land B4, B5 + O6 (funnel taxonomy) + O1 (page_url parsing) — coupled change.
4. Land O2 (CIO-ID write-back) — independent, can be deployed last.

Each step gets its own commit + Railway deploy + 1-batch verification before the next.
