# W1 Fundament — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development / executing-plans. Checkbox steps. TDD, frequent commits. Branch: `palacios-sync` (local, kein Deploy).

**Goal:** Trusted foundation — canonical label (HubSpot Deal Won), real-funnel URL mapping, cohort-aware baseline. NO change to live tier thresholds / dialer push.

**Spec:** `docs/superpowers/specs/2026-06-20-w1-fundament-design.md`

**Verified facts (use as-is, don't re-derive):**
- HubSpot Vertrieb pipeline `id=168455110`, Won stage `id=311698367` (747 wins). Token = env `HUBSPOT_ACCESS_TOKEN`.
- Whyros (Supabase `kugjoikxhdsueddbbeyu`, RO): `purchases.payment_status='completed'` = paid; `events.event_type ∈ pageview/scroll/video_play/video_progress/video_complete/form_submit/click`; `events.page_url`.
- Canonical funnel URLs (Tracking-Crew, GA4-verified): payment=`/payment`,`inner-journey-payment`,`bookinea.app`; offer/sales=`/offer`,`/masterclass`,`/grundausbildung/`,`kosten-termine`; price=`kosten-termine`; eignungscheck=`/eignungscheck`,`/onsite/eignungscheck/`; optin=`/optin`,`/optin-thx`; replay/webinar=`basisseminar`,`masterclass`,`live-workshop`,`day-1..4`.

---

### Task 1: Canonical label module — `analytics/labels.py` + tests

- [ ] Test `tests/test_labels.py`: `is_converted(cid, email, won_set, completed_set)` → True if cid in won_set OR email in completed_set; False else; handles empty sets.
- [ ] Run test → fail.
- [ ] Implement `analytics/labels.py`:
  - Constants `WON_DEAL_PIPELINE_ID="168455110"`, `WON_DEAL_STAGE_ID="311698367"`.
  - `is_converted(cid, email, won_set, completed_set) -> bool` (pure).
  - `async fetch_won_contacts() -> set[str]`: search HubSpot deals `pipeline=168455110 AND dealstage=311698367`, paginate, for each deal GET `/crm/v3/objects/deals/{id}/associations/contacts` → collect contact ids. Reuse `HUBSPOT_ACCESS_TOKEN`.
  - `async fetch_completed_purchase_emails() -> set[str]`: Whyros via existing `integrations/supabase.py` client — distinct lower(email) where `purchases.payment_status='completed'`.
  - Module docstring = canonical definition (the data dictionary source).
- [ ] Run test → pass. Commit `feat: canonical conversion label module (HubSpot Deal Won)`.

### Task 2: URL/event-mapping fix — `main.py` (+ `scoring/touchpoint_mapper.py`) + tests

- [ ] Test `tests/test_url_mapping.py`: table of real funnel URLs → expected `_map_cio_event` result (e.g. `…/payment`→checkout_visited, `…/offer`→sales_page_visited, `…/kosten-termine/`→price_info_viewed, `…/eignungscheck`→application-relevant, `…/masterclass`→video/replay context).
- [ ] Run → fail.
- [ ] Update in `main.py`: `CHECKOUT_URL_PATTERNS`, `SALES_PAGE_PATTERNS`, `PRICE_INFO_PATTERNS` to the canonical funnel paths; add `REPLAY_URL_PATTERNS`. Mirror in `scoring/touchpoint_mapper.py` browser mapping. **Do NOT touch tier thresholds / BASE_POINTS values.**
- [ ] Run → pass. Run full suite. Commit `fix: pin URL taxonomy to real funnel paths (mapping coverage)`.

### Task 3: Cohort-aware baseline — `analytics/baseline.py`

- [ ] Implement re-runnable `analytics/baseline.py` (`python -m analytics.baseline`): reads Whyros (cohort by `contacts.created_at` month, behavior signals) + `labels.fetch_won_contacts`/`fetch_completed_purchase_emails`; prints: conversion (canonical label) per cohort, signal lift (form_submit/video_complete/replay/price/eignungscheck), mapping-coverage % (events classified vs generic page_visited). Fail-soft on partial data.
- [ ] Smoke-test `tests/test_baseline_smoke.py` (runs against small fixtures without crash).
- [ ] Commit `feat: cohort-aware baseline analysis (canonical label)`.

### Task 4: product_key robustness + DATA_DICTIONARY

- [ ] Ensure NULL/unknown `product_key` handled without crash (log as 'unclassified'); add a test if a gap exists.
- [ ] Write `docs/DATA_DICTIONARY.md`: term → SSOT system → field → definition (conversion=HubSpot Deal Won; revenue=HubSpot amount+Bexio; cohort; signals; channels). Reference Tracking-Crew docs for tracking side.
- [ ] Commit `docs: data dictionary + product_key null handling`.

### Task 5: Verify no live-behavior change

- [ ] Confirm tier-relevant code (`combined.py` TIERS, `scorer.py` thresholds, BASE_POINTS) UNCHANGED (git diff review).
- [ ] Run full pytest suite → all green.
- [ ] Mapping-coverage delta documented in the baseline output (before/after the Task-2 fix).

## Self-Review (during planning)
- Spec coverage: label (T1), mapping (T2), baseline (T3), product_key+dict (T4), no-behavior-change guard (T5). ✓
- Real IDs/URLs baked in (no guessing). ✓
- Read-only on Whyros + HubSpot; no deploy; branch palacios-sync. ✓
