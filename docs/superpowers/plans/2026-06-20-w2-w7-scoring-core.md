# W2+W7 Scoring-Kern — Implementation Plan

> REQUIRED SUB-SKILL: superpowers:subagent-driven-development / executing-plans. TDD, frequent commits. Branch `palacios-sync` (local). Run pytest with dummy env prefix: `HUBSPOT_ACCESS_TOKEN=test SUPABASE_URL=https://test.supabase.co SUPABASE_SERVICE_KEY=test DEBUG_API_KEY=test python -m pytest <file> -q`. DO NOT change live tier behavior when `SCORING_MODE != points` (default engagement). No git push / deploy (orchestrator handles).

**Goal:** Transparent, data-calibrated point-system targeting HubSpot Deal Won, using Tally Eignungscheck signals. Flag-gated shadow-then-flip.

**Spec:** `docs/superpowers/specs/2026-06-20-w2-w7-scoring-core-design.md`

**Verified facts:** Label = HubSpot Deal Won (pipeline 168455110 / stage 311698367, in `analytics/labels.py`). Tally form `nPJzEe` (Hypnose-Eignungscheck) via `tally_palacios` (UA header required). Point weights = spec table (from measured close-rates).

---

### Task 1: Tally answer-mapping module — `integrations/tally.py` + tests
- [ ] Test `tests/test_tally.py`: `map_eignungscheck(responses, questions) -> dict` returns normalized `{budget, interest, consult, goal, eig_score}` from a fixture submission. Maps by QUESTION-TITLE keyword (robust to ID drift): budget←"budget", interest←"interesse", consult←"beraten", goal←"ziel". Normalize budget→enum (`unter_2000`/`2000_4000`/`4000_6000`/`6000_8000`), interest→enum (`keines`/`grundsaetzlich`/`naechster_schritt`), consult→bool.
- [ ] Run → fail.
- [ ] Implement `integrations/tally.py`: `TALLY_FORM_ID="nPJzEe"`; pure `map_eignungscheck()`; `async fetch_submissions()` (Tally API `/forms/{id}/submissions?filter=completed`, paginated, UA header, by-email). Read-only.
- [ ] Run → pass. Commit `feat: Tally Eignungscheck answer mapping`.

### Task 2: Point-system — `scoring/points.py` + tests
- [ ] Test `tests/test_points.py`: `compute_points(signals)` per spec table — budget 4000+ → +30, 2000-4000 → +15, unter_2000 → 0; interest naechster_schritt → +25, grundsaetzlich → +10, keines → tier `4_disqualified`; consult → +15; replay/video_complete → +20; checkout → +25; price → +15; form_submit → +10; interest_category hypnose → +10. Tier from thresholds (start: Hot≥50, Warm≥25, else Cold; disqualify overrides). `reasons` list explains each contribution.
- [ ] Run → fail.
- [ ] Implement `scoring/points.py`: weight constants (spec table), `compute_points(signals: dict) -> PointsResult(points, tier, reasons)`. Pure. Missing signal → 0 (no crash).
- [ ] Run → pass. Commit `feat: transparent point-system scorer (Deal Won target)`.

### Task 3: Calibration — `analytics/calibrate_points.py`
- [ ] Implement `analytics/calibrate_points.py` (`python -m analytics.calibrate_points`): assemble per-contact signals (Whyros behavior + HubSpot Tally props + interest), run `compute_points`, bucket by points → real Deal-Won rate (via `analytics/labels.py`). Output: points-bucket→close-rate table + Hot/Warm/Cold threshold recommendation + closes-concentration (% of closes in Hot/Warm). Fail-soft.
- [ ] Smoke-test `tests/test_calibrate_smoke.py` (fixtures).
- [ ] Commit `feat: point-system calibration vs Deal Won`.

### Task 4: HubSpot properties (config)
- [ ] Add to `create_hs_properties.py`: `lead_eig_budget` (enum), `lead_eig_interest` (enum), `lead_eig_consult` (booleancheckbox), `lead_eig_goal` (text), `lead_eig_score` (number), `lead_points` (number, shadow). (Do NOT run — orchestrator/Sandro runs with creds.)
- [ ] Commit `feat: add Tally + shadow-points HubSpot properties`.

### Task 5: Scorer integration (flag-gated shadow) + deploy TODOs
- [ ] In `batch/scorer.py`: read `SCORING_MODE` env (default `engagement`). When building each lead's HubSpot properties, ALWAYS compute `compute_points(signals)` and write `lead_points` (shadow). Only when `SCORING_MODE=='points'` use its tier for `lead_tier`/`lead_combined_score` + add `reasons` to the Aircall card. When `engagement` (default) → tier logic UNCHANGED (shadow only).
- [ ] Signals assembled from: W1-mapped behavior (form_submit/video_complete/replay/checkout/price), HubSpot Tally props (`lead_eig_*`), interest category.
- [ ] TODO-A: in the list-membership loop, SKIP `batch_add_to_list` for dynamic lists 365-370 (only static 352/362/363/364). 
- [ ] TODO-B: skip note re-write when the card content is unchanged (hash compare) to cut the ~7k-call note tail.
- [ ] Tests `tests/test_scoring_mode.py`: engagement-mode → tier unchanged + lead_points written; points-mode → tier from points.
- [ ] Run full suite. Commit `feat: flag-gated point-system in scorer (shadow) + list/note fixes`.

### Task 6: Verify (adversarial)
- [ ] git diff: confirm `SCORING_MODE!=points` leaves tier/threshold behavior UNCHANGED (shadow only). Phone is NOT a point signal. Weights match spec. Full suite green. Report PASS/FAIL + issues.

## Self-Review (planning)
- Spec coverage: Tally (T1+T4), points (T2), calibration (T3), integration+shadow+TODOs (T5), guard (T6). ✓
- Shadow-then-flip: T5 writes lead_points always, flips tier only on flag. ✓
- Real values baked (form nPJzEe, weights, label IDs). Creds-runs (props create, Tally backfill, calibration, flip) = mine, post-build.
