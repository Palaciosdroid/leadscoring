# Multi-Signal Funnel Attribution — Design

**Date:** 2026-05-20
**Scope:** Determine first-touch and last-touch funnel per setting-call lead by combining all available attribute signals across HubSpot, Customer.io, and Supabase.
**Status:** Approved (sections 1–5)

---

## 1. Problem

For the 269 distinct contacts who booked a setting call between 2026-01-01 and today, earlier analyses produced incomplete or inconsistent funnel attribution:

- HubSpot scheduling page identifies the funnel for only ~16% of bookings (most use generic calendar tools).
- Supabase `customerio_id` sync covers only 24% — actual CIO presence is 69%.
- Single-source views miss leads that engage with multiple funnels (90 of 188 CIO leads touched ≥2 funnels).
- Several known signals were never queried: HubSpot contact-level `hs_analytics_*`, Supabase `events.page_url`, CIO `*_optin_date` values.

We need one report that reconciles every available signal into a defensible per-lead first-touch and last-touch funnel verdict with explicit confidence.

## 2. Goal

Produce a markdown report at `analytics/funnel_full_report.md` that, for each of the 269 setting-call leads, assigns:

- a **first-touch funnel** (entry funnel)
- a **last-touch funnel** (the funnel closest to the booked call)
- a **confidence level** for each (HIGH / MEDIUM / NONE / CONFLICT)
- the list of sources that contributed

Plus aggregates (per-month, per-funnel, first→last transition matrix) and an appendix of conflicting leads.

Non-goals: writing back to HubSpot or Supabase, modifying the live scoring pipeline, building a real-time dashboard.

## 3. Architecture

Four-stage pipeline. Each stage isolated and testable on its own.

```
Stage 1: Roster                  Stage 2: Signal extraction
─────────────                    ─────────────────────────
Supabase SQL  ─────────────────► 5 extractors run in parallel
269 lead rows                    Each returns per-lead:
{email, contact_id,                {first_funnel, first_date,
 visitor_id,                        last_funnel,  last_date,
 hubspot_deal_id,                   raw_evidence}
 earliest_call_at}

                                 Stage 3: Resolution
                                 ──────────────────
                                 Cross-check 5 votes per lead
                                 per touch-type → assign
                                 funnel + confidence + flags

                                 Stage 4: Report
                                 ───────────────
                                 Markdown: coverage, lead table,
                                 aggregates, transition matrix,
                                 conflict appendix
```

**Modules:**

| File | Purpose |
|------|---------|
| `analytics/funnel_signals.py` | 5 signal extractors, each independent. Pure functions: `(lead) → SignalVote` |
| `analytics/funnel_resolve.py` | Resolution + confidence logic. Pure function over the 5 votes |
| `analytics/funnel_full_report.py` | Orchestrator: roster → run extractors → resolve → write report |

Reuses existing patterns from `analytics/funnel_attribution.py` (HubSpot batch reads) and `analytics/cio_funnel.py` (CIO lookup, rate limiting).

## 4. Funnel Taxonomy

Canonical funnels:

| Code | Label |
|------|-------|
| `HC` | Hypnose |
| `MC` | Meditation |
| `GC` | Gesprächscoach |
| `BF` | Bewusstseinsformel |
| `LM` | E-Book / Lead Magnet (generic entry) |
| `UNK` | unbekannt |

Every signal-to-funnel mapping must produce exactly one code from this list. New funnels are added here first, then the mapping rules.

## 5. Signal Extractors

Each extractor produces `{lead_id: SignalVote(first_funnel, first_date, last_funnel, last_date, evidence)}`. `None` for any field means "no signal." Build order = pre-build verification step: each extractor's mapping rules are confirmed against a sample of real data before the full run.

### 5.1 Touchpoints (Supabase)

- Query: `touchpoints` rows where `contact_id` is in the roster, ordered by `created_at`.
- First-touch: row with `is_first_touch = true` (fallback: earliest row).
- Last-touch: latest row with `created_at < earliest_call_at`.
- Funnel mapping: `(channel, source, campaign)` → funnel via keyword rules. Keywords include `hc_`/`hypnose`, `mc_`/`meditation`, `gc_`/`gespräch`/`gespraech`/`life ?coach`, `bf_`/`bewusstseinsformel`, `e-?book`/`ebook`.

### 5.2 Events.page_url (Supabase)

- Query: `events` rows where `visitor_id` is in the roster, ordered by `created_at`. Limit per lead to the first 50 and the last 50 events with `created_at < earliest_call_at` to keep payload bounded.
- Funnel mapping: regex/substring on `page_url`. Pre-built rules:
  - `gb_e=gc-` or `/gespr` or `/life-coach` → GC
  - `gb_e=hc-` or `/hypnose` or `hypno` → HC
  - `gb_e=mc-` or `/meditation` → MC
  - `bewusstseinsformel` or `/bf-` → BF
  - `/ebook`, `/pdf`, `kostenloses-pdf` → LM
- These rules are validated against actual URL samples in the verification step before the full run.

### 5.3 customer_journeys (Supabase)

- First-touch only. Query: `customer_journeys` by email.
- Funnel mapping: `first_utm_campaign` keyword match + `first_landing_page` URL match (same rules as 5.2).
- Does not contribute a last-touch vote.

### 5.4 CIO attributes

- Query: CIO `customers?email=` to get `cio_id`, then `customers/{cio_id}/attributes?id_type=cio_id`.
- Identify `*_optin_date` attributes; map prefix to funnel: `hc_*` → HC, `mc_*` → MC, `gc_*` → GC, `bf_*` → BF, `ebook_*` → LM.
- Parse the date value of each. **Earliest** date among funnel-prefixed dates = first-touch funnel. **Latest** date ≤ `earliest_call_at` = last-touch funnel.
- Skipped silently for the 81 leads not in CIO.

### 5.5 HubSpot contact analytics

- Query: HubSpot contact search by email, properties: `hs_analytics_source`, `hs_analytics_source_data_1`, `hs_analytics_source_data_2`, `hs_analytics_first_url`, `hs_analytics_last_url`, `recent_conversion_event_name`, `first_conversion_event_name`.
- First-touch: `hs_analytics_first_url` parsed with the same URL rules as 5.2; fallback to `first_conversion_event_name` keyword match.
- Last-touch: `hs_analytics_last_url` parsed; fallback to `recent_conversion_event_name`.
- `hs_analytics_source = OFFLINE` plus `hs_analytics_source_data_1 = INTEGRATION` → no signal (we already know that pattern is uninformative).

## 6. Resolution Logic

For each lead, run resolution independently for first-touch and last-touch.

```
Inputs:  five votes, each either a funnel code or None
Output:  resolved_funnel, confidence, sources_used, conflict_flag
```

| Pattern over the 5 votes | confidence | resolved_funnel |
|--------------------------|-----------|-----------------|
| ≥2 non-None votes agree, no disagreement | **HIGH** 🟢 | the agreed funnel |
| 2+ non-None votes, but they disagree | **CONFLICT** 🔴 | pick by priority order (below); log all votes |
| exactly 1 non-None vote | **MEDIUM** 🟡 | that funnel |
| 0 non-None votes | **NONE** ⚪ | `UNK` |

**Priority order** (most reliable first, used only as conflict tiebreaker):

1. `events.page_url` — URL is the funnel
2. `customer_journeys.first_landing_page` / `first_utm_campaign` — URL- and UTM-based
3. CIO `*_optin_date` — explicit funnel attribute
4. `touchpoints.campaign` — campaign name keyword match
5. HubSpot `hs_analytics_*` — known to be sparse / "OFFLINE" for most contacts

URL-based signals rank first because the page IS the funnel — no inference needed. HubSpot analytics ranks last because empirically it returns "OFFLINE/INTEGRATION" for the majority of these contacts.

## 7. Report Structure

`analytics/funnel_full_report.md` contains:

1. **Coverage summary** — count of leads by confidence level, for first-touch and last-touch separately.
2. **Per-lead table** — one row per lead: `email · first_funnel · 🟢🟡🔴⚪ · last_funnel · 🟢🟡🔴⚪ · sources`. Sortable by confidence + funnel.
3. **First-touch funnel distribution** — totals and per-month breakdown.
4. **Last-touch funnel distribution** — totals and per-month breakdown.
5. **First → Last transition matrix** — N×N grid showing how leads moved between funnels (e.g., entered HC, booked via MC).
6. **Conflict appendix** — one entry per CONFLICT lead, showing all 5 votes and which one was picked.

Raw signals are also serialized to `analytics/funnel_signals.json` so the resolution can be re-run with different priority rules without re-querying the APIs.

## 8. Edge Cases

| Case | Behavior |
|------|----------|
| Lead has multiple setting calls | Use earliest call's `scheduled_at` as the last-touch cutoff. One row per lead in the report. |
| 81 leads not in CIO | Extractor 5.4 yields no vote — the other four extractors still contribute. |
| 119 leads with no touchpoints | Extractor 5.1 yields no vote — 5.2 (events) and 5.5 (HubSpot) often still fire. |
| Lead has no `visitor_id` | Extractor 5.2 skipped for this lead, logged once. |
| Events / touchpoints after `earliest_call_at` | Filtered out when computing last-touch — no temporal leakage. |
| CIO rate limits (429) | Exponential backoff retry, same pattern as `cio_funnel.py`. |
| HubSpot batch read partial failure | Failed deal IDs logged; lead contributes no HubSpot vote rather than crashing the run. |
| Unmappable URL or campaign string | No vote from that extractor; the raw string is preserved in `evidence` for manual review. |

## 9. Verification (pre-build)

Before the full run, each URL/keyword rule set is sanity-checked against a 20-row sample from the live data:

- 5.2: sample 20 distinct `page_url` values from events of these leads → confirm regex coverage and look for unmapped patterns.
- 5.3: sample 20 `first_landing_page` values → same.
- 5.4: pull all attribute key prefixes seen across the 188 CIO leads → confirm taxonomy covers them.
- 5.5: sample 20 `hs_analytics_first_url` values → same.

Findings from these checks feed back into the funnel taxonomy (section 4) and the URL rules (section 5) before the full report is generated.

## 10. Out of Scope

- No writes to HubSpot, Supabase, or CIO.
- No change to the live `batch/scorer.py` scoring pipeline.
- No real-time / dashboard component — this is a one-time analysis report.
- No HubSpot Calls engagement properties for the EC Pipeline (separate analysis if needed).
