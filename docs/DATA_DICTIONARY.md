# Data Dictionary — SBC Lead-Scoring

One vocabulary for the scoring system. Each term maps to **one** SSOT system and
field. If a definition lives elsewhere as code, that code is the authority and is
linked here — this file does not redefine it.

- **Conversion / Revenue / Tracking truth** is owned by the Tracking-Crew. The
  scoring system *consumes* it, never rebuilds it.
- **Canonical label code:** [`analytics/labels.py`](../analytics/labels.py)
  (module docstring = source of truth for "converted?").
- **Funnel-URL taxonomy:** Tracking-Crew canon `[[Tracking-Funnel-URLs-Overview]]`
  (GA4-verified money-path URLs) — mirrored in code in `main.py`
  (`*_URL_PATTERNS`) and `scoring/touchpoint_mapper.py`. For the tracking side
  (pixel/CAPI/sGTM, attribution) see the Tracking-Crew docs; not duplicated here.

---

## Core terms

| Term | SSOT system | Field / source | Definition |
|------|-------------|----------------|------------|
| **Conversion (primary)** | HubSpot | Deal in Vertrieb pipeline `168455110`, stage `311698367` ("Deal Won", 747 wins @ 2026-06-20) | A contact is *converted* if any associated deal sits in the Won stage. Resolved by `analytics/labels.fetch_won_contacts()`. The canonical label for all baseline/calibration work. |
| **Conversion (secondary / entry-level)** | Whyros (Supabase `kugjoikxhdsueddbbeyu`, RO) | `purchases.payment_status = 'completed'`, matched by lowercased `contacts.email` | Completed entry-level purchase. Carried *separately* from the primary label (see `is_converted()` which ORs both). |
| **NOT a conversion label** | — | `contacts.total_purchases`, `contacts.total_revenue`, `contacts.lead_score` | Derived/mutable fields. Never use as ground truth for "converted?". |
| **Revenue (sales)** | HubSpot | Deal `amount` on Won deals | Deal value booked by Sales. |
| **Revenue (entry / e-commerce)** | Bexio (via Tracking-Crew) | Bexio invoice/payment total | Owned by Tracking-Crew — **do not re-integrate Bexio here.** Consume the Tracking-Crew feed. |
| **Cohort** | Whyros | `contacts.created_at`, truncated to month (YYYY-MM) | Signup month. Baseline reports conversion **per cohort** (maturity-aware), never pooled — avoids the "pooled 1.77%" artifact. |
| **Mapping coverage** | derived (Whyros `events`) | % of `events` classified to a specific `event_type` vs. generic `page_visited` | Quality metric for the URL taxonomy. Measured before/after the W1 mapping fix; rises when real funnel paths match. |

## Signals (behavioral, scored)

Event types live in `scoring/engagement.BASE_POINTS` (do not change values in W1).
Whyros source: `events.event_type` / `events.page_url` (classified via the
funnel-URL taxonomy in `main.py` + `scoring/touchpoint_mapper.py`).

| Signal | SSOT system | Field / derivation | Definition |
|--------|-------------|--------------------|------------|
| **form_submit / application** | Whyros | `events.event_type='form_submit'`; URL `…/eignungscheck`, `/onsite/eignungscheck/` → `application_submitted` | Eignungscheck / application — high-intent qualification step. |
| **video watched** | Whyros | `events.event_type ∈ video_play/video_progress/video_complete` → `video_watched_50/75/100` | Replay/webinar watch depth. `video_watched_100` = strongest commitment. |
| **replay / webinar** | Whyros | `page_url` ∈ `basisseminar`, `masterclass`, `live-workshop`, `day-1..4` → replay/video-watch context | Replay-intent (`REPLAY_URL_PATTERNS`). |
| **checkout visited** | Whyros | `page_url` ∈ `/payment`, `inner-journey-payment`, `bookinea.app` → `checkout_visited` | Reached payment/checkout — top of `BASE_POINTS`. |
| **sales/offer page** | Whyros | `page_url` ∈ `/offer`, `/masterclass`, `/grundausbildung/`, `kosten-termine` → `sales_page_visited` | Viewed an offer/sales page. |
| **price info** | Whyros | `page_url` ∈ `kosten-termine` → `price_info_viewed` | Saw pricing — buying-intent signal. |
| **optin** | Whyros | `page_url` ∈ `/optin`, `/optin-thx` | Lead capture. |
| **email engagement** | Whyros / CIO | touchpoints `channel='email'`, `touchpoint_type ∈ opened/clicked` → `email_opened` / `email_link_clicked` | Email opens/clicks. |

## Channels

| Term | SSOT system | Field | Definition |
|------|-------------|-------|------------|
| **Channel** | Whyros | `touchpoints.channel` (e.g. `email`, ad-click channels) | Acquisition/interaction channel for a touchpoint. Mapped in `scoring/touchpoint_mapper._MAPPING_RULES`. |
| **Source / Medium / Campaign** | Whyros | `touchpoints.source` / `medium` / `campaign`, `events.utm_*` | UTM attribution dimensions. Attribution logic = Tracking-Crew / `analytics/funnel_attribution.py`. |

## Products

| Term | SSOT system | Field | Definition |
|------|-------------|-------|------------|
| **product_key** | Whyros | `purchases.product_key` | Short product code (e.g. `hc`, `mc`, `gc`, `afk`, `tfmw`, `bf`, `ik`, `med`). Mapped to funnels in `batch.scorer._PRODUCT_KEY_TO_FUNNEL`. |
| **unclassified product_key** | Whyros | `purchases.product_key` IS NULL / unknown | ~35 purchases carry a NULL/empty `product_key`. Handled by `batch.scorer.classify_product_key()` → returned as `'unclassified'` and logged. **Never guessed** into a funnel — fix at the source, not in code. |
| **Ausbildung (full)** | derived | `product_key ∈ {hc, mc, gc}` (`_AUSBILDUNG_KEYS`) | Full training purchase → customer exclusion for that funnel. Entry/bundle products (afk, tfmw, bf, Inner Journey) are interest signals, **not** exclusions. |

---

*W1 scope: mapping/label/analysis only. Tier thresholds, `BASE_POINTS`, and
`combined.py` TIERS are unchanged. Conversion-sender + Bexio revenue truth remain
Tracking-Crew responsibilities (W8/W9 consume, do not build).*
