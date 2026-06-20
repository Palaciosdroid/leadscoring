# W1 Fundament — Design (Lead-Scoring-Optimierung)

Datum: 2026-06-20
Status: Spec zur Review. Teil des Master-Konzepts `2026-06-20-leadscoring-complete-architecture.md`.
Quellen: HubSpot API (`hubspot_palacios`), Whyros (Supabase `kugjoikxhdsueddbbeyu`, RO),
Tracking-Crew-Kanon [[Tracking-Funnel-URLs-Overview]] (GA4-verifizierte Money-Pfad-URLs).

## Zweck

Das Fundament für alle weiteren Scoring-Arbeiten: **eine verifizierte Wahrheit** für Label +
Umsatz, **korrektes Event-/URL-Mapping** (echte Funnel-Pfade statt Raten) und eine **re-runnbare,
kohorten-bewusste Baseline**. KEINE Änderung an Tier-Schwellen / Dialer-Push (das ist W2).

## Design-Entscheide (zur Bestätigung)

1. **Kanonisches Conversion-Label = HubSpot „Deal Won" (Vertrieb-Pipeline, 747).** Sekundär:
   `purchases.payment_status='completed'` (Whyros, Entry-Level). NIE `total_purchases`/`total_revenue`.
2. **Umsatz-Wahrheit:** HubSpot Deal-Amount (Sales) + **Bexio** (via Tracking-Crew, nicht neu anbinden).
3. **URL-Taxonomie = Tracking-Crew-Kanon** (echte Pfade): `/payment`, `inner-journey-payment`,
   `bookinea.app` → checkout; `/offer`, `/masterclass`, `/grundausbildung/`, `kosten-termine` →
   sales/offer; `kosten-termine` → price; `quiz…/eignungscheck`, `/onsite/eignungscheck/` →
   Eignungscheck; `/optin`, `/optin-thx` → optin. Replay/Webinar (`basisseminar`, `masterclass`,
   `day-1..4`, `live-workshop`) → video-watch-Signal.
4. **W1 ändert NICHT die Live-Tier-Logik** — nur Mapping-Korrektheit (Input). Coverage vorher/nachher messen.
5. Whyros + HubSpot-Label-Fetch = **read-only**.

## Komponenten

### A · Kanonische Definitionen + Label-Fetch — `analytics/labels.py` (neu)
- `WON_DEAL_PIPELINE_ID`, `WON_DEAL_STAGE_ID` (Vertrieb / „Deal Won") als Konstanten.
- `async fetch_won_contacts() -> set[str]`: HubSpot Deals (pipeline=Vertrieb, dealstage=Won) →
  assoziierte Contact-IDs → set. (Batch über `/crm/v3/objects/deals/search` + associations.)
- `async fetch_completed_purchase_emails() -> set[str]`: Whyros `purchases.payment_status='completed'`
  → contact-emails (für Entry-Level-Label).
- `is_converted(contact_id, email, won_set, completed_set) -> bool`: pure, kombiniert beide.
- Docstring = die kanonische Daten-Definition (eine Quelle).

### B · Kohorten-Baseline — `analytics/baseline.py` (neu)
- Re-runnbar (CLI: `python -m analytics.baseline`). Liest Whyros (Verhalten/Kohorte) + ruft
  `fetch_won_contacts`. Report (Print + optional Slack):
  - Conversion (kanonisches Label) **je Anmelde-Kohorte** (reifezeit-bewusst, nicht gepoolt).
  - Signal-Lift: form_submit / video_complete / replay-watch / price-page / Eignungscheck → Conversion.
  - **Mapping-Coverage:** % Events korrekt klassifiziert (vs. generisch `page_visited`).
- Zweck: löst „nie wieder gepoolt 1,77%"; Basis für W2-Kalibrierung + W4-Messung.

### C · URL-/Event-Mapping-Fix — `main.py` + `scoring/touchpoint_mapper.py`
- `CHECKOUT_URL_PATTERNS`, `SALES_PAGE_PATTERNS`, `PRICE_INFO_PATTERNS` in `main.py` auf den
  Tracking-Kanon umstellen (echte Pfade, s.o.). Neu: `REPLAY_URL_PATTERNS` (masterclass/basisseminar/
  live-workshop/day-N) → `video_watched_*` als Replay-Intent.
- `_map_cio_event` + `map_browser_events_batch` entsprechend.
- Tests: Tabelle echter URLs → erwartetes `event_type` (verhindert Drift).
- ⚠️ Nur Klassifikation; KEINE Tier-Schwellen-Änderung. Coverage-Delta dokumentieren.

### D · product_key-Handling
- `_extract_purchased_funnels` / Mapping: NULL/unknown `product_key` robust (kein Crash, geloggt);
  35 Nulls werden als „unklassifiziert" geführt, nicht geraten.

### E · Daten-Dictionary — `docs/DATA_DICTIONARY.md` (neu)
- Tabelle: Begriff → SSOT-System → Feld → Definition (Conversion, Umsatz, Kohorte, Signale, Kanäle).
- Verweist auf Tracking-Crew-Docs für die Tracking-Seite (keine Dopplung).

## Error Handling
- HubSpot/Whyros-Fetch: Retry + Fail-soft (Baseline meldet Teil-Daten, crasht nicht).
- Mapping: unbekannte URL → `page_visited` (Default), geloggt für Taxonomie-Pflege.

## Testing
- `analytics/labels.py`: `is_converted` pure → Unit-Tests (won/completed/none).
- Mapping: Fall-Tabelle echter Funnel-URLs → erwartete event_types (pytest).
- Baseline: smoke-test (läuft ohne Crash gegen Test-Fixtures).

## Erfolgskriterien
- Eine `labels.py`-Definition, von Baseline (+ später W2/W4) genutzt.
- Baseline zeigt Conversion **je Kohorte** + Signal-Lift gegen das **echte** Label (Deal Won).
- Mapping-Coverage messbar ↑ (checkout/offer/price-Events steigen, weil echte Pfade matchen).
- Keine Änderung an Live-Tiers/Push (verifiziert: Tier-Verteilung vor/nach unverändert).

## Abhängigkeiten / Koordination
- URL-Taxonomie aus [[Tracking-Funnel-URLs-Overview]] (Tracking-Crew-Kanon) — bei neuen Funnel-URLs dort nachsehen.
- Umsatz-Wahrheit (Bexio) + Conversion-Sender = Tracking-Crew (W8/W9 = konsumieren, nicht bauen).

## Offen 🔶 (beim Review zu bestätigen)
- Label: Deal Won primär — completed-purchase als sekundäres Entry-Level-Label mitführen? (Default: ja, getrennt)
- Won-Stage-ID der Vertrieb-Pipeline beim Bau aus HubSpot ziehen (nicht hardcoden raten).
