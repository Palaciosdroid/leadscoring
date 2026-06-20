# Konzept: Lead-Scoring Funnel-Optimierung (Gabriel Palacios / SBC)

Datum: 2026-06-20
Status: Zerlegung approved; **revidiert nach adversarial Red-Team** (Zahlen selbst gegen Whyros gegengeprüft). Konzept-Spec zur Review. Erster Workstream nach Walkthrough.
Datenquelle: Whyros 1.0 (Supabase `kugjoikxhdsueddbbeyu`, READ-ONLY — niemals schreiben).

## Zweck

Den Lead-Scoring → Aircall-Dialer → Closing-Funnel datenbasiert optimieren. Empirischer
Audit + Zerlegung in unabhängige Workstreams mit Reihenfolge und Erfolgsmetriken. Jeder
Workstream bekommt danach sein eigenes Spec → Plan.

## Leitthese (revidiert nach Red-Team)

Foundation-first ist RICHTIG — und stärker als zunächst gedacht, weil **beide naheliegenden
Labels korrupt sind** (`total_revenue` UND `total_purchases`), 75% der „Käufer" nur *pending*
sind, die Conversion eine **kollabierende Kohorten-Artefakt** ist und der bestehende
`lead_score` zu 100% Null ist (greenfield). **Reichweite ist aber überbewertet** als #2:
der Phone-Gap ist real, aber Phone ist großteils *Outcome*, nicht Ursache, und der
adressierbare warme Pool ist klein. Höherer ROI als #2: ein **dead-simple deterministischer
Score**, den die Daten heute schon hergeben — ein gefittetes ML-Modell über nur ~105–124
Closes würde overfitten und diese Heuristik unterbieten.

**Revidierte Reihenfolge:** Fundament (Label + Kohorten fixen) → deterministischer Score →
gezielte Reichweite (die ~3.600 warmen Phone-Inhaber) → Feedback-Loop → gefittetes Modell
ERST später (wenn `completed`-Label-Volumen wächst). Lecks parallel.

## Daten-Baseline (alle Zahlen 2× verifiziert: Haupt-Analyse + unabhängiger Red-Team, ✅Fakt / 🔶Hypothese)

Zeitraum 2025-12-30 bis 2026-06-20 (~6 Mte). 52.828 contacts, 1.636.520 touchpoints,
1.396.190 events, 1.107 purchases, 639 meetings.

### Conversion — das Label ist die Kernfrage
- ✅ `total_purchases>0` liefert „936 Käufer" — **unbrauchbar**: davon nur `completed`
  **216 Kontakte / 596k CHF**; `pending` 726 / 850k (unbestätigt); `refunded` 38 / 134k.
  „Käufer" ≈ „Checkout gestartet", nicht „bezahlt".
- ✅ Echte bezahlte Signale: `purchases.payment_status='completed'` (**216 Kontakte**) +
  `meetings.outcome='closed_won'` (**124 Deals / 105 Kontakte**, ⌀ 4.716 CHF, 547k CHF).
- ✅ **Conversion kollabiert + March-Dump:** completed-Rate je Kohorte Jan 2,62% → Feb
  1,54% → **März 0,30% (25.317 Kontakte = 48% der DB)** → Apr 0,15% → Mai 0,04% → Jun 0,05%.
  Die gepoolte „1,77%" mischt eine reife Mini-Kohorte mit einem riesigen kalten März-Import.
  → NIE gepoolt arbeiten; per Kohorte + reifezeit-bereinigt.
- ✅ März-Import = **Customer.io-Subscriber-Sync** (13.461 `source=customerio` + 11.856 null,
  beide ~82–91% mit cio_id), nur **6,3% Phone**, nur **25% mit Website-Events** aber 99% mit
  (Email-)Touchpoints → echte, aber niedrig-intente Email-Subscriber, kein kalter Junk.
  Bestätigt: per Kohorte + per Engagement arbeiten; für den Dialer nur das engagierte Sub-Set (~6k).

### Reichweite (Phone)
- ✅ Phone gesamt 8,4% (4.440). Phone-Inhaber kaufen zu **19%**, Phonelose zu **0,19%** (100×).
- ✅ ABER Phone ist großteils *Outcome*: Käufer ⌀142 Events vs. Nicht-Käufer ⌀24; Phone wird
  beim Booking/Checkout erfasst. „Phone-Coverage breit erhöhen" ist schwächer als es aussieht.
- ✅ Adressierbarer **warmer** Pool: ~**3.596 Phone-Inhaber-Nicht-Käufer** (94% mit form_submit,
  ⌀24 Events, nur 182 mit Meeting) + ~**962 phonelose-aber-engagierte** (form+video_complete).
  Das ist der Hebel — nicht die 48k kalten Phonelosen.

### Scoring-Signal (Kalibrierung)
- ✅ `form_submit`-Lift (9×) ist **großteils tautologisch** (form_submit = Optin = wird erst
  Kontakt). → form_submit als **Gate** (anonym vs. identifiziert), NICHT als Gewicht.
- ✅ Echtes separierendes Signal *unter* den Identifizierten (Basis 4,67%): `video_complete`
  7,72% (~1,8×), **≥2 Optins 10,21%**, Preis-Seite-Besuch ~33%, Phone-Präsenz. Diese tragen.
- ✅ `contacts.lead_score` = 100% Null (greenfield — es wird heute faktisch nichts gescort).
- ✅ **Replay/Webinar-Aufzeichnung geschaut** → 3,49% echte Käufer vs. 0,45% ohne (~7,8×, 1.231
  Kontakte) — prädiktiv. Quelle = **Whyros** (on-site Video). **CIO trackt das NICHT** (nur Events
  `form_submit` + `purchase_completed`; CIO-Attribute = Funnel-Optin-Daten, UTM/Ad-Attribution,
  Tags, `unsubscribed`, Retargeting-Flags). CIOs einziger Proxy = Replay-Link-Klick (Email-Engagement).
- 🔶 High-Intent-URL-Counts (checkout/price/salespage) sind **taxonomie-abhängig** und
  reproduzierten zwischen den Analysen NICHT sauber (echte Pfade nutzen z.B. `/offer#pricetable`).
  URL-Taxonomie an echte Pfade pinnen, bevor darauf gewichtet wird (W1).

### Umsatz, Produkt, Daten-Integrität
- ✅ `contacts.total_revenue` korrupt (Summe 6,37M; corr mit echten Käufen 0,45; 290/918
  Kontakte ≥3× aufgebläht; Top 348k vs. 3,7k). **UND `total_purchases` korrupt** (corr 0,34;
  Top-Kontakt 45 vs. 3 Zeilen; 9 „Käufer" mit 0 Kauf-Zeilen). → beide als Label/Umsatz unbrauchbar.
- ✅ Produkt-Mix (completed-Sicht relevant): `hc` (Hypnosecoach) Hauptgeld; `mc` (Meditation)
  Massenware ⌀363; `gc` ⌀2.458.
- ✅ `al` „92% Refund" = **Daten-Artefakt** (21/22 am 2026-05-27, refunded binnen ~0,4 Tagen =
  Test-Batch/Bulk-Reversal) — NICHT als Produktkrise werten; ausschließen.
- ✅ EUR→`amount_chf` korrekt (per-Row-Ratio 0,945); vermeintliche „0,44" war Aggregat-Artefakt
  einzelner Zeilen — kein systemischer Bug.
- ✅ 35 Käufe ohne `product_key`.

### Kanäle
- ✅ email dominant (49.679 Kontakte), meta_ads 6.859, google_ads nur 159, tiktok 1.

## Workstreams (revidierte Priorität)

### W1 — Fundament: Label + Daten-Trust + Mapping  *(zuerst, non-negotiable)*
- Problem: beide Labels korrupt; Conversion = Kohorten-Artefakt; URL-Taxonomie unklar;
  EUR-Konversion fraglich; product_key-Lücken; März-Dump-Provenienz unklar.
- Änderung: **kanonisches Conversion-Label** festschreiben = `completed`-Kauf ODER `closed_won`;
  Umsatz = `amount_chf(completed)` + `meetings.revenue_generated(closed_won)`; Kohorten-/
  Reifezeit-Sicht als Standard; Whyros→Scorer-Event-Mapping + URL-Taxonomie an echte Pfade
  pinnen; product_key-Backfill; EUR verifizieren; März-Dump-Quelle klären (exkludieren wenn kalt).
- Erfolg: eine verifizierte Wahrheit für Label+Umsatz; Mapping-Coverage messbar.

### W2 — Deterministischer Score (Quick-Win, datengetrieben ohne ML)  *(braucht W1)*
- Problem: heute scort nichts (lead_score=0); Hand-Gewichte zielen auf „Engagement".
- Änderung: einfache, erklärbare Regel. **Score = reines Verhalten** (video_complete ODER
  **Replay/Webinar-Aufzeichnung geschaut** [3,49%] ODER ≥2 Optins ODER Preis-Seite); **Phone =
  separater Dialer-Gate**, NICHT als Score-Signal (Phone ist Outcome → sonst Leakage). Tiers an
  echte Close-Rate kalibrieren. Replay-Signal-Quelle = Whyros, nicht CIO.
- ✅ Validiert gegen echtes Label (completed∪closed_won): identifiziert+qualifiziert **3,29%**
  (7.816 Kontakte / 257 echte Käufer) vs. identifiziert-aber-unqualifiziert **0,00%** (8.527 / 0)
  vs. anonym 0,00%. Die Regel fängt praktisch ALLE echten Käufer und schließt die 8.527
  Null-Käufer-Identifizierten sauber aus.
- Erfolg: Kevins Queue priorisiert nachweislich höher-konvertierende Segmente; robust, kein Overfit.

### W3 — Gezielte Reichweite: die ~3.600 warmen Phone-Inhaber  *(braucht W1)*
- Problem: ~3.596 Phone-Inhaber-Nicht-Käufer (warm, kontaktierbar) sind nicht systematisch
  bearbeitet; ~962 warme Phonelose unerreichbar.
- Änderung: warme Phone-Inhaber priorisiert in den Dialer (via W2-Score); Phone-am-Optin als
  **A/B-Test** (nicht blind — Friction-Risiko) für die phonelosen Warmen.
- Erfolg: Anteil bearbeiteter warmer Phone-Inhaber ↑; mehr Calls auf echte Warm-Leads.

### W4 — Feedback-Loop: Tier → Close messen  *(braucht W1+W2)*
- Problem: `lead_tier_at_first_call`-Snapshot existiert, wird nie ausgewertet.
- Änderung: wöchentlich Tier-bei-Erstcall → Outcome (closed_won) → Predictiveness/Drift messen.
- Erfolg: laufende Messung „sagt der Score den Close voraus?"; Basis fürs spätere Modell.

### W5 — Gefittetes Modell  *(SPÄTER; braucht mehr completed-Labels)*
- Heute nur ~105–124 Closes / 216 completed → ML würde overfitten. Erst aktivieren, wenn
  W4-Daten + Label-Volumen tragen. Bis dahin schlägt die Heuristik (W2) jedes Modell.

### W6 — CIO→HubSpot Enrichment-Sync (Cron, Email-Match)  *(Enrichment, mittel-Prio, parallel)*
- Idee (Sandro): Cron, der HubSpot-Kontakte per Email-Match mit CIO abgleicht + fehlende Daten
  auffüllt, damit HubSpot immer alle CIO-Daten hat.
- Was CIO liefern kann (verifiziert): Funnel-Optin-Daten pro Funnel (`hc/mc/gc_launch_optin_date`),
  Offer/Launch-States, UTM/Ad-Attribution (`fbclid`, `hsa_*`), `unsubscribed`, Tags,
  Retargeting-Audience-Flags. (KEIN „Aufzeichnung geschaut" — das ist Whyros.)
- Änderung: Cron CIO App-API (`/customers/{email}/attributes?id_type=email`) → HubSpot batch-update,
  nur LEERE HubSpot-Felder füllen (nie blind überschreiben).
- Adversarial-Caution: VOR Bau verifizieren, welche Felder HubSpot wirklich fehlen (vs. Whyros-
  Duplikat — der Scorer liest UTM/Channel schon aus Whyros); Email-Match-Zuverlässigkeit;
  Volumen/Rate-Limits (52k Kontakte, CIO-Pagination); bewegt den Nordstern nur indirekt (Kontext/
  Segmentierung), kein Kern-Hebel.
- Erfolg: HubSpot-Kontakte vollständiger für Segmentierung + Kevins Kontext; messbar via
  Feld-Coverage vorher/nachher.

### Parallel — Lecks  *(diagnostisch, leicht)*
- 97 No-Shows (15%) → Reminder (ggf. MC-Setter-Territorium); product_key-Backfill.
- `al`-Refund NICHT als Krise behandeln (Artefakt, s.o.).

## Reihenfolge & Abhängigkeiten

```
W1 Fundament ──► W2 Deterministischer Score ──► W4 Feedback ──► W5 Modell (später)
             └─► W3 Gezielte Reichweite
Lecks: parallel/diagnostisch
```

## Erfolgsmetriken
- Nordstern: **closed_won-Umsatz pro Kevin-Stunde** (Baseline 547k / 124 Closes).
- Sekundär: Anteil bearbeiteter warmer Phone-Inhaber; Tier→Close-Korrelation; Mapping-Coverage;
  completed-Conversion je Kohorte (reifezeit-bereinigt).

## Adversarial Review (durchgeführt, eingearbeitet)
Unabhängiger Red-Team-Agent hat alle 7 Kernzahlen gegen Whyros nachgezogen (6/7 exakt
bestätigt; URL-Signale nicht reproduzierbar → 🔶). Überlebende Kritiken, alle hier integriert:
1. „1,77%" = Kohorten-Mix-Artefakt, kollabierend (selbst verifiziert). → per Kohorte arbeiten.
2. „Käufer" 75% pending; nur 216 completed (selbst verifiziert). → Label = completed/closed_won.
3. `total_purchases` ebenfalls korrupt (nicht nur `total_revenue`). → beide verworfen.
4. `form_submit`-Lift großteils tautologisch. → Gate statt Gewicht.
5. `al`-Refund = Einzeltag-Artefakt. → ausgeschlossen.
6. Reichweite überbewertet (Phone = Outcome); echter Hebel = ~3.600 warme Phone-Inhaber +
   deterministischer Score schlägt ML bei dieser Close-Zahl. → Priorität W2/W3 getauscht, W5 vertagt.

## Offene 🔶 (in den Workstreams zu klären)
- URL-Taxonomie an echte Pfade pinnen (W1) · HubSpot-Tier-Verteilung vs. echte Conversion
  (W4 — braucht HubSpot-API) · Phone-am-Optin Friction-Impact (W3, A/B-Test).
- Geklärt: März-Dump (CIO-Subscriber-Import, niedrig-intent) · EUR-Konversion (korrekt) ·
  Deterministik-Regel (gegen echtes Label validiert, 3,29% vs 0,00%).
