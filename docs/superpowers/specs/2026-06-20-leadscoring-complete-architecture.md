# Komplette Daten-Architektur + Lead-Scoring-Konzept (Gabriel Palacios / SBC)

Datum: 2026-06-20
Status: Architektur-Synthese nach HubSpot-Direktzug. Ersetzt die Whyros-zentrische Annahme des Vorgänger-Konzepts in mehreren Kernpunkten (s. „Korrekturen").
Quellen: Whyros (Supabase `kugjoikxhdsueddbbeyu`, RO), CIO App-API (EU), HubSpot API (`hubspot_palacios`).

## Kern-Korrekturen durch HubSpot-Direktzug (alle ✅ verifiziert)

1. **Label ist viel größer als gedacht:** echte Sales-Wins = **HubSpot Vertrieb-Pipeline „Deal Won" = 747** (nicht 124 Whyros-Meetings, nicht 216 completed-purchases). → **ein gefittetes Modell ist machbar** (mein früheres „zu wenig Labels → nur Deterministik" war auf Whyros gebaut, das Outcomes nur partiell spiegelt: 124 von 747).
2. **Phone ist KEIN Flaschenhals für gescorte Leads:** der Dialer liest HubSpot, nicht Whyros. HubSpot hat **20.233 Phones**; **Hot+Warm zu 91% mit Phone** (Hot 390/421, Warm 1.432/1.585) → anrufbarer Pool **1.822**. Die „8,4%" waren Whyros-Top-Funnel-Rauschen. → **W3 (Reichweite/Phone-Recovery) demontiert.**
3. **HubSpot = Sales-/Outcome-Wahrheit**, Whyros = Verhaltens-Input (partiell bei Outcomes). Verschiedene Populationen: HubSpot 69.508 > CIO ~59k+ > Whyros 52.828.
4. **`contacts.total_revenue` (Whyros) bleibt kaputt** → Umsatz-Wahrheit = HubSpot Deal-Amount (Sample ⌀3.833) + **Bexio** (Accounting).

## Populationen & SSOT pro Domäne

| Domäne | SSOT | Größe |
|---|---|---|
| Web-Verhalten / Touchpoints / Ad-Attribution | Whyros | 52.828 Kontakte, 1,6M TP, 1,4M Events |
| Email-Engagement / Automation / Segmente | Customer.io | ~59k+ (Superset), 131 Segmente |
| CRM / Sales / Pipeline / Calls / Tier / Phone | **HubSpot** | 69.508 Kontakte, 532 Props, 4 Pipelines |
| Umsatz / Rechnungen (Geld-Wahrheit) | Bexio | — |
| Telefonie / Dialer | Aircall | 1.822 anrufbar |
| Call-Aufnahmen / Transkripte | Zoom | — |

## Daten-Quellen-Inventar (was wir ziehen können / könnten)

| # | System | Schlüssel-Daten | Status | Rolle / ungenutztes Potenzial |
|---|--------|-----------------|--------|------------------------------|
| 1 | **Whyros** (Supabase) | pageview/scroll/video_*/form_submit/click; touchpoints (email/ad); utm/gclid/fbclid; low-ticket purchases | ✅ integriert (Scorer liest) | INPUT-Signale. |
| 2 | **HubSpot** | 69.5k Kontakte, 532 Props; Deals: Vertrieb (747 Won)/EC/Webinar/Fulfillment; Call-Outcomes; lead_*-Scoring-Props; 20k Phones | ✅ integriert (R/W) | OUTCOME/LABEL + Dialer-Input + Kevins Workspace. |
| 3 | **Customer.io** | open/click-120d, 131 Segmente (Webinar-Funnel, Eignungscheck, Goals, Käuferstatus), 15.253 Unsub, Funnel-Optin-Daten | 🟡 teilweise (Buyer-Segmente) | Engagement + Segment-Signale. **Ungenutzt:** Live-Launch/Launchcall-Optin (7k–12k), Engaged-120d, Goal, Unsub-Suppression. |
| 4 | **Bexio** | Rechnungen, Zahlungen, echter Umsatz | ❌ nicht im Scoring | **Geld-Wahrheit** → Label-Wert/Kalibrierung. Hohes Potenzial. |
| 5 | **Aircall** | Dialer, Call-Dauer, Dispositions | ✅ integriert | Call-Ausführung + Outcome. |
| 6 | **Zoom** | Aufnahmen → VTT → AI-Summary | 🟡 teilweise (HubSpot-Notes) | **Ungenutzt:** Sentiment/Einwände/Qualität → Scoring + Coaching. |
| 7 | **Kajabi** | Käufe (Gratis-Optins), Kurs-Konsum | 🟡 teilweise (Käufe) | **Ungenutzt:** Kurs-Konsum → Retention/Upsell. |
| 8 | **Tally** | **Eignungscheck-ANTWORTEN** (Ziel/Situation) | 🟡 nur „submitted" | **Ungenutzt + Gold:** echte Qualifikations-Antworten → Scoring + Kevins Call. |
| 9 | **Meta/Google Ads** | Spend, Kampagnen/Ad-Performance, CAC | ❌ (Whyros hat ad_spend) | **Ungenutzt:** CAC/Qualität pro Ad → welche Quelle bringt High-LTV. |
| 10 | GA4 | Web-Analytics aggregiert | ❌ | Aggregat-Metriken. |
| 11 | Stape (sGTM/CAPIG) | Server-side Events (CAPI) | infra | Zuverlässige Event-Zustellung. |
| 12 | Webflow | Funnel-Sites/Formulare | infra | Optin-Quelle (wo Phone erfasst würde). |

## Architektur (siehe Diagramm im Chat)

```
Meta/Google Ads ─► Stape sGTM+CAPIG (Klick-IDs) ─► Whyros
                                                      │
Signal-Quellen: Whyros(Verhalten+Ad-Attr) · CIO(Engage) · Tally(Antworten) · HubSpot(CRM)
        │                                                                     ▲
        ▼                                                                     │
   SBC Scoring-Engine ─► HubSpot Tier+Lists ─► Aircall Dialer(Kevin) ─► HubSpot Deals(747)+Bexio
        ▲                                                                     │
        ├──────── Feedback A: Outcome → Re-Kalibrierung ──────────────────────┤
        └─ Feedback B (W9): Deal Won → Meta/Google via CAPIG (value-based) ────┘
```
Zwei Loops: (A) Outcome kalibriert/trainiert den Score; (B) **W9** spielt echte Abschlüsse +
Wert an die Ad-Plattformen zurück → Algorithmen optimieren auf Käufer statt Leads.
**Wichtig (Tracking-Crew):** sGTM `GTM-PN6X3W6Z` v20 ist **geparkt** (alle CAPI-Tags paused, bewusst
kein 2. Meta-Pfad); aktiver Server-Leg = **CAPIG** `sptpwfrm` (Pixel 314). gclid/fbclid in Whyros sind
**client-side** (Collector), unabhängig vom sGTM. **W9 ist KEIN Neubau** — die Tracking-Crew sendet
Conversions bereits (Bexio/Ablefy → Meta-CAPI; geplanter Railway-Service `palacios-tracking`) →
**koordinieren, nicht doppeln** (Obsidian [[Lead-Scoring-Tracking-Synergien-2026-06-20]]).

- **Engine** liest Whyros (Verhalten) + HubSpot (CRM/Phone/Tier) + CIO (Segmente) [+ künftig Tally-Antworten], rechnet Score→Tier, schreibt HubSpot, füttert Aircall.
- **Label/Feedback** = HubSpot Deal Won (Vertrieb) → kalibriert + trainiert den Score (Loop = W4).

## Revidiertes Lead-Scoring-Konzept

- **Label = HubSpot Deal Won (747)**, Umsatz-Gewicht via Deal-Amount/Bexio. → Modell jetzt machbar.
- **Scoring-Ansatz neu offen:** mit 747 Labels ist ein **kalibriertes, erklärbares Modell** (Logistic) realistisch — nicht mehr nur die Deterministik-Heuristik. Empfehlung: erklärbares Modell mit Holdout-Validierung; Deterministik als Fallback/Sanity-Baseline.
- **Signale:** Whyros (form_submit-Gate, video_complete, **Replay-Watch 7,8×**, Preis-Seite) + CIO (open/click-120d, Live-Launch/Launchcall-Optin = Webinar-Intent, Eignungscheck-Segment) + **Tally-Antworten** (Ziel/Situation) + HubSpot (Pipeline-Stage, Vortouches). Phone = Dialer-Gate, kein Score-Signal.
- **Phone/Reichweite demontiert:** Hot/Warm zu 91% mit Phone. Der Hebel ist NICHT mehr Phone, sondern (a) Scoring-Genauigkeit (sind die 1.822 die Richtigen?), (b) mehr Leads überhaupt scoren, (c) Tally-Antworten + Zoom-Intelligence.
- **Suppression:** 15.253 CIO-Unsub müssen Dialer + Sync gaten.

## Revidierte Workstreams

| WS | Inhalt | Änderung ggü. Vorgänger-Konzept |
|----|--------|--------------------------------|
| W1 Fundament | Label = HubSpot Deal Won; Population/SSOT-Map; Mapping/URL-Taxonomie-Fix | Label-Quelle gewechselt (Whyros→HubSpot Deals) |
| W2 Scoring | Modell ODER Deterministik (747 Labels → Modell machbar); Signale aus 4 Quellen; Tiers an Close-Rate | „nur Deterministik" aufgehoben — Modell zurück auf dem Tisch |
| W3 Reichweite | **demontiert** (Phone kein Bottleneck) → ersetzt durch „mehr scoren + Tally/Zoom" | war „größter Hebel", ist es nicht |
| W4 Feedback-Loop | Tier-bei-Erstcall → Deal Won messen | Label-Quelle HubSpot Deals |
| W6 CIO-Sync | Engagement/Segmente/Unsub → HubSpot (KEIN Phone-Recovery) | Phone-Begründung gestrichen |
| **W7 (neu)** Tally-Antworten | Eignungscheck-Antworten als Qualifikations-Signal + Kevins Card | NEU — hohes Potenzial |
| **W8 (neu)** Bexio-Umsatz | Geld-Wahrheit für Label-Wert/ROAS | NEU |
| **W9 (neu)** Server-Side Conversion-Feedback | Deal Won → Meta/Google via Stape CAPIG (value-based bidding) | NEU — nutzt bestehende Tracking-Infra |
| Lecks | No-Show; `al`=Artefakt | unverändert |

## Workstream-Details: W7 / W8 / W9

### W7 — Tally-Eignungscheck-Antworten *(größter ungenutzter Signal-Hebel)*
Form `nPJzEe` „Hypnose-Eignungscheck": 2.533 completed (von 6.912). Pro Lead bisher ungenutzt:
- **Budget** (✅ verteilt): Unter 2000 **57%** · 2000–4000 26% · 4000–6000 5% · 6000–8000 0,7%
  → echte Käufer-Range (4000+) = ~146 Leads. Killer-Qualifier (Ausbildung ~CHF 4–9k).
- **Beratung gewünscht:** „Ja, gerne!" **745** → sofort in Dialer.
- **Interesse:** „richtiger nächster Schritt" 450 (heiß) vs. „grundsätzlich" 1.877 vs. „gar nicht" 154.
- **Ziel:** Pers. Weiterentwicklung 1.273 · Beruflicher Wechsel/2. Standbein 953 · Methodik 255.
- 9-Fragen-Self-Assessment mit **berechnetem Score** (Calculated Fields) + UTM-Hidden-Fields.
- Nutzung: Budget/Interesse/Beratung → Score-Boost/-Gate; Ziel+Score → Kevins Card.
- 🔶 Offen: Conversion-Join (Budget → echte Close-Rate) als finaler Beweis; Tally-API write-fähig.

### W8 — Bexio-Umsatz-Wahrheit
Bexio = Accounting-SSOT (Memory). Liefert den echten bezahlten Umsatz pro Kontakt/Produkt →
ersetzt das kaputte `total_revenue` als Wert-Label; ermöglicht ROAS/CAC korrekt zu rechnen.

### W9 — Server-Side Conversion-Feedback (value-based bidding) *(nutzt bestehende Infra)*
- Befund: 7.485 Kontakte sind ad-attribuiert (gclid 73k / fbclid 270k / meta_ad_id 273k Events).
  Ad-Param als **direktes** Score-Signal schwach (0,72% vs 0,48%) — Wert liegt im Loop.
- Lücke: Klick-IDs gehen rein, aber **echte Abschlüsse gehen nicht zurück**.
- ⚠️ **Koordination PFLICHT (kein Parallel-Sender):** die Tracking-Crew (Multi-CC-Board) sendet
  Conversions bereits (`bexio_meta_offline.py` Diplome → Meta-CAPI, `ablefy/kajabi`-Sender; geplanter
  Service `palacios-tracking`). W9 NICHT parallel bauen — abstimmen, ob HubSpot `Deal Won` etwas VOR
  der Bexio-Zahlung beiträgt (CRM-Stage vs. bezahlt). AW-8-Dedup-Disziplin (1 event_id/Sale) respektieren.
- sGTM v20 geparkt (CAPI-Tags paused) → Server-Leg via CAPIG; gclid/fbclid client-side in Whyros.
- Plus (falls additiv): CAC/Qualität pro Kampagne aus `ad_spend` (15.967) + Attribution.
- Risiko: Doppel-Sender / Pixel-314-Mehrfachzählung — daher Tracking-Crew-PM-Abstimmung vor jeder Aktivierung.

## Offene Verifikation 🔶
- HubSpot lead_*-Props enthalten bereits die Lifecycle-/Phone-Felder (`lead_pause_until`,
  `lead_no_answer_streak/cycles`, `lead_dialer_removed`, `lead_phone_status`, `lead_phone_dnc`) —
  ich hatte deren Anlage deferred. **Wer/wann angelegt?** Vor Dialer-Lifecycle-Deploy verifizieren.
- Tally-API-Zugang + Feld-Mapping der Eignungscheck-Antworten (W7).
- Bexio↔HubSpot/Whyros Match-Key (W8).
- HubSpot 532 Props: welche tragen verwertbares Signal (gezielter Scan in W1).
