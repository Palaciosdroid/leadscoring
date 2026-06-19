# Design: Dialer Lead-Lifecycle & Phone-Validierungs-Regeln

Datum: 2026-06-19
Projekt: SBC Lead Scoring (`Palaciosdroid/leadscoring`)
Status: Approved (Brainstorming abgeschlossen 2026-06-19)

## Ziel

Den Aircall Power Dialer (Kevins Closer-Queue) um einen sauberen Lead-Lebenszyklus
erweitern: Leads nach Call-Ergebnis pausieren statt sie tot- oder dauer-anzurufen,
nach Ablauf automatisch wieder aufnehmen, und Telefonnummern vor dem Wählen
validieren/reparieren.

## Kontext / Ist-Zustand (verifiziert im Code)

- Scoring + Push laufen über `batch/scorer.py` (`run_batch_scoring`), Cron 3×/Tag
  08/12/16 CET. Aircall-Logik in `integrations/aircall.py`.
- Call-Ergebnisse kommen via `batch/call_poller.py` (Poll alle 5 min) →
  `write_call_outcome()` schreibt `lead_last_call_date` + `lead_last_call_outcome`.
- Exclusion heute: `_should_exclude_from_queue()` in `scorer.py` mit Tages-Cooldowns
  (erreicht 7d, voicemail/no-answer 3d) und `MAX_CALL_ATTEMPTS=5`.

### Zwei kritische Gaps (Blocker für die neuen Regeln)

1. **`lead_call_attempts` wird nirgends hochgezählt.** `scorer.py:1041` liest den Wert,
   aber kein Schreiber existiert. `MAX_CALL_ATTEMPTS=5` greift faktisch nie. Der
   Versuchs-/Streak-Zähler muss erst gebaut werden.
2. **No-Answer-Calls schreiben gar kein Outcome.** In `call_poller.py` werden
   "Anschläge" (`Keine Antwort`, `Besetzt`, `Voicemail`) nur in `_processed_call_ids`
   abgelegt und geloggt — `write_call_outcome()` läuft NUR für `connected_calls`
   (`Kontakt aufgenommen`, `Live-Nachricht`). Ohne Fix sieht die State-Machine
   "nicht erreicht" nie. Muss geändert werden: alle Dispositions verarbeiten
   (State-Update), aber weiterhin nur Connected → Slack.

### Echte Call-Outcomes (`integrations/hubspot.py` HS_DISPOSITION_MAP)

| Outcome | Klasse |
|---|---|
| `Kontakt aufgenommen` | reached |
| `Live-Nachricht hinterlassen` | reached |
| `Keine Antwort` | no_answer |
| `Besetzt` | no_answer |
| `Voicemail hinterlassen` | no_answer |
| `Falsche Nummer` | wrong_number |

## Entschiedene Regeln (Brainstorming 2026-06-19)

1. **Pause-Loop = Zyklus mit Obergrenze.** Nach Pause Counter-Reset, erneut anrufen.
   Nach max. 2 vollen No-Answer-Pause-Zyklen ohne je erreicht → endgültig raus
   (bleibt im E-Mail-Nurturing).
2. **Erreicht = jeder Erreichte → 3 Monate Pause** (auch "kein Interesse", da es dafür
   keine eigene Disposition gibt). Echte Totalabsagen setzt Kevin manuell über
   `lead_not_interested` → dauerhaft raus.
3. **Telefon = validieren + sichere Auto-Korrektur** via Google `phonenumbers`. Was
   danach ungültig bleibt → nicht anrufen, in HubSpot + Slack flaggen.
4. **Zusatzregeln v1 (alle 4):** Intent-Reaktivierung, Anrufzeit-Fenster + Zeitzone,
   Telefon-DNC getrennt von E-Mail-Opt-out, Dedupe nach Telefonnummer.

## Architektur

### State-Machine (Herzstück)

Zustände: `active` → `paused_3mo` / `paused_2mo` → `active` (Re-Entry) / `removed`.

Persistente HubSpot-Properties (neu, müssen angelegt werden):

| Property | Typ | Zweck |
|---|---|---|
| `lead_pause_until` | datetime (ISO) | Solange in Zukunft → nicht im Dialer. Ein Feld für beide Pausen. |
| `lead_no_answer_streak` | number | Aufeinanderfolgende "nicht erreicht"; Reset bei "erreicht". |
| `lead_no_answer_cycles` | number | Abgeschlossene 2-Mte-Pause-Zyklen; Cap = 2. |
| `lead_phone_status` | enum: `valid`/`corrected`/`invalid` | Ergebnis der Phone-Validierung. |
| `lead_phone_dnc` | bool | Permanentes Telefon-Opt-out (unabhängig von E-Mail). |

Vorhandene Properties bleiben: `lead_last_call_date`, `lead_last_call_outcome`,
`lead_not_interested`, `lead_call_booked`, `lead_tier`, `lead_combined_score`.

#### Übergangsfunktion (pure, testbar)

`apply_call_outcome(state: LifecycleState, outcome_class: str, now: datetime) -> LifecycleState`

- `reached` → `pause_until = now + 90d`; `no_answer_streak = 0`; `no_answer_cycles = 0`.
- `no_answer` → `no_answer_streak += 1`; wenn `streak >= 3`:
  - wenn `no_answer_cycles >= 2` → `removed = True` (raus ins Nurturing).
  - sonst → `pause_until = now + 60d`; `streak = 0`; `no_answer_cycles += 1`.
- `wrong_number` → `removed = True`; `phone_status = "invalid"` (+ Slack-Flag).
- Unbekannte Disposition → keine Änderung (no-op).
- Gebuchte Termine (`lead_call_booked` / `0_booked`) werden hier nicht angefasst —
  Booking-Logik im Scorer hat Vorrang.

`now + 90d` / `now + 60d` als Kalender-Monate vereinfacht auf Tage (90/60); exakte
Monatsarithmetik nicht nötig.

### Re-Entry & Exclusion (im Scorer)

Ein Lead wird vom Dialer ausgeschlossen wenn eine dieser Bedingungen gilt:
- `now < lead_pause_until` (aktive Pause), ODER
- `lead_phone_dnc == true`, ODER
- `removed`-Zustand (Cap erreicht), ODER
- bestehende DNC-Gründe: unsubscribed, `lead_call_booked`, already-purchased,
  `lead_not_interested`.

Die alten Tages-Cooldowns (`_should_exclude_from_queue`) werden durch `pause_until`
ersetzt. (Der 24h-Hard-Floor in `do_not_call.py` bleibt als Sicherheitsnetz.)

#### Intent-Reaktivierung

Wenn ein pausierter Lead ein High-Intent-Event NACH dem Pause-Auslöser zeigt, wird
die Pause sofort aufgehoben und der Lead hoch priorisiert.

- Anker = `lead_last_call_date` (Pause wird direkt nach einem Call gesetzt, also gilt:
  Intent-Event mit `timestamp > lead_last_call_date`).
- High-Intent-Events: `checkout_visited`, `price_info_viewed`, `cta_clicked`,
  `email_link_clicked` (aus den bereits gemappten `scored_events`).
- Effekt: `pause_until = now` (= aktiv), `no_answer_streak = 0`. `no_answer_cycles`
  bleibt erhalten (kein Endlos-Loop).
- Gilt v1 nur für `paused_*`, NICHT für `removed`. (Resurrection aus `removed` =
  möglicher v2-Zusatz.)

### Phone-Validierung (`integrations/phone.py`, neu)

`validate_and_normalize(raw: str, default_region: str = "CH") -> tuple[str | None, str]`
→ `(e164_or_None, status)` mit `status ∈ {valid, corrected, invalid}`.

- Parse mit `phonenumbers.parse(raw, default_region)`.
- Vorab sichere Heuristiken (wie bisher in `_normalize_phone`, jetzt zentralisiert):
  Apostroph strippen, `00XX→+XX`, Leerzeichen/Bindestriche entfernen, DACH-Local-Mobile
  → `+CC`. Danach erneut validieren.
- `is_valid_number()` true → `valid` (oder `corrected`, wenn Heuristik gegriffen hat).
- sonst → `(None, "invalid")`.
- `default_region`: aus vorhandener `+`-Vorwahl ableiten; sonst Fallback `CH`
  (Hauptmarkt DACH). Optionale Verbesserung: HubSpot-`country`-Property nutzen.
- Ersetzt `_normalize_phone()` im Push-Pfad. Ungültig → `lead_phone_status=invalid`,
  kein Push, Eintrag in den Slack-Batch-Report (Kevin korrigiert manuell in HubSpot).

### Anrufzeit-Fenster + Zeitzone

- Aus validierter E.164 → Region → repräsentative Zeitzone
  (`phonenumbers` Region-Code + Mapping auf `zoneinfo`).
- Push nur wenn lokale Zeit `09:00 ≤ t < 20:00` UND Wochentag ≠ Sonntag.
- ⚠️ Limitation: Batch läuft nur 3×/Tag → grobes Fenster. Für v1 akzeptiert.

### Dedupe nach Telefonnummer

- Vor dem Aufbau der `aircall_queue`: nach normalisierter E.164 gruppieren, pro Nummer
  nur den höchstpriorisierten Lead behalten (Sortierung: `AIRCALL_PRIORITY`, dann Score).
- Übrige werden geloggt + übersprungen.

### Telefon-DNC

- `lead_phone_dnc == true` → Skip in `check_do_not_call`.
- Gesetzt manuell von Kevin (HubSpot-Property) oder über eine künftige
  "Nicht anrufen"-Disposition (Mapping in `HS_DISPOSITION_MAP`).

## Betroffene Dateien

| Datei | Änderung |
|---|---|
| `batch/lifecycle.py` (neu) | `LifecycleState` + `apply_call_outcome()` pure function. |
| `batch/call_poller.py` | Alle Dispositions verarbeiten (State-Update), nur Connected → Slack. State laden, `apply_call_outcome`, persistieren. |
| `batch/scorer.py` | Exclusion via `pause_until`+DNC; Intent-Reaktivierung; Dedupe; Anrufzeit-Fenster; Phone-Validierung im Push-Pfad. |
| `batch/do_not_call.py` | `lead_phone_dnc`-Check ergänzen. |
| `integrations/phone.py` (neu) | `validate_and_normalize()` via `phonenumbers`. |
| `integrations/hubspot.py` | State-Persistenz (Properties schreiben); ggf. neue Getter. |
| `create_hs_properties.py` | 5 neue Properties in HubSpot anlegen. |
| `requirements.txt` | `phonenumbers` Dependency. |
| `tests/` | Unit-Tests State-Machine + Phone-Modul. |

## Error Handling

- HubSpot-Schreibfehler bei State-Persistenz → loggen, Lead bleibt im alten Zustand
  (nächster Call korrigiert). Niemals den ganzen Poll-Job crashen.
- Phone-Validierung wirft nie → bei Ausnahme `(None, "invalid")` zurückgeben.
- Unbekannte Disposition → no-op (kein State-Change), geloggt.

## Testing

- `apply_call_outcome` (pure) — Unit-Test pro Übergang: reached→3mo, no_answer×3→2mo+cycle,
  Cap (cycles=2 → removed), wrong_number→removed, unbekannt→no-op, Streak-Reset bei reached.
- `validate_and_normalize` — Fall-Tabelle: `+41…` valid, `0041…→+41`, fehlende Vorwahl
  (CH-Fallback), Apostroph/Excel-Artefakt, zu kurz → invalid, Tippfehler-Vorwahl → invalid.
- Intent-Reaktivierung: Event nach `last_call_date` hebt Pause; Event davor nicht.
- Dedupe: gleiche Nummer, 2 Leads → 1 in Queue (höhere Prio gewinnt).
- Fügt sich ins bestehende pytest-Setup (Railway Python 3.12).

## Rollout / Migration

- 5 neue HubSpot-Properties via `create_hs_properties.py` anlegen (idempotent).
- Bestand: Leads ohne `streak`/`cycles`/`pause_until` → Defaults (0 / nicht gesetzt) →
  verhalten sich als `active`. Kein Backfill nötig.
- `phonenumbers` zu `requirements.txt`; Railway-Redeploy.
- Reihenfolge: (1) Properties + Dependency, (2) call_poller State-Schreiben,
  (3) scorer Exclusion/Re-Entry/Phone/Dedupe/Window, (4) Tests grün, (5) Deploy.

## Erfolgskriterien

- Nach einem `Keine Antwort`-Call ist `lead_no_answer_streak` in HubSpot inkrementiert
  (heute: bleibt 0).
- Nach `Kontakt aufgenommen` ist `lead_pause_until ≈ now + 90d`, Streaks 0.
- 3× `Keine Antwort` in Folge → `lead_pause_until ≈ now + 60d`, `cycles = 1`.
- Ungültige Nummer → `lead_phone_status = invalid`, kein Aircall-Push, Slack-Flag.
- Gleiche Nummer erscheint nur 1× in der Dialer-Queue.
- Pausierter Lead mit neuem Checkout-Besuch ist im nächsten Batch wieder im Dialer.
- Alle neuen Unit-Tests grün.

## Offene Punkte (beim Spec-Review zu bestätigen)

- `default_region` Fallback = `CH` ok? (Alternative: HubSpot-`country` heranziehen.)
- `removed` bei Intent-Signal v1 terminal lassen (kein Resurrect)?
- Anrufzeit-Fenster grob wegen 3×/Tag-Batch — für v1 akzeptiert?
