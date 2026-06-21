# W2+W7 Scoring-Kern — Design (Transparentes Punkte-System + Tally-Integration)

Datum: 2026-06-20
Status: Spec zur Review. Architektur **A (transparentes Punkte-System)** von Sandro bestätigt.
Voraussetzung: W1 (Fundament) live. Master-Konzept `2026-06-20-leadscoring-complete-architecture.md`.

## Zweck
Den Dialer-Score von handgeschätzten Engagement-Punkten auf ein **erklärbares, data-kalibriertes
Punkte-System** umstellen, das auf **HubSpot Deal Won** (echtes Label) zielt und die **Tally-
Eignungscheck-Antworten** (stärkstes Signal) nutzt. Voll nachvollziehbar für Sandro + Kevin.

## Datengrundlage (gemessene Deal-Won-Rate — alle ✅ verifiziert)
| Signal | Close-Rate | → Punkte (Startwert) |
|---|---|---|
| Budget 4000-6000 / 6000-8000 | 10,9% / 5,6% | **+30** |
| Budget 2000-4000 | 6,6% | **+15** |
| Budget Unter 2000 | 1,5% | 0 |
| Interesse „richtiger nächster Schritt" | 7,3% | **+25** |
| Interesse „grundsätzlich interessiert" | 2,7% | +10 |
| Interesse „gar nicht interessiert" | **0% (0/154)** | **→ DISQUALIFY** |
| Beratung „Ja, gerne!" | 4,6% | +15 |
| Replay/Webinar-Watch | 3,5% (7,8×) | +20 |
| video_complete / checkout-visit / price-page | (W1-mapped) | +20 / +25 / +15 |
| form_submit (Optin-Baseline) | 4,3% (9×) | +10 |

Startwerte werden in der **Kalibrierung** (s.u.) gegen die echte Close-Rate justiert, nicht final geraten.

## Komponenten

### W7 — Tally→HubSpot-Sync  *(neue Properties + Sync)*
- `integrations/tally.py` (neu): Eignungscheck-Submissions (Form `nPJzEe`) via Tally-API (UA-Header),
  by-email → Antworten. Read-only auf Tally.
- 5 neue HubSpot-Props (via `create_hs_properties.py`): `lead_eig_budget` (enum), `lead_eig_interest`
  (enum), `lead_eig_consult` (bool), `lead_eig_goal` (text), `lead_eig_score` (number).
- Sync schreibt sie pro Kontakt nach HubSpot (nur leere/geänderte Felder). Scorer liest sie; **Kevins
  Aircall-Card zeigt Budget + Ziel + Self-Assessment-Score** (perfekter Gesprächseinstieg).

### W2 — Punkte-System  *(scoring/points.py, pure + getestet)*
- `compute_points(signals: dict) -> PointsResult(points:int, tier:str, reasons:list[str])`.
- Gewichte = obige Tabelle (Konstanten, kalibriert). „gar nicht interessiert" / unsubscribed →
  `tier=4_disqualified`. **Phone = Dialer-Gate, KEIN Score-Signal** (Leakage-Schutz).
- `reasons` = erklärbarer Breakdown („Budget 4000+ +30 · Replay +20 · …") für Kevins Card.
- Tier-Schwellen aus Kalibrierung (Hot/Warm/Cold).

### W2-Kalibrierung  *(analytics/calibrate_points.py)*
- Punkte-System auf historische Kontakte rechnen → Punkte-Bucket → **echte Deal-Won-Rate** → Hot/Warm/
  Cold-Schwellen dort setzen, wo die Close-Rate es rechtfertigt. Output = die finalen Konstanten + ein
  Konzentrations-Report (welcher %-Anteil der Closes fällt in Hot/Warm).

### W2-Integration  *(scorer.py, flag-gegated)*
- `SCORING_MODE` env: `engagement` (alt, default/rollback) | `points` (neu). Bei `points` nutzt der
  Scorer `compute_points` (liest Tally-Props + W1-Verhalten + Interest), schreibt `lead_tier`/
  `lead_combined_score`; Aircall-Card bekommt den `reasons`-Breakdown.
- **Rollout:** Props anlegen → Tally-Backfill → kalibrieren → Deploy mit `SCORING_MODE=engagement`
  (Punkte werden berechnet + als Schatten-Property `lead_points` geloggt, Dialer unverändert) → 1 Batch
  Vergleich (konzentriert Punkte die Closes besser?) → **Flip auf `points`**. Rollback = Flag zurück.

### Mitgenommene TODOs (aus Deploy-Validierung)
- Scorer überspringt **dynamische** Listen 365-370 in `batch_add_to_list` (redundant → 400-Noise raus;
  sie füllen sich aus den Properties selbst).
- Note-Writing-Performance (~7k sequentielle Calls/Batch) — Skip-unchanged via Card-Hash.

## Error Handling
- Tally/HubSpot-Fetch: Retry + Fail-soft (Sync überspringt fehlende, crasht nie).
- `compute_points`: fehlende Signale → 0 Punkte (kein Crash), Default-Tier Cold.

## Testing
- `points.py`: `compute_points` pure → Unit-Tests pro Signal + Disqualify + Tier-Grenzen + reasons.
- `tally.py`: Antwort-Mapping (Fixtures), Enum-Normalisierung.
- Kalibrierung: smoke-test gegen Fixtures.

## Erfolgskriterien
- Tally-Antworten als 5 HubSpot-Props live + auf Kevins Card sichtbar.
- `compute_points` erklärbar (reasons) + getestet.
- Kalibrierung zeigt: Hot/Warm konzentriert die echten Closes deutlich stärker als die alte Logik.
- Flag-Flip ohne Crash; Rollback in 1 Env-Var.

## Offen 🔶 (Review)
- Schatten-Property `lead_points` für den Vergleich vor Flip ok? (Default: ja, sicherer als Blind-Flip)
- Tier-Schwellen final aus Kalibrierung (nicht im Spec festnageln).
- Tally-Sync als eigener Cron oder Teil des Batch? (Default: leichter eigener Cron, entkoppelt vom Score-Batch)
