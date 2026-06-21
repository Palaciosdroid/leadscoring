"""
Transparent Point-System Scorer (target: HubSpot Deal Won).

Calibrated point weights from MEASURED Deal-Won close-rates (see design spec
2026-06-20-w2-w7-scoring-core-design.md). Every contribution is explained in
`reasons` so Kevin's Aircall card shows a fully auditable breakdown.

Signals (all optional — a missing signal contributes 0, never crashes):
    budget            — enum unter_2000 | 2000_4000 | 4000_6000 | 6000_8000
    interest          — enum keines | grundsaetzlich | naechster_schritt
    consult           — bool (Tally "Ja, gerne!")
    replay            — bool (replay / webinar-watch behavior, W1-mapped)
    video_complete    — bool (full video watch, W1-mapped)
    checkout          — bool (checkout-page visit, W1-mapped)
    price             — bool (price-page visit, W1-mapped)
    form_submit       — bool (optin baseline, W1-mapped)
    interest_category — str  ("hypnose" gets a small product-fit bonus)
    unsubscribed      — bool (hard disqualify)

Phone is INTENTIONALLY not a signal — it is the dialer gate, not a score input
(leakage protection). Do not add it here.
"""

from __future__ import annotations

from dataclasses import dataclass, field

# ---------------------------------------------------------------------------
# Point weights — calibrated against measured Deal-Won close-rate.
# Budget is bucketed by enum; the 4000+ buckets share the strongest weight.
# ---------------------------------------------------------------------------
BUDGET_POINTS: dict[str, int] = {
    "6000_8000": 30,   # 5.6% close-rate
    "4000_6000": 30,   # 10.9% close-rate
    "2000_4000": 15,   # 6.6% close-rate
    "unter_2000": 0,   # 1.5% close-rate
}

INTEREST_POINTS: dict[str, int] = {
    "naechster_schritt": 25,   # "richtiger nächster Schritt" — 7.3%
    "grundsaetzlich":    10,   # "grundsätzlich interessiert" — 2.7%
    "keines":             0,   # "gar nicht interessiert" — 0% -> disqualify
}

CONSULT_POINTS = 15          # "Ja, gerne!" beratung — 4.6%
REPLAY_POINTS = 20           # replay / webinar-watch — 3.5% (7.8x lift)
VIDEO_COMPLETE_POINTS = 20   # full video watch (W1-mapped)
CHECKOUT_POINTS = 25         # checkout-page visit (W1-mapped)
PRICE_POINTS = 15            # price-page visit (W1-mapped)
FORM_SUBMIT_POINTS = 10      # optin baseline — 4.3% (9x lift)
HYPNOSE_CATEGORY_POINTS = 10  # product-fit bonus for hypnose interest

# A SKIPPED Eignungscheck question is "unknown", NOT "low" — empirically it
# converts near base-rate (missing-interest even higher). Give a neutral weight,
# never 0-penalize. Gated on `eignungscheck`: only applies to leads who actually
# took the quiz; non-takers (no Tally data) get 0 here and score on behavior only.
MISSING_BUDGET_POINTS = 8     # skipped budget answer — neutral
MISSING_INTEREST_POINTS = 10  # skipped interest answer (NOT the explicit "keines")

DISQUALIFIED_TIER = "4_disqualified"

# ---------------------------------------------------------------------------
# Tier thresholds — descending, first match wins (>= check).
# Start values per spec; the calibration step (analytics/calibrate_points.py)
# tunes them against the real Deal-Won rate before the flag-flip.
# ---------------------------------------------------------------------------
TIERS: list[tuple[str, int]] = [
    ("1_hot",  50),   # calibrated: 8.7% close-rate, 24% of cohort, 58% of all closes
    ("2_warm", 35),   # 35-49 pts (~3%) — secondary calling tier
    ("3_cold",  0),   # <35 pts (~1%) — deprioritize
]


def _determine_tier(points: int) -> str:
    for tier_id, threshold in TIERS:
        if points >= threshold:
            return tier_id
    return "3_cold"


@dataclass
class PointsResult:
    points: int
    tier: str
    reasons: list[str] = field(default_factory=list)


def compute_points(signals: dict) -> PointsResult:
    """
    Compute the transparent point-score for a single lead.

    Pure function: same input -> same output, no I/O. Missing signals contribute
    0 points (no crash). "gar nicht interessiert" or unsubscribed force the
    `4_disqualified` tier regardless of accumulated points.

    Returns PointsResult(points, tier, reasons) where `reasons` lists each
    contribution as a human-readable string (for the Aircall card).
    """
    points = 0
    reasons: list[str] = []
    disqualified = False
    # Neutral-fill skipped answers ONLY for actual quiz-takers (non-takers have no Tally data).
    took_eig = bool(signals.get("eignungscheck"))

    # --- Budget ------------------------------------------------------------
    budget = signals.get("budget")
    if budget in BUDGET_POINTS:
        pts = BUDGET_POINTS[budget]
        if pts:
            points += pts
            reasons.append(f"Budget {budget} +{pts}")
    elif took_eig:
        points += MISSING_BUDGET_POINTS
        reasons.append(f"Budget unbekannt +{MISSING_BUDGET_POINTS}")

    # --- Interest ----------------------------------------------------------
    interest = signals.get("interest")
    if interest == "keines":
        disqualified = True
        reasons.append("Interesse keines -> disqualified")
    elif interest in INTEREST_POINTS:
        pts = INTEREST_POINTS[interest]
        if pts:
            points += pts
            reasons.append(f"Interesse {interest} +{pts}")
    elif took_eig:
        points += MISSING_INTEREST_POINTS
        reasons.append(f"Interesse unbekannt +{MISSING_INTEREST_POINTS}")

    # --- Consult -----------------------------------------------------------
    if signals.get("consult"):
        points += CONSULT_POINTS
        reasons.append(f"Beratung Ja +{CONSULT_POINTS}")

    # --- Behavior (W1-mapped) ---------------------------------------------
    if signals.get("replay"):
        points += REPLAY_POINTS
        reasons.append(f"Replay +{REPLAY_POINTS}")
    if signals.get("video_complete"):
        points += VIDEO_COMPLETE_POINTS
        reasons.append(f"Video complete +{VIDEO_COMPLETE_POINTS}")
    if signals.get("checkout"):
        points += CHECKOUT_POINTS
        reasons.append(f"Checkout +{CHECKOUT_POINTS}")
    if signals.get("price"):
        points += PRICE_POINTS
        reasons.append(f"Price page +{PRICE_POINTS}")
    if signals.get("form_submit"):
        points += FORM_SUBMIT_POINTS
        reasons.append(f"Form submit +{FORM_SUBMIT_POINTS}")

    # --- Product-fit bonus -------------------------------------------------
    if signals.get("interest_category") == "hypnose":
        points += HYPNOSE_CATEGORY_POINTS
        reasons.append(f"Hypnose interest +{HYPNOSE_CATEGORY_POINTS}")

    # --- Hard disqualify ---------------------------------------------------
    if signals.get("unsubscribed"):
        disqualified = True
        reasons.append("Unsubscribed -> disqualified")

    if disqualified:
        return PointsResult(points=points, tier=DISQUALIFIED_TIER, reasons=reasons)

    return PointsResult(points=points, tier=_determine_tier(points), reasons=reasons)
