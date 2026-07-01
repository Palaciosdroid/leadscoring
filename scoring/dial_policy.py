"""Single source of truth for the POSITIVE dialer-push decision.

All three push paths — the batch scorer, the realtime webhook, and the
WhatsApp webhook — call `should_push_lead()` so the score / freshness /
eignungscheck thresholds stay identical everywhere. Previously each path had
its own inline gate, which drifted (realtime lacked the fresh score floor,
WhatsApp ignored freshness and list routing entirely).

This function answers only "is this lead worth dialing at all". The separate
EXCLUSION decision (unsubscribed / paused / booked / DNC) is handled by
`batch.dialer_gate.dialer_suppressed` (webhooks) and the inline DNC/pause
checks (batch) — never here.
"""

# Score thresholds — the single definition; scorer.py imports these so its
# tiering/routing and this push gate can never disagree.
SCORE_WARM = 30       # >= 30 → dialable (Hot + Warm)
FRESH_MIN_SCORE = 10  # a "fresh" lead still needs >= this to enter Aircall
                      # (blocks a single page_visited=3 lead from being dialled)


def should_push_lead(
    *,
    score: float,
    is_fresh: bool,
    list_key: str | None,
    is_dormant_warm: bool = False,
) -> bool:
    """True if the lead qualifies to be pushed to the Power Dialer.

    Mirrors the batch scorer's original rule, now shared by all paths:
      - Eignungscheck leads always qualify (form submission is the gate).
      - Fresh leads qualify only at score >= FRESH_MIN_SCORE.
      - Any lead at score >= SCORE_WARM qualifies.
      - Dormant Hot/Warm leads (previously engaged, no recent events) qualify.

    Callers still AND this with `has_phone` and run the exclusion gate.
    """
    return bool(
        list_key == "eignungscheck"
        or (is_fresh and score >= FRESH_MIN_SCORE)
        or score >= SCORE_WARM
        or is_dormant_warm
    )
