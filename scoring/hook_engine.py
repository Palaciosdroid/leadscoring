"""
Hook Engine — Rule-based conversation hook suggestions for call cards.

Generates context-specific opening lines for Kevin (the closer)
based on the lead's engagement history and funnel position.
"""


def generate_hook(context: dict) -> str:
    """
    Generate a context-specific conversation hook for the closer.

    Evaluates rules in priority order and returns the first matching
    hook.  Falls back to a generic opener if no specific rule fires.

    Expected context keys:

    - ``video_percent``: float (0-100, Phase 2 — video watch progress)
    - ``eignungscheck``: bool (completed the qualification check)
    - ``call_booked``: bool (already booked a call)
    - ``email_clicked``: bool (clicked an email link recently)
    - ``checkout_visited``: bool (visited the checkout page)
    - ``purchased_products``: list[str] (products already purchased)
    - ``is_fresh``: bool (new lead)
    - ``fresh_hours``: float (hours since first touch)
    - ``last_email_subject``: str (subject/content of last clicked email)
    - ``funnel``: str (``"hypnose"`` | ``"meditation"`` | ``"lifecoach"``)
    - ``score``: float (overall lead score)
    - ``visited_offer_page``: bool (viewed the offer/sales page)
    - ``visited_checkout``: bool (visited checkout page — from browser events)
    - ``watched_video_on_offer``: bool (played video on offer page)
    - ``viewed_pricing``: bool (viewed pricing/costs page)
    """
    for rule in _RULES:
        result = rule(context)
        if result is not None:
            return result

    # Should never reach here because the last rule always matches,
    # but just in case:
    return _FALLBACK_HOOK


# ---------------------------------------------------------------------------
# Internal rule definitions (priority order)
# ---------------------------------------------------------------------------

_FALLBACK_HOOK = "Wie bist du auf Gabriel aufmerksam geworden?"


def _rule_video_watched(ctx: dict) -> str | None:
    """High video engagement — lead watched most of the VSL."""
    video_percent = ctx.get("video_percent") or 0
    if video_percent > 75:
        return (
            "Du hast das Video fast komplett geschaut "
            "— was hat dich angesprochen?"
        )
    return None


def _rule_checkout_abandoned(ctx: dict) -> str | None:
    """Checkout visited but no full Ausbildung yet — cart abandonment or upsell."""
    visited_checkout = ctx.get("checkout_visited") or ctx.get("visited_checkout")
    # Only suppress for leads who already own a full Ausbildung (hc/mc/gc)
    has_ausbildung = any(
        p.lower() in ("hc", "mc", "gc") for p in (ctx.get("purchased_products") or [])
    )
    if visited_checkout and not has_ausbildung:
        return (
            "Du warst schon fast dabei "
            "— was hat dich noch zurückgehalten?"
        )
    return None


def _rule_inner_journey_buyer(ctx: dict) -> str | None:
    """Has Inner Journey bundle but no full Ausbildung — upsell opener."""
    purchased = ctx.get("purchased_products") or []
    has_inner_journey = any(
        "inner_journey" in p.lower() or "inner journey" in p.lower()
        for p in purchased
    )
    # Only fire if no full Ausbildung purchased yet
    has_ausbildung = any(
        p.lower() in ("hc", "mc", "gc")
        for p in purchased
    )
    if has_inner_journey and not has_ausbildung:
        return (
            "Du kennst schon das Inner Journey Paket "
            "— was hat dir daran am besten gefallen?"
        )
    return None


def _rule_offer_video_watched(ctx: dict) -> str | None:
    """Watched video on offer page — strong buying signal."""
    if ctx.get("watched_video_on_offer"):
        return (
            "Du hast dir das Video auf der Angebotsseite angeschaut "
            "— was hat dich besonders angesprochen?"
        )
    return None


def _rule_offer_page_visited(ctx: dict) -> str | None:
    """Visited the offer page — browsing intent."""
    if ctx.get("visited_offer_page") and not ctx.get("visited_checkout"):
        return (
            "Du hast dir das Angebot angeschaut "
            "— hast du noch Fragen dazu?"
        )
    return None


def _rule_pricing_viewed(ctx: dict) -> str | None:
    """Viewed pricing page — price-conscious, near decision."""
    if ctx.get("viewed_pricing"):
        return (
            "Du hast dir die Kosten und Termine angeschaut "
            "— soll ich dir die Optionen erklären?"
        )
    return None


def _rule_eignungscheck_no_call(ctx: dict) -> str | None:
    """Completed qualification check but hasn't booked a call yet."""
    if ctx.get("eignungscheck") and not ctx.get("call_booked"):
        return (
            "Du hast den Eignungscheck gemacht — super! "
            "Wie war dein Ergebnis?"
        )
    return None


def _rule_email_clicked(ctx: dict) -> str | None:
    """Recently clicked an email — reference the subject."""
    subject = ctx.get("last_email_subject")
    if ctx.get("email_clicked") and subject:
        return (
            f"Du hast dir '{subject}' angeschaut "
            "— was hat dich interessiert?"
        )
    return None


def _rule_very_fresh(ctx: dict) -> str | None:
    """Brand-new lead (under 2 hours)."""
    if ctx.get("is_fresh") and (ctx.get("fresh_hours") or float("inf")) < 2:
        return (
            "Toll, dass du gerade erst dabei bist! "
            "Was hat dich zu Gabriel geführt?"
        )
    return None


def _rule_fresh(ctx: dict) -> str | None:
    """New lead (fresh but not ultra-fresh)."""
    if ctx.get("is_fresh"):
        return (
            "Du bist noch ganz frisch dabei "
            "— was hat dein Interesse geweckt?"
        )
    return None


def _rule_funnel_hypnose(ctx: dict) -> str | None:
    """Lead is in the hypnosis funnel."""
    if ctx.get("funnel") == "hypnose":
        return "Was hat dich an der Hypnose-Ausbildung interessiert?"
    return None


def _rule_funnel_meditation(ctx: dict) -> str | None:
    """Lead is in the meditation funnel."""
    if ctx.get("funnel") == "meditation":
        return "Was hat dich an der Meditations-Ausbildung interessiert?"
    return None


def _rule_funnel_lifecoach(ctx: dict) -> str | None:
    """Lead is in the lifecoach funnel."""
    if ctx.get("funnel") == "lifecoach":
        return "Was hat dich an der Gesprächscoach-Ausbildung interessiert?"
    return None


def _rule_fallback(ctx: dict) -> str | None:
    """Generic fallback — always matches."""
    return _FALLBACK_HOOK


# Ordered rule chain — first match wins.
# Priority: strongest buying signals first.
_RULES: list = [
    _rule_video_watched,          # Watched VSL video (highest intent)
    _rule_checkout_abandoned,     # Was on checkout but didn't buy
    _rule_inner_journey_buyer,    # Has Inner Journey but no Ausbildung (upsell)
    _rule_offer_video_watched,    # Watched video on offer page
    _rule_eignungscheck_no_call,  # Did qualification check
    _rule_pricing_viewed,         # Looked at pricing/costs
    _rule_offer_page_visited,     # Browsed the offer page
    _rule_email_clicked,          # Clicked email link
    _rule_very_fresh,             # Brand new (< 2h)
    _rule_fresh,                  # Fresh (< 24h)
    _rule_funnel_hypnose,
    _rule_funnel_meditation,
    _rule_funnel_lifecoach,
    _rule_fallback,
]
