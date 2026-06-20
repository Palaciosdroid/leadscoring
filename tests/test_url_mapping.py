"""URL/event-mapping tests — pin the canonical funnel taxonomy.

Real SBC funnel URLs (Tracking-Crew-Kanon, GA4-verified) → expected internal
event_type via main._map_cio_event. Guards against taxonomy drift.

Canonical precedence: checkout > price > eignungscheck > replay > offer > optin.
Every produced event_type must exist in scoring.engagement.BASE_POINTS, else the
scorer silently drops it — that invariant is asserted at the bottom.
"""

import pytest

from main import (
    _map_cio_event,
    _classify_funnel_url,
    CHECKOUT_URL_PATTERNS,
    SALES_PAGE_PATTERNS,
    PRICE_INFO_PATTERNS,
    EIGNUNGSCHECK_PATTERNS,
    REPLAY_URL_PATTERNS,
    OPTIN_URL_PATTERNS,
)
from scoring.engagement import BASE_POINTS


def _page(url: str) -> dict:
    return {"event": "page", "data": {"page": {"url": url}}}


def _click(url: str) -> dict:
    return {"event": "click", "data": {"page": {"url": url}}}


# (url, expected event_type for a PAGE event)
PAGE_CASES = [
    # --- Checkout / payment ---
    ("https://www.sbc-academy.de/inner-journey-payment", "checkout_visited"),
    ("https://sbc.de/grundausbildung/payment", "checkout_visited"),
    ("https://buchung.bookinea.app/sbc/termin", "checkout_visited"),
    # --- Price (kosten-termine) ---
    ("https://www.sbc-academy.de/grundausbildung/kosten-termine/", "price_info_viewed"),
    ("https://sbc.de/kosten-termine", "price_info_viewed"),
    # --- Eignungscheck (application step) ---
    ("https://www.sbc-academy.de/eignungscheck", "application_submitted"),
    ("https://quiz.sbc.de/onsite/eignungscheck/", "application_submitted"),
    # --- Replay / webinar (video-watch intent) ---
    ("https://www.sbc-academy.de/masterclass", "video_watched_50"),
    ("https://sbc.de/basisseminar/replay", "video_watched_50"),
    ("https://sbc.de/live-workshop", "video_watched_50"),
    ("https://sbc.de/masterclass/day-1", "video_watched_50"),
    ("https://sbc.de/masterclass/day-4", "video_watched_50"),
    # --- Offer / sales ---
    ("https://www.sbc-academy.de/grundausbildung/offer", "sales_page_visited"),
    ("https://sbc.de/grundausbildung/", "sales_page_visited"),
    # --- Optin / thank-you (tracked, low-intent) ---
    ("https://www.sbc-academy.de/optin", "page_visited"),
    ("https://www.sbc-academy.de/optin-thx", "page_visited"),
    # --- Unknown funnel path → generic page ---
    ("https://sbc.de/blog/artikel", "page_visited"),
    ("https://sbc.de/", "page_visited"),
]


# (url, expected event_type for a CLICK event)
CLICK_CASES = [
    ("https://sbc.de/grundausbildung/payment", "checkout_visited"),
    ("https://buchung.bookinea.app/sbc", "checkout_visited"),
    ("https://sbc.de/kosten-termine", "price_info_viewed"),
    ("https://sbc.de/eignungscheck", "application_submitted"),
    ("https://sbc.de/masterclass", "video_watched_50"),
    ("https://sbc.de/grundausbildung/offer", "cta_clicked"),   # CTA on offer page
    ("https://sbc.de/grundausbildung/", "cta_clicked"),
    ("https://sbc.de/optin", None),                            # low-intent click ignored
    ("https://sbc.de/blog", None),                             # generic click ignored
]


class TestPageMapping:
    @pytest.mark.parametrize("url,expected", PAGE_CASES)
    def test_page_url(self, url, expected):
        assert _map_cio_event(_page(url)) == expected


class TestClickMapping:
    @pytest.mark.parametrize("url,expected", CLICK_CASES)
    def test_click_url(self, url, expected):
        assert _map_cio_event(_click(url)) == expected


class TestPrecedence:
    """Overlapping paths must resolve to the higher-intent classification."""

    def test_kosten_termine_is_price_not_offer(self):
        # kosten-termine lives in BOTH PRICE and SALES sets — price wins.
        assert _classify_funnel_url("https://sbc.de/kosten-termine") == "price_info_viewed"

    def test_masterclass_is_replay_not_offer(self):
        # masterclass lives in BOTH REPLAY and SALES sets — replay wins.
        assert _classify_funnel_url("https://sbc.de/masterclass") == "video_watched_50"

    def test_payment_beats_everything(self):
        assert _classify_funnel_url("https://sbc.de/masterclass/payment") == "checkout_visited"


class TestTaxonomyInvariants:
    """Constants are the canonical Tracking-Crew paths; outputs stay scoreable."""

    def test_canonical_constants(self):
        assert CHECKOUT_URL_PATTERNS == ("/payment", "inner-journey-payment", "bookinea.app")
        assert PRICE_INFO_PATTERNS == ("kosten-termine",)
        assert EIGNUNGSCHECK_PATTERNS == ("/eignungscheck", "/onsite/eignungscheck/")
        assert OPTIN_URL_PATTERNS == ("/optin", "/optin-thx")
        assert "/offer" in SALES_PAGE_PATTERNS and "/grundausbildung/" in SALES_PAGE_PATTERNS
        for tag in ("basisseminar", "masterclass", "live-workshop", "day-1", "day-4"):
            assert tag in REPLAY_URL_PATTERNS

    def test_every_classified_type_is_scoreable(self):
        # Whatever _classify_funnel_url can return must be a real scoreable type
        # (or None), otherwise engagement.calculate_engagement_score drops it.
        produced = set()
        for url, _ in PAGE_CASES + CLICK_CASES:
            for ev in (_page(url), _click(url)):
                et = _map_cio_event(ev)
                if et is not None:
                    produced.add(et)
        assert produced.issubset(set(BASE_POINTS)), produced - set(BASE_POINTS)
