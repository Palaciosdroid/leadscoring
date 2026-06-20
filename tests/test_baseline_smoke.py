"""Smoke tests for analytics/baseline.py — runs the full report path against
small in-memory fixtures, no live HubSpot/Whyros calls.

Covers the three baseline outputs (cohort conversion, signal lift, mapping
coverage) plus fail-soft behavior on empty/partial data.
"""

from datetime import datetime, timezone

from analytics.baseline import (
    cohort_month,
    is_mature,
    classify_signals,
    coverage_for_events,
    build_report,
    format_report,
    BaselineReport,
    SIGNALS,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

# A fixed "now" so maturity flags are deterministic regardless of run date.
REF = datetime(2026, 6, 20, tzinfo=timezone.utc)


def _contact(cid, email, vid, created_at):
    return {"id": cid, "email": email, "visitor_id": vid, "created_at": created_at}


# 4 contacts across 3 cohorts; c1 won (HubSpot), c2 converted via purchase email,
# c3/c4 not converted. c4 is in an immature (recent) cohort.
CONTACTS = [
    _contact("c1", "a@x.de", "v1", "2026-01-10T08:00:00Z"),
    _contact("c2", "b@x.de", "v2", "2026-01-20T08:00:00Z"),
    _contact("c3", "c@x.de", "v3", "2026-03-05T08:00:00Z"),
    _contact("c4", "d@x.de", "v4", "2026-06-15T08:00:00Z"),
]

WON_SET = {"c1"}                 # c1 won a HubSpot deal
COMPLETED_SET = {"b@x.de"}       # c2 has a completed purchase

# Events per visitor: mix of classified funnel pages, generic pages, and signals.
EVENTS_BY_VISITOR = {
    # c1 (converted): form_submit + price page + a classified checkout page + generic
    "v1": [
        {"event_type": "form_submit", "page_url": "https://sbc.de/eignungscheck"},
        {"event_type": "pageview", "page_url": "https://sbc.de/kosten-termine"},
        {"event_type": "pageview", "page_url": "https://sbc.de/grundausbildung/payment"},
        {"event_type": "pageview", "page_url": "https://sbc.de/blog/artikel"},
    ],
    # c2 (converted): video_complete + replay page
    "v2": [
        {"event_type": "video_complete", "page_url": "https://sbc.de/masterclass/day-1"},
        {"event_type": "pageview", "page_url": "https://sbc.de/masterclass"},
    ],
    # c3 (not converted): only generic pages — no signals, 0% classified
    "v3": [
        {"event_type": "pageview", "page_url": "https://sbc.de/"},
        {"event_type": "pageview", "page_url": "https://sbc.de/optin"},
    ],
    # c4 (not converted, immature cohort): price page only
    "v4": [
        {"event_type": "pageview", "page_url": "https://sbc.de/kosten-termine"},
    ],
}


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


class TestCohortMonth:
    def test_z_suffix(self):
        assert cohort_month("2026-01-10T08:00:00Z") == "2026-01"

    def test_offset(self):
        assert cohort_month("2026-03-05T08:00:00+00:00") == "2026-03"

    def test_none(self):
        assert cohort_month(None) is None

    def test_empty(self):
        assert cohort_month("   ") is None

    def test_date_only_fallback(self):
        assert cohort_month("2026-11") == "2026-11"


class TestMaturity:
    def test_old_cohort_mature(self):
        assert is_mature("2026-01", REF) is True

    def test_recent_cohort_immature(self):
        # 2026-06 vs ref 2026-06 → 0 months → immature
        assert is_mature("2026-06", REF) is False


class TestClassifySignals:
    def test_form_submit(self):
        evs = [{"event_type": "form_submit", "page_url": ""}]
        assert "form_submit" in classify_signals(evs)

    def test_video_complete(self):
        evs = [{"event_type": "video_complete", "page_url": ""}]
        assert "video_complete" in classify_signals(evs)

    def test_price_and_eignungscheck_and_replay_from_url(self):
        evs = [
            {"event_type": "pageview", "page_url": "https://sbc.de/kosten-termine"},
            {"event_type": "pageview", "page_url": "https://sbc.de/eignungscheck"},
            {"event_type": "pageview", "page_url": "https://sbc.de/masterclass/day-2"},
        ]
        fired = classify_signals(evs)
        assert {"price", "eignungscheck", "replay"} <= fired

    def test_no_events_no_signals(self):
        assert classify_signals([]) == set()

    def test_signals_are_subset_of_known(self):
        fired = classify_signals(EVENTS_BY_VISITOR["v1"])
        assert fired <= set(SIGNALS)


class TestCoverage:
    def test_classified_vs_generic(self):
        cls, tot = coverage_for_events(EVENTS_BY_VISITOR["v1"])
        # 3 pageviews counted (form_submit excluded); price+payment classified,
        # blog generic → 2/3 classified.
        assert tot == 3
        assert cls == 2

    def test_all_generic(self):
        cls, tot = coverage_for_events(EVENTS_BY_VISITOR["v3"])
        # both generic (/, /optin → page_visited) → 0 classified, 2 total
        assert (cls, tot) == (0, 2)

    def test_no_page_events(self):
        cls, tot = coverage_for_events(
            [{"event_type": "form_submit", "page_url": "x"}]
        )
        assert (cls, tot) == (0, 0)


# ---------------------------------------------------------------------------
# Full report assembly
# ---------------------------------------------------------------------------


class TestBuildReport:
    def _report(self):
        return build_report(
            CONTACTS, EVENTS_BY_VISITOR, WON_SET, COMPLETED_SET, ref=REF
        )

    def test_does_not_crash(self):
        r = self._report()
        assert isinstance(r, BaselineReport)

    def test_overall_conversion(self):
        r = self._report()
        assert r.contacts_total == 4
        assert r.contacts_converted == 2  # c1 (won) + c2 (purchase)

    def test_cohorts_not_pooled(self):
        r = self._report()
        by = {c.cohort: c for c in r.cohorts}
        # 3 distinct cohorts, kept separate (not pooled)
        assert set(by) == {"2026-01", "2026-03", "2026-06"}
        # 2026-01 has both converters
        assert by["2026-01"].total == 2 and by["2026-01"].converted == 2
        # 2026-03 has c3, not converted
        assert by["2026-03"].converted == 0

    def test_recent_cohort_flagged_immature(self):
        r = self._report()
        by = {c.cohort: c for c in r.cohorts}
        assert by["2026-06"].mature is False
        assert by["2026-01"].mature is True

    def test_signal_lift_present(self):
        r = self._report()
        sig = {s.signal: s for s in r.signals}
        assert set(sig) == set(SIGNALS)
        # form_submit fired only for c1 (converted) → with-rate 100%
        assert sig["form_submit"].with_signal == 1
        assert sig["form_submit"].with_signal_converted == 1
        assert sig["form_submit"].rate_with == 1.0

    def test_coverage_aggregated(self):
        r = self._report()
        # v1: 2/3, v2: classified masterclass replay page (1/1), v3: 0/2, v4: 1/1
        # total page/click events = 3 + 1 + 2 + 1 = 7; classified = 2+1+0+1 = 4
        assert r.coverage_total == 7
        assert r.coverage_classified == 4
        assert 0 < r.coverage_pct <= 100

    def test_format_runs(self):
        r = self._report()
        text = format_report(r)
        assert "COHORT BASELINE" in text
        assert "SIGNAL LIFT" in text
        assert "MAPPING COVERAGE" in text


# ---------------------------------------------------------------------------
# Fail-soft
# ---------------------------------------------------------------------------


class TestFailSoft:
    def test_empty_everything(self):
        r = build_report([], {}, set(), set(), ref=REF)
        assert r.contacts_total == 0
        assert r.coverage_total == 0
        # produces notes, does not crash, formats cleanly
        assert r.notes
        assert "COHORT BASELINE" in format_report(r)

    def test_contacts_but_no_label(self):
        r = build_report(CONTACTS, EVENTS_BY_VISITOR, set(), set(), ref=REF)
        assert r.contacts_total == 4
        assert r.contacts_converted == 0   # no label data → nobody converted
        assert any("label" in n.lower() for n in r.notes)

    def test_contacts_but_no_events(self):
        r = build_report(CONTACTS, {}, WON_SET, COMPLETED_SET, ref=REF)
        # cohort + conversion still work; coverage just empty
        assert r.contacts_converted == 2
        assert r.coverage_total == 0
        for s in r.signals:
            assert s.with_signal == 0
