"""Smoke tests for analytics/calibrate_points.py — runs the full calibration
path against small in-memory fixtures, no live HubSpot/Whyros/Tally calls.

Covers the three calibration outputs (points-bucket close-rate table, threshold
recommendation, closes-concentration) plus signal assembly, disqualify handling,
and fail-soft behavior on empty/partial data.
"""

from analytics.calibrate_points import (
    BUCKET_EDGES,
    bucket_for_points,
    assemble_signals,
    recommend_thresholds,
    build_report,
    format_report,
    BucketStat,
    CalibrationReport,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _contact(cid, email, vid, **props):
    base = {"id": cid, "email": email, "visitor_id": vid}
    base.update(props)
    return base


# 4 contacts:
#   c1 — Tally budget 4000-6000 (+30) + interest naechster_schritt (+25) +
#        consult (+15) + a checkout pageview (+25) = 95 → top bucket, WON.
#   c2 — interest grundsaetzlich (+10) + price page (+15) = 25 bucket, WON (purchase).
#   c3 — form_submit only (+10) = 10 bucket, not converted.
#   c4 — interest keines → DISQUALIFIED, not converted.
CONTACTS = [
    _contact(
        "c1", "a@x.de", "v1",
        lead_eig_budget="4000_6000",
        lead_eig_interest="naechster_schritt",
        lead_eig_consult=True,
    ),
    _contact(
        "c2", "b@x.de", "v2",
        lead_eig_interest="grundsaetzlich",
    ),
    _contact("c3", "c@x.de", "v3"),
    _contact("c4", "d@x.de", "v4", lead_eig_interest="keines"),
]

WON_SET = {"c1"}                 # c1 won a HubSpot deal
COMPLETED_SET = {"b@x.de"}       # c2 has a completed purchase

# Touchpoints keyed by contact_id (direct form_submit → application_submitted).
TOUCHPOINTS_BY_CONTACT = {
    # c3: a direct form submission → form_submit signal (+10)
    "c3": [
        {"channel": "direct", "source": "web", "touchpoint_type": "form_submit",
         "created_at": "2026-01-10T08:00:00Z"},
    ],
}

# Browser events keyed by visitor_id.
EVENTS_BY_VISITOR = {
    # c1: checkout page visit → checkout_visited (+25)
    "v1": [
        {"event_type": "pageview", "page_url": "https://sbc.de/grundausbildung/payment"},
    ],
    # c2: price page visit → price_info_viewed (+15)
    "v2": [
        {"event_type": "pageview", "page_url": "https://sbc.de/kosten-termine"},
    ],
}


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


class TestBucketForPoints:
    def test_zero(self):
        assert bucket_for_points(0) == 0

    def test_mid(self):
        # 27 → bucket whose lower edge is 25
        assert bucket_for_points(27) == 25

    def test_exact_edge(self):
        assert bucket_for_points(50) == 50

    def test_above_top(self):
        assert bucket_for_points(999) == BUCKET_EDGES[-1]

    def test_negative_clamps_to_floor(self):
        assert bucket_for_points(-5) == BUCKET_EDGES[0]


class TestAssembleSignals:
    def test_tally_props_flow_through(self):
        sig = assemble_signals([], [], CONTACTS[0], unsubscribed=False)
        assert sig["budget"] == "4000_6000"
        assert sig["interest"] == "naechster_schritt"
        assert sig["consult"] is True
        # Phone is NEVER a signal.
        assert "phone" not in sig

    def test_behavior_signal_from_browser_event(self):
        sig = assemble_signals([], EVENTS_BY_VISITOR["v1"], CONTACTS[0], unsubscribed=False)
        assert sig["checkout"] is True

    def test_form_submit_from_touchpoint(self):
        sig = assemble_signals(TOUCHPOINTS_BY_CONTACT["c3"], [], CONTACTS[2], unsubscribed=False)
        assert sig["form_submit"] is True

    def test_missing_everything_is_safe(self):
        sig = assemble_signals([], [], CONTACTS[2], unsubscribed=False)
        # No crash; behavior signals all falsey, tally props all None.
        assert sig["budget"] is None
        assert sig["form_submit"] is False


class TestRecommendThresholds:
    def test_empty(self):
        rec = recommend_thresholds([])
        assert rec.hot is None and rec.warm is None
        assert "no buckets" in rec.note

    def test_monotone_data_picks_thresholds(self):
        # High buckets close well, low buckets don't. Cumulative-from-top rates:
        #   0+  = (1+5+9)/300 = 5.0%   (whole population)
        #   25+ = (5+9)/200   = 7.0%
        #   50+ = 9/100       = 9.0%
        buckets = [
            BucketStat(lower=0, total=100, converted=1),
            BucketStat(lower=25, total=100, converted=5),
            BucketStat(lower=50, total=100, converted=9),
        ]
        rec = recommend_thresholds(buckets)
        # Hot target 8%: only 50+ clears → hot = 50.
        assert rec.hot == 50
        # Warm target 4%: the whole population (0+) already clears 4% → lowest
        # clearing edge is 0.
        assert rec.warm == 0

    def test_warm_threshold_lifts_when_base_below_target(self):
        # Base population below the 4% warm target; only higher bands clear.
        #   0+  = (0+2+9)/300 = 3.67%  (below 4% → 0 does NOT clear)
        #   25+ = (2+9)/200   = 5.5%   (clears warm)
        #   50+ = 9/100       = 9.0%   (clears hot)
        buckets = [
            BucketStat(lower=0, total=100, converted=0),
            BucketStat(lower=25, total=100, converted=2),
            BucketStat(lower=50, total=100, converted=9),
        ]
        rec = recommend_thresholds(buckets)
        assert rec.hot == 50
        assert rec.warm == 25

    def test_thin_buckets_skipped(self):
        buckets = [BucketStat(lower=50, total=5, converted=5)]  # 100% but n<MIN
        rec = recommend_thresholds(buckets)
        assert rec.hot is None and rec.warm is None


# ---------------------------------------------------------------------------
# Full report assembly
# ---------------------------------------------------------------------------


class TestBuildReport:
    def _report(self):
        return build_report(
            CONTACTS, TOUCHPOINTS_BY_CONTACT, EVENTS_BY_VISITOR, WON_SET, COMPLETED_SET,
        )

    def test_does_not_crash(self):
        assert isinstance(self._report(), CalibrationReport)

    def test_overall_conversion(self):
        r = self._report()
        assert r.contacts_total == 4
        assert r.contacts_converted == 2  # c1 (won) + c2 (purchase)

    def test_disqualified_bucketed_separately(self):
        r = self._report()
        # c4 (interest=keines) is disqualified, not in the point buckets.
        assert r.disqualified_total == 1
        assert r.disqualified_converted == 0
        # Sum of bucket totals = tierable population (3, excludes c4).
        assert sum(b.total for b in r.buckets) == 3

    def test_high_score_contact_lands_in_top_bucket(self):
        r = self._report()
        by = {b.lower: b for b in r.buckets}
        # c1 = 95 points → top bucket (80), converted.
        top = by[BUCKET_EDGES[-1]]
        assert top.total == 1
        assert top.converted == 1

    def test_low_score_contact_in_low_bucket(self):
        r = self._report()
        by = {b.lower: b for b in r.buckets}
        # c3 = form_submit only = 10 points → bucket 10, not converted.
        assert by[10].total == 1
        assert by[10].converted == 0

    def test_buckets_cover_all_edges(self):
        r = self._report()
        assert [b.lower for b in r.buckets] == list(BUCKET_EDGES)

    def test_format_runs(self):
        text = format_report(self._report())
        assert "POINT-SYSTEM CALIBRATION" in text
        assert "CLOSE-RATE" in text
        assert "THRESHOLD RECOMMENDATION" in text
        assert "CLOSES-CONCENTRATION" in text


# ---------------------------------------------------------------------------
# Fail-soft
# ---------------------------------------------------------------------------


class TestFailSoft:
    def test_empty_everything(self):
        r = build_report([], {}, {}, set(), set())
        assert r.contacts_total == 0
        assert r.notes  # produced fail-soft notes
        assert "POINT-SYSTEM CALIBRATION" in format_report(r)

    def test_contacts_but_no_label(self):
        r = build_report(CONTACTS, TOUCHPOINTS_BY_CONTACT, EVENTS_BY_VISITOR, set(), set())
        assert r.contacts_total == 4
        assert r.contacts_converted == 0   # no label data → nobody converted
        assert any("label" in n.lower() for n in r.notes)

    def test_contacts_but_no_behavior(self):
        # No touchpoints / events: contacts score on Tally props only, no crash.
        r = build_report(CONTACTS, {}, {}, WON_SET, COMPLETED_SET)
        assert r.contacts_total == 4
        assert r.contacts_converted == 2
        # c1 still scores high on Tally alone (30+25+15=70 → top-ish bucket).
        assert sum(b.converted for b in r.buckets) >= 1
