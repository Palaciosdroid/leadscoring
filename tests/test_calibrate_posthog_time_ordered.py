"""The signal calibration must only count purchases that came after the signal.

REGRESSION (run 19.08.2026): the report asked "has a signal AND ever bought" and
called any overlap a conversion. On the live population, 22 of 26 resolvable
conversions had bought BEFORE the signal, a median of 456 days before — past
customers revisiting the offer page. They appeared in every bucket alike, lifted
all six rates into a 9.2-13.5% band, and inverted the ranking so vsl>=90% scored
worse than vsl<50%. The report looked healthy precisely because every bucket sat
above the reference base rate.

Two rules encoded here:

1. Each bucket is judged against ITS OWN anchor. A dwell signal can only be
   credited with a purchase that followed the dwell, not one that followed some
   other signal on the same contact.
2. Past customers leave the population instead of counting as failures. They
   already bought and are out of the market; scoring them as "did not convert"
   would understate every signal — the opposite error, equally wrong.
"""

from datetime import datetime, timezone, timedelta

from analytics.calibrate_posthog_signals import build_report

T = datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc)
LATER = (T + timedelta(days=10)).isoformat()
EARLIER = (T - timedelta(days=456)).isoformat()
ANCHOR = T.isoformat()


def contact(cid, email, **props):
    base = {"email": email}
    base.update(props)
    return {"id": cid, "properties": base}


class TestPastCustomersLeaveThePopulation:

    def test_prior_buyer_is_not_counted_as_a_failure(self):
        c = contact("1", "a@x.de", offer_dwell_minutes="9", offer_dwell_last_at=ANCHOR)
        r = build_report([c], {"1": datetime.fromisoformat(EARLIER)}, {})
        assert r.contacts_total == 0, "past customer must leave the population entirely"
        assert r.contacts_converted == 0
        assert r.prior_buyers_excluded == 1

    def test_purchase_after_the_signal_counts(self):
        c = contact("1", "a@x.de", offer_dwell_minutes="9", offer_dwell_last_at=ANCHOR)
        r = build_report([c], {"1": datetime.fromisoformat(LATER)}, {})
        assert r.contacts_total == 1
        assert r.contacts_converted == 1
        assert r.prior_buyers_excluded == 0

    def test_no_purchase_stays_in_as_a_genuine_negative(self):
        c = contact("1", "a@x.de", offer_dwell_minutes="9", offer_dwell_last_at=ANCHOR)
        r = build_report([c], {}, {})
        assert r.contacts_total == 1
        assert r.contacts_converted == 0

    def test_excluding_past_customers_changes_the_rate(self):
        """The whole point: one buyer of each kind must not read as 50%."""
        cs = [
            contact("1", "a@x.de", offer_dwell_minutes="9", offer_dwell_last_at=ANCHOR),
            contact("2", "b@x.de", offer_dwell_minutes="9", offer_dwell_last_at=ANCHOR),
        ]
        r = build_report(cs, {"1": datetime.fromisoformat(EARLIER)}, {})
        assert r.contacts_total == 1 and r.contacts_converted == 0


class TestEachBucketUsesItsOwnAnchor:

    def test_dwell_bucket_ignores_a_purchase_that_preceded_the_dwell(self):
        c = contact("1", "a@x.de",
                    offer_dwell_minutes="9", offer_dwell_last_at=LATER,
                    vsl_watched_percent="95", vsl_watched_last_at=EARLIER)
        # Purchase sits between the vsl signal and the dwell signal.
        r = build_report([c], {"1": T}, {})
        by = {b.label: b for b in r.buckets}
        dwell = next(b for lbl, b in by.items() if lbl.startswith("offer_dwell >="))
        vsl = next(b for lbl, b in by.items() if lbl.startswith("vsl >="))
        assert dwell.converted == 0, "dwell came after the purchase — cannot claim it"
        assert vsl.converted == 1, "vsl came before the purchase — may claim it"

    def test_payment_bucket_uses_the_payment_date_as_its_own_anchor(self):
        c = contact("1", "a@x.de", payment_page_visited=ANCHOR)
        r = build_report([c], {"1": datetime.fromisoformat(LATER)}, {})
        pay = next(b for b in r.buckets if b.label.startswith("payment_page_visited"))
        assert pay.total == 1 and pay.converted == 1


class TestUndecidable:

    def test_signal_without_an_anchor_is_reported_not_guessed(self):
        """A value with no anchor cannot be ordered. Drop it, and say how many."""
        c = contact("1", "a@x.de", offer_dwell_minutes="9")  # no offer_dwell_last_at
        r = build_report([c], {"1": datetime.fromisoformat(LATER)}, {})
        assert r.undecidable_excluded == 1
        assert r.contacts_total == 0

    def test_undecidable_is_counted_separately_from_prior_buyers(self):
        cs = [
            contact("1", "a@x.de", offer_dwell_minutes="9"),
            contact("2", "b@x.de", offer_dwell_minutes="9", offer_dwell_last_at=ANCHOR),
        ]
        r = build_report(cs, {"2": datetime.fromisoformat(EARLIER)}, {})
        assert r.undecidable_excluded == 1
        assert r.prior_buyers_excluded == 1


class TestWhyrosSource:

    def test_whyros_purchase_after_signal_counts(self):
        c = contact("1", "a@x.de", offer_dwell_minutes="9", offer_dwell_last_at=ANCHOR)
        r = build_report([c], {}, {"a@x.de": datetime.fromisoformat(LATER)})
        assert r.contacts_converted == 1

    def test_whyros_purchase_before_signal_excludes(self):
        c = contact("1", "a@x.de", offer_dwell_minutes="9", offer_dwell_last_at=ANCHOR)
        r = build_report([c], {}, {"a@x.de": datetime.fromisoformat(EARLIER)})
        assert r.prior_buyers_excluded == 1


class TestEmptySources:

    def test_both_label_sources_empty_is_flagged_as_a_fetch_failure(self):
        c = contact("1", "a@x.de", offer_dwell_minutes="9", offer_dwell_last_at=ANCHOR)
        r = build_report([c], {}, {})
        assert any("Fetch-Fehler" in n for n in r.notes)
