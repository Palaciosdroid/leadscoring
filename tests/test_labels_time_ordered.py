"""A conversion label has to know which came first.

REGRESSION (calibration run 19.08.2026): `calibrate_posthog_signals` asked "does
this contact have a signal, and did they ever buy?" and treated any overlap as a
hit. Measured on the same population: of the 26 converted contacts whose purchase
date could be resolved, 22 bought BEFORE the signal — median 456 days before. Those
are past customers revisiting the offer page. They landed in every bucket alike,
lifted all rates to a narrow 9.2-13.5% band, and inverted the ranking so that
vsl>=90% scored WORSE than vsl<50%.

The sync only excludes buyers with a PostHog purchase in the last 60 days, so a
deal won two years ago stays in the population.

`is_converted` stays exactly as it is — baseline.py and calibrate_points.py depend
on the "ever bought" question, which is the right one for them. This adds the
time-ordered question alongside it.

Three outcomes, not two. A past customer is NOT a negative example: they already
bought and are no longer in the market, so counting them as "did not convert"
would understate every signal. They leave the population instead.
"""

from datetime import datetime, timezone, timedelta

import pytest

from analytics.labels import ConversionTiming, converted_after

T0 = datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc)          # the signal anchor
BEFORE = T0 - timedelta(days=456)                               # the real-world median
AFTER = T0 + timedelta(days=14)


class TestOrdering:

    def test_bought_after_the_signal_is_a_real_conversion(self):
        assert converted_after("123", "a@x.de", {"123": AFTER}, {}, T0) is ConversionTiming.CONVERTED_AFTER

    def test_bought_before_the_signal_is_a_prior_buyer(self):
        assert converted_after("123", "a@x.de", {"123": BEFORE}, {}, T0) is ConversionTiming.PRIOR_BUYER

    def test_never_bought_is_not_converted(self):
        assert converted_after("123", "a@x.de", {}, {}, T0) is ConversionTiming.NOT_CONVERTED

    def test_purchase_exactly_at_the_anchor_counts_as_prior(self):
        """Same timestamp cannot show the signal caused it — do not claim a hit."""
        assert converted_after("123", "a@x.de", {"123": T0}, {}, T0) is ConversionTiming.PRIOR_BUYER


class TestBothSources:

    def test_whyros_purchase_after_counts(self):
        assert converted_after("123", "a@x.de", {}, {"a@x.de": AFTER}, T0) is ConversionTiming.CONVERTED_AFTER

    def test_one_after_wins_over_one_before(self):
        """A repeat customer who bought again after the signal did convert again."""
        got = converted_after("123", "a@x.de", {"123": BEFORE}, {"a@x.de": AFTER}, T0)
        assert got is ConversionTiming.CONVERTED_AFTER

    def test_both_before_stays_prior(self):
        got = converted_after("123", "a@x.de", {"123": BEFORE}, {"a@x.de": BEFORE}, T0)
        assert got is ConversionTiming.PRIOR_BUYER


class TestInputHandling:
    """Same tolerances as is_converted — these inputs come straight from HubSpot."""

    def test_email_is_case_insensitive_and_trimmed(self):
        assert converted_after("9", "  A@X.de ", {}, {"a@x.de": AFTER}, T0) is ConversionTiming.CONVERTED_AFTER

    def test_contact_id_coerced_to_str(self):
        assert converted_after(123, None, {"123": AFTER}, {}, T0) is ConversionTiming.CONVERTED_AFTER

    def test_missing_email_falls_back_to_contact_id(self):
        assert converted_after("123", None, {"123": AFTER}, {}, T0) is ConversionTiming.CONVERTED_AFTER

    def test_naive_purchase_datetime_is_treated_as_utc(self):
        """HubSpot and Supabase disagree about tzinfo; a crash here is not acceptable."""
        naive = AFTER.replace(tzinfo=None)
        assert converted_after("123", None, {"123": naive}, {}, T0) is ConversionTiming.CONVERTED_AFTER


class TestNoAnchor:

    def test_without_an_anchor_the_question_cannot_be_answered(self):
        """No anchor means no ordering. Never guess — the caller must drop the row."""
        assert converted_after("123", "a@x.de", {"123": AFTER}, {}, None) is ConversionTiming.UNDECIDABLE

    def test_undecidable_even_when_there_is_no_purchase(self):
        assert converted_after("123", "a@x.de", {}, {}, None) is ConversionTiming.UNDECIDABLE


class TestPopulationRules:
    """The three outcomes must stay distinguishable — that is the whole point."""

    def test_all_four_outcomes_are_distinct(self):
        assert len({
            ConversionTiming.CONVERTED_AFTER,
            ConversionTiming.PRIOR_BUYER,
            ConversionTiming.NOT_CONVERTED,
            ConversionTiming.UNDECIDABLE,
        }) == 4

    def test_prior_buyer_is_not_a_negative_example(self):
        """Guards the intent: PRIOR_BUYER must never be conflated with NOT_CONVERTED."""
        assert ConversionTiming.PRIOR_BUYER is not ConversionTiming.NOT_CONVERTED
