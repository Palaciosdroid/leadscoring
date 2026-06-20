"""Tests for the canonical conversion label in analytics/labels.py.

Only the pure is_converted() is unit-tested here — fetch_won_contacts() and
fetch_completed_purchase_emails() hit HubSpot/Whyros and are covered by the
baseline smoke test against fixtures.
"""

from analytics.labels import (
    WON_DEAL_PIPELINE_ID,
    WON_DEAL_STAGE_ID,
    is_converted,
)


class TestIsConverted:
    """The canonical label: Won deal (by contact id) OR completed purchase (by email)."""

    def test_won_by_contact_id(self):
        assert is_converted("123", "a@x.de", {"123"}, set()) is True

    def test_converted_by_completed_email(self):
        assert is_converted("999", "a@x.de", set(), {"a@x.de"}) is True

    def test_both_sources_match(self):
        assert is_converted("123", "a@x.de", {"123"}, {"a@x.de"}) is True

    def test_not_converted(self):
        assert is_converted("123", "a@x.de", {"456"}, {"b@x.de"}) is False

    def test_empty_sets(self):
        assert is_converted("123", "a@x.de", set(), set()) is False

    # --- Normalization / robustness ---

    def test_email_match_is_case_insensitive(self):
        assert is_converted("123", "A@X.de", set(), {"a@x.de"}) is True

    def test_email_match_strips_whitespace(self):
        assert is_converted("123", "  a@x.de  ", set(), {"a@x.de"}) is True

    def test_contact_id_coerced_to_str(self):
        # won_set holds strings; an int cid must still match
        assert is_converted(123, "a@x.de", {"123"}, set()) is True

    def test_missing_email_falls_back_to_contact_id(self):
        assert is_converted("123", None, {"123"}, set()) is True

    def test_missing_email_no_won_is_false(self):
        assert is_converted("123", None, set(), {"a@x.de"}) is False

    def test_empty_contact_id_uses_email(self):
        assert is_converted("", "a@x.de", set(), {"a@x.de"}) is True


class TestCanonicalConstants:
    """Verified IDs from the Vertrieb pipeline — must not drift."""

    def test_pipeline_id(self):
        assert WON_DEAL_PIPELINE_ID == "168455110"

    def test_stage_id(self):
        assert WON_DEAL_STAGE_ID == "311698367"
