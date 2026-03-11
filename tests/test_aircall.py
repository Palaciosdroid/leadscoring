"""Tests for Aircall integration (unit tests with mocked HTTP)."""

from datetime import datetime, timezone, timedelta
from unittest.mock import patch, AsyncMock

import pytest

from integrations.aircall import _is_fresh, _select_list


class TestIsFresh:
    def test_fresh_lead(self):
        created = datetime.now(timezone.utc) - timedelta(hours=2)
        assert _is_fresh(created) is True

    def test_old_lead(self):
        created = datetime.now(timezone.utc) - timedelta(hours=30)
        assert _is_fresh(created) is False

    def test_none_not_fresh(self):
        assert _is_fresh(None) is False

    def test_exactly_24h_not_fresh(self):
        created = datetime.now(timezone.utc) - timedelta(hours=24)
        assert _is_fresh(created) is False


class TestSelectList:
    @patch("integrations.aircall.DIALER_LIST_FRESH", "list-fresh")
    @patch("integrations.aircall.DIALER_LIST_WARM", "list-warm")
    def test_fresh_lead_goes_to_fresh_list(self):
        created = datetime.now(timezone.utc) - timedelta(hours=1)
        assert _select_list(30, created) == "list-fresh"  # fresh trumps low score

    @patch("integrations.aircall.DIALER_LIST_FRESH", "list-fresh")
    @patch("integrations.aircall.DIALER_LIST_WARM", "list-warm")
    def test_warm_score_goes_to_warm_list(self):
        created = datetime.now(timezone.utc) - timedelta(days=3)
        assert _select_list(60, created) == "list-warm"

    @patch("integrations.aircall.DIALER_LIST_FRESH", "list-fresh")
    @patch("integrations.aircall.DIALER_LIST_WARM", "list-warm")
    def test_low_score_old_lead_returns_none(self):
        created = datetime.now(timezone.utc) - timedelta(days=3)
        assert _select_list(30, created) is None

    @patch("integrations.aircall.DIALER_LIST_FRESH", "")
    @patch("integrations.aircall.DIALER_LIST_WARM", "list-warm")
    def test_fresh_but_no_fresh_list_falls_to_warm(self):
        created = datetime.now(timezone.utc) - timedelta(hours=1)
        # Fresh list not configured, score < 50 → None (no fallback)
        assert _select_list(30, created) is None

    @patch("integrations.aircall.DIALER_LIST_FRESH", "")
    @patch("integrations.aircall.DIALER_LIST_WARM", "list-warm")
    def test_fresh_high_score_uses_warm_list(self):
        created = datetime.now(timezone.utc) - timedelta(hours=1)
        # Fresh list not configured, but score ≥ 50 → warm list
        assert _select_list(55, created) == "list-warm"
