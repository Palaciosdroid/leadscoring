"""Tests for Aircall integration (unit tests with mocked HTTP)."""

from datetime import datetime, timezone, timedelta
from unittest.mock import patch, AsyncMock, MagicMock

import pytest
import httpx

from integrations.aircall import (
    _is_fresh,
    _should_dial,
    _build_tags,
    add_to_power_dialer,
)


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


class TestShouldDial:
    def test_fresh_lead_always_dials(self):
        created = datetime.now(timezone.utc) - timedelta(hours=1)
        assert _should_dial(30, created) is True  # fresh trumps low score

    def test_warm_score_dials(self):
        created = datetime.now(timezone.utc) - timedelta(days=3)
        assert _should_dial(60, created) is True

    def test_low_score_old_lead_skipped(self):
        created = datetime.now(timezone.utc) - timedelta(days=3)
        assert _should_dial(30, created) is False

    def test_exactly_50_dials(self):
        created = datetime.now(timezone.utc) - timedelta(days=3)
        assert _should_dial(50, created) is True

    def test_just_below_50_skipped(self):
        created = datetime.now(timezone.utc) - timedelta(days=3)
        assert _should_dial(49, created) is False


class TestBuildTags:
    def test_fresh_lead_tags(self):
        created = datetime.now(timezone.utc) - timedelta(hours=1)
        tags = _build_tags(75, created, "Coaching")
        assert "score-75" in tags
        assert "fresh" in tags
        assert "Coaching" in tags
        assert "warm" not in tags

    def test_warm_lead_tags(self):
        created = datetime.now(timezone.utc) - timedelta(days=3)
        tags = _build_tags(60, created, None)
        assert "score-60" in tags
        assert "warm" in tags
        assert "fresh" not in tags

    def test_no_interest_category(self):
        created = datetime.now(timezone.utc) - timedelta(days=3)
        tags = _build_tags(55, created, None)
        assert len(tags) == 2  # score + warm only


class TestAddToPowerDialer:
    """Integration-level tests with mocked HTTP."""

    LEAD = {
        "phone": "+4915112345678",
        "firstname": "Max",
        "lastname": "Muster",
        "email": "max@example.com",
        "notes": "Test lead",
    }

    @pytest.mark.asyncio
    @patch("integrations.aircall.AIRCALL_API_ID", "test-id")
    @patch("integrations.aircall.AIRCALL_API_TOKEN", "test-token")
    @patch("integrations.aircall.AIRCALL_CLOSER_USER_ID", "12345")
    async def test_low_score_returns_none(self):
        old = datetime.now(timezone.utc) - timedelta(days=3)
        result = await add_to_power_dialer(self.LEAD, score=30, created_at=old)
        assert result is None

    @pytest.mark.asyncio
    @patch("integrations.aircall.AIRCALL_API_ID", "test-id")
    @patch("integrations.aircall.AIRCALL_API_TOKEN", "test-token")
    @patch("integrations.aircall.AIRCALL_CLOSER_USER_ID", "12345")
    @patch("integrations.aircall._upsert_contact", new_callable=AsyncMock, return_value="c-99")
    @patch("integrations.aircall._push_to_dialer_campaign", new_callable=AsyncMock, return_value={"status": "added", "phone": "+4915112345678"})
    async def test_warm_lead_pushes_to_dialer(self, mock_push, mock_upsert):
        old = datetime.now(timezone.utc) - timedelta(days=3)
        result = await add_to_power_dialer(self.LEAD, score=70, created_at=old, interest_category="Coaching")
        assert result is not None
        assert result["status"] == "added"
        mock_upsert.assert_called_once()
        mock_push.assert_called_once()

    @pytest.mark.asyncio
    @patch("integrations.aircall.AIRCALL_API_ID", "test-id")
    @patch("integrations.aircall.AIRCALL_API_TOKEN", "test-token")
    @patch("integrations.aircall.AIRCALL_CLOSER_USER_ID", "12345")
    @patch("integrations.aircall._upsert_contact", new_callable=AsyncMock, return_value="c-100")
    @patch("integrations.aircall._push_to_dialer_campaign", new_callable=AsyncMock, return_value={"status": "added", "phone": "+4915112345678"})
    async def test_fresh_lead_bypasses_score(self, mock_push, mock_upsert):
        fresh = datetime.now(timezone.utc) - timedelta(hours=1)
        result = await add_to_power_dialer(self.LEAD, score=20, created_at=fresh)
        assert result is not None
        mock_upsert.assert_called_once()
        # Verify tags contain 'fresh'
        call_kwargs = mock_upsert.call_args
        tags = call_kwargs.kwargs.get("tags", [])
        assert "fresh" in tags

    @pytest.mark.asyncio
    @patch("integrations.aircall.AIRCALL_API_ID", "")
    @patch("integrations.aircall.AIRCALL_API_TOKEN", "")
    async def test_missing_credentials_raises(self):
        with pytest.raises(EnvironmentError, match="AIRCALL_API_ID"):
            await add_to_power_dialer(self.LEAD, score=80)

    @pytest.mark.asyncio
    @patch("integrations.aircall.AIRCALL_API_ID", "test-id")
    @patch("integrations.aircall.AIRCALL_API_TOKEN", "test-token")
    @patch("integrations.aircall.AIRCALL_CLOSER_USER_ID", "")
    async def test_missing_closer_id_raises(self):
        fresh = datetime.now(timezone.utc) - timedelta(hours=1)
        with pytest.raises(EnvironmentError, match="AIRCALL_CLOSER_USER_ID"):
            await add_to_power_dialer(self.LEAD, score=80, created_at=fresh)
