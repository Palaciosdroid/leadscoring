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
        """Fresh leads (< 24h) dial regardless of score or tier."""
        created = datetime.now(timezone.utc) - timedelta(hours=1)
        assert _should_dial(0, created, lead_tier="3_cold") is True

    def test_hot_tier_dials(self):
        """Hot tier (1_hot) qualifies for the Power Dialer."""
        old = datetime.now(timezone.utc) - timedelta(days=3)
        assert _should_dial(30, old, lead_tier="1_hot") is True

    def test_warm_tier_dials(self):
        """Warm tier (2_warm) with score >= 30 qualifies for the Power Dialer."""
        old = datetime.now(timezone.utc) - timedelta(days=3)
        assert _should_dial(40, old, lead_tier="2_warm") is True

    def test_warm_tier_low_score_skipped(self):
        """Warm tier (2_warm) with score < 30 does NOT qualify (TASK B)."""
        old = datetime.now(timezone.utc) - timedelta(days=3)
        assert _should_dial(20, old, lead_tier="2_warm") is False

    def test_booked_tier_skipped(self):
        """Booked tier (0_booked) never dials — lead already has a meeting."""
        fresh = datetime.now(timezone.utc) - timedelta(hours=1)
        assert _should_dial(90, fresh, lead_tier="0_booked") is False

    def test_cold_tier_skipped(self):
        """Cold tier (3_cold) does NOT qualify — CIO nurturing only."""
        old = datetime.now(timezone.utc) - timedelta(days=3)
        assert _should_dial(10, old, lead_tier="3_cold") is False

    def test_disqualified_tier_skipped(self):
        """Disqualified tier (4_disqualified) does NOT qualify."""
        old = datetime.now(timezone.utc) - timedelta(days=3)
        assert _should_dial(-10, old, lead_tier="4_disqualified") is False

    def test_empty_tier_not_fresh_skipped(self):
        """No tier + not fresh → skip."""
        old = datetime.now(timezone.utc) - timedelta(days=3)
        assert _should_dial(50, old, lead_tier="") is False

    def test_is_fresh_flag_overrides_created_at_none(self):
        """is_fresh=True bypasses created_at=None — scorer's 7-day freshness is accepted."""
        assert _should_dial(15, None, lead_tier="3_cold", is_fresh=True) is True

    def test_is_fresh_flag_blocked_for_booked(self):
        """is_fresh=True still cannot override booked tier."""
        assert _should_dial(15, None, lead_tier="0_booked", is_fresh=True) is False


class TestBuildTags:
    def test_fresh_lead_tags_with_list_key(self):
        created = datetime.now(timezone.utc) - timedelta(hours=1)
        tags = _build_tags(75, created, "hypnose", list_key="hc-fresh")
        assert "score-75" in tags
        assert "hc-fresh" in tags
        assert "HC" in tags
        assert "priority-2-warm" in tags  # 60 <= 75 < 80

    def test_warm_lead_tags_with_list_key(self):
        created = datetime.now(timezone.utc) - timedelta(days=5)
        tags = _build_tags(60, created, "meditation", list_key="mc-warm")
        assert "score-60" in tags
        assert "mc-warm" in tags
        assert "MC" in tags
        assert "priority-2-warm" in tags  # 60 <= 60 < 80

    def test_no_interest_category(self):
        created = datetime.now(timezone.utc) - timedelta(days=5)
        tags = _build_tags(55, created, None)
        assert "score-55" in tags
        assert "priority-3-nurture" in tags  # 30 <= 55 < 60

    def test_no_interest_category_priority(self):
        """Score 55 without interest -> has priority-3-nurture tag."""
        created = datetime.now(timezone.utc) - timedelta(days=5)
        tags = _build_tags(55, created, None)
        assert "score-55" in tags
        assert "priority-3-nurture" in tags  # 30 <= 55 < 60

    def test_priority_1_hot_tag(self):
        """Score >= 80 gets priority-1-hot tag."""
        created = datetime.now(timezone.utc) - timedelta(days=5)
        tags = _build_tags(85, created, "hypnose", list_key="hc-warm")
        assert "priority-1-hot" in tags
        assert "score-85" in tags

    def test_priority_2_warm_tag(self):
        """Score 60-79 gets priority-2-warm tag."""
        created = datetime.now(timezone.utc) - timedelta(days=5)
        tags = _build_tags(65, created, "meditation")
        assert "priority-2-warm" in tags

    def test_priority_3_nurture_tag(self):
        """Score 30-59 gets priority-3-nurture tag."""
        created = datetime.now(timezone.utc) - timedelta(days=5)
        tags = _build_tags(45, created, "lifecoach")
        assert "priority-3-nurture" in tags

    def test_no_priority_tag_below_30(self):
        """Score < 30 gets no priority tag."""
        created = datetime.now(timezone.utc) - timedelta(days=5)
        tags = _build_tags(15, created, None)
        assert not any(t.startswith("priority-") for t in tags)


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
    async def test_cold_tier_returns_none(self):
        """Cold tier lead is not pushed to the Power Dialer."""
        old = datetime.now(timezone.utc) - timedelta(days=3)
        result = await add_to_power_dialer(self.LEAD, score=10, created_at=old, lead_tier="3_cold")
        assert result is None

    @pytest.mark.asyncio
    @patch("integrations.aircall.AIRCALL_API_ID", "test-id")
    @patch("integrations.aircall.AIRCALL_API_TOKEN", "test-token")
    @patch("integrations.aircall.AIRCALL_CLOSER_USER_ID", "12345")
    @patch("integrations.aircall._write_contact_note", new_callable=AsyncMock)
    @patch("integrations.aircall._upsert_contact", new_callable=AsyncMock, return_value="c-99")
    @patch("integrations.aircall._push_to_dialer_campaign", new_callable=AsyncMock, return_value={"status": "added", "phone": "+4915112345678"})
    async def test_warm_tier_pushes_to_dialer(self, mock_push, mock_upsert, mock_note):
        """Warm tier lead with score >= 30 gets pushed to the Power Dialer."""
        old = datetime.now(timezone.utc) - timedelta(days=5)
        result = await add_to_power_dialer(self.LEAD, score=40, created_at=old, interest_category="Coaching", lead_tier="2_warm")
        assert result is not None
        assert result["status"] == "added"
        mock_upsert.assert_called_once()
        mock_push.assert_called_once()

    @pytest.mark.asyncio
    @patch("integrations.aircall.AIRCALL_API_ID", "test-id")
    @patch("integrations.aircall.AIRCALL_API_TOKEN", "test-token")
    @patch("integrations.aircall.AIRCALL_CLOSER_USER_ID", "12345")
    @patch("integrations.aircall._write_contact_note", new_callable=AsyncMock)
    @patch("integrations.aircall._upsert_contact", new_callable=AsyncMock, return_value="c-100")
    @patch("integrations.aircall._push_to_dialer_campaign", new_callable=AsyncMock, return_value={"status": "added", "phone": "+4915112345678"})
    async def test_fresh_lead_bypasses_score(self, mock_push, mock_upsert, mock_note):
        fresh = datetime.now(timezone.utc) - timedelta(hours=1)
        result = await add_to_power_dialer(self.LEAD, score=20, created_at=fresh)
        assert result is not None
        mock_upsert.assert_called_once()

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


class TestUpsertContact:
    """Test _upsert_contact with mocked HTTP responses."""

    LEAD = TestAddToPowerDialer.LEAD

    @pytest.mark.asyncio
    @patch("integrations.aircall.AIRCALL_API_ID", "test-id")
    @patch("integrations.aircall.AIRCALL_API_TOKEN", "test-token")
    @patch("integrations.aircall._cleanup_stale_priority_tags", new_callable=AsyncMock)
    async def test_success_returns_contact_id(self, mock_cleanup):
        from integrations.aircall import _upsert_contact

        mock_response = MagicMock()
        mock_response.status_code = 201
        mock_response.json.return_value = {"contact": {"id": 12345}}

        mock_client = AsyncMock()
        with patch("integrations.aircall._aircall_request", return_value=mock_response):
            result = await _upsert_contact(mock_client, self.LEAD, tags=["score-75", "hc-fresh"])
            assert result == "12345"

    @pytest.mark.asyncio
    @patch("integrations.aircall.AIRCALL_API_ID", "test-id")
    @patch("integrations.aircall.AIRCALL_API_TOKEN", "test-token")
    async def test_no_phone_raises_value_error(self):
        from integrations.aircall import _upsert_contact

        lead_no_phone = {**self.LEAD, "phone": ""}
        mock_client = AsyncMock()
        with pytest.raises(ValueError, match="No phone number"):
            await _upsert_contact(mock_client, lead_no_phone, tags=[])

    @pytest.mark.asyncio
    @patch("integrations.aircall.AIRCALL_API_ID", "test-id")
    @patch("integrations.aircall.AIRCALL_API_TOKEN", "test-token")
    async def test_http_error_raises(self):
        from integrations.aircall import _upsert_contact

        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "Server Error", request=MagicMock(), response=mock_response
        )

        mock_client = AsyncMock()
        with patch("integrations.aircall._aircall_request", return_value=mock_response):
            with pytest.raises(httpx.HTTPStatusError):
                await _upsert_contact(mock_client, self.LEAD, tags=[])

    @pytest.mark.asyncio
    @patch("integrations.aircall.AIRCALL_API_ID", "test-id")
    @patch("integrations.aircall.AIRCALL_API_TOKEN", "test-token")
    async def test_no_tags_omits_tags_key(self):
        from integrations.aircall import _upsert_contact

        mock_response = MagicMock()
        mock_response.status_code = 201
        mock_response.json.return_value = {"contact": {"id": 999}}

        mock_client = AsyncMock()
        with patch("integrations.aircall._aircall_request", return_value=mock_response):
            await _upsert_contact(mock_client, self.LEAD, tags=None)


class TestPushToDialerCampaign:
    """Test _push_to_dialer_campaign with mocked HTTP responses."""

    LEAD = TestAddToPowerDialer.LEAD

    @pytest.mark.asyncio
    @patch("integrations.aircall.AIRCALL_CLOSER_USER_ID", "1492144")
    @patch("integrations.aircall.AIRCALL_API_ID", "test-id")
    @patch("integrations.aircall.AIRCALL_API_TOKEN", "test-token")
    async def test_success_returns_added(self):
        from integrations.aircall import _push_to_dialer_campaign

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {}

        mock_client = AsyncMock()
        with patch("integrations.aircall._aircall_request", return_value=mock_response):
            result = await _push_to_dialer_campaign(mock_client, self.LEAD)
            assert result == {"status": "added", "phone": "+4915112345678"}

    @pytest.mark.asyncio
    @patch("integrations.aircall.AIRCALL_CLOSER_USER_ID", "1492144")
    @patch("integrations.aircall.AIRCALL_API_ID", "test-id")
    @patch("integrations.aircall.AIRCALL_API_TOKEN", "test-token")
    async def test_already_imported_422_returns_status(self):
        from integrations.aircall import _push_to_dialer_campaign

        mock_response = MagicMock()
        mock_response.status_code = 422
        mock_response.text = '{"error": "Phone number already imported in campaign"}'

        mock_client = AsyncMock()
        with patch("integrations.aircall._aircall_request", return_value=mock_response):
            result = await _push_to_dialer_campaign(mock_client, self.LEAD)
            assert result["status"] == "already_imported"

    @pytest.mark.asyncio
    @patch("integrations.aircall.AIRCALL_CLOSER_USER_ID", "1492144")
    @patch("integrations.aircall.AIRCALL_API_ID", "test-id")
    @patch("integrations.aircall.AIRCALL_API_TOKEN", "test-token")
    async def test_other_422_raises(self):
        from integrations.aircall import _push_to_dialer_campaign

        mock_response = MagicMock()
        mock_response.status_code = 422
        mock_response.text = '{"error": "Invalid phone number format"}'
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "Unprocessable", request=MagicMock(), response=mock_response
        )

        mock_client = AsyncMock()
        with patch("integrations.aircall._aircall_request", return_value=mock_response):
            with pytest.raises(httpx.HTTPStatusError):
                await _push_to_dialer_campaign(mock_client, self.LEAD)

    @pytest.mark.asyncio
    @patch("integrations.aircall.AIRCALL_CLOSER_USER_ID", "1492144")
    @patch("integrations.aircall.AIRCALL_API_ID", "test-id")
    @patch("integrations.aircall.AIRCALL_API_TOKEN", "test-token")
    async def test_server_error_raises(self):
        from integrations.aircall import _push_to_dialer_campaign

        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "Server Error", request=MagicMock(), response=mock_response
        )

        mock_client = AsyncMock()
        with patch("integrations.aircall._aircall_request", return_value=mock_response):
            with pytest.raises(httpx.HTTPStatusError):
                await _push_to_dialer_campaign(mock_client, self.LEAD)


class TestEndToEndFlow:
    """Full flow test: add_to_power_dialer with high-level mocks."""

    LEAD = TestAddToPowerDialer.LEAD

    @pytest.mark.asyncio
    @patch("integrations.aircall.AIRCALL_API_ID", "test-id")
    @patch("integrations.aircall.AIRCALL_API_TOKEN", "test-token")
    @patch("integrations.aircall.AIRCALL_CLOSER_USER_ID", "1492144")
    @patch("integrations.aircall._write_contact_note", new_callable=AsyncMock)
    @patch("integrations.aircall._upsert_contact", new_callable=AsyncMock, return_value="c-777")
    @patch("integrations.aircall._push_to_dialer_campaign", new_callable=AsyncMock, return_value={"status": "added", "phone": "+4915112345678"})
    async def test_full_flow_contact_note_dialer(self, mock_push, mock_upsert, mock_note):
        """Verify all steps: contact → note → dialer push."""
        fresh = datetime.now(timezone.utc) - timedelta(hours=1)
        result = await add_to_power_dialer(
            self.LEAD, score=85, created_at=fresh, interest_category="Hypnose"
        )

        assert result == {"status": "added", "phone": "+4915112345678"}
        mock_upsert.assert_called_once()
        mock_push.assert_called_once()
