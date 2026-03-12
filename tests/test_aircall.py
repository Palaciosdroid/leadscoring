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
        assert _should_dial(4, created) is False

    def test_exactly_5_dials(self):
        created = datetime.now(timezone.utc) - timedelta(days=3)
        assert _should_dial(5, created) is True

    def test_just_below_5_skipped(self):
        created = datetime.now(timezone.utc) - timedelta(days=3)
        assert _should_dial(4, created) is False


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


class TestUpsertContact:
    """Test _upsert_contact with mocked HTTP responses."""

    LEAD = TestAddToPowerDialer.LEAD

    @pytest.mark.asyncio
    @patch("integrations.aircall.AIRCALL_API_ID", "test-id")
    @patch("integrations.aircall.AIRCALL_API_TOKEN", "test-token")
    async def test_success_returns_contact_id(self):
        from integrations.aircall import _upsert_contact

        mock_response = MagicMock()
        mock_response.status_code = 201
        mock_response.json.return_value = {"contact": {"id": 12345}}

        with patch("httpx.AsyncClient") as MockClient:
            instance = AsyncMock()
            instance.post.return_value = mock_response
            instance.__aenter__ = AsyncMock(return_value=instance)
            instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = instance

            result = await _upsert_contact(self.LEAD, tags=["score-75", "fresh"], timeout=5.0)
            assert result == "12345"
            instance.post.assert_called_once()
            call_kwargs = instance.post.call_args
            assert "contacts" in call_kwargs.args[0]
            assert call_kwargs.kwargs["json"]["tags"] == ["score-75", "fresh"]

    @pytest.mark.asyncio
    @patch("integrations.aircall.AIRCALL_API_ID", "test-id")
    @patch("integrations.aircall.AIRCALL_API_TOKEN", "test-token")
    async def test_no_phone_raises_value_error(self):
        from integrations.aircall import _upsert_contact

        lead_no_phone = {**self.LEAD, "phone": ""}
        with pytest.raises(ValueError, match="No phone number"):
            await _upsert_contact(lead_no_phone, tags=[], timeout=5.0)

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

        with patch("httpx.AsyncClient") as MockClient:
            instance = AsyncMock()
            instance.post.return_value = mock_response
            instance.__aenter__ = AsyncMock(return_value=instance)
            instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = instance

            with pytest.raises(httpx.HTTPStatusError):
                await _upsert_contact(self.LEAD, tags=[], timeout=5.0)

    @pytest.mark.asyncio
    @patch("integrations.aircall.AIRCALL_API_ID", "test-id")
    @patch("integrations.aircall.AIRCALL_API_TOKEN", "test-token")
    async def test_no_tags_omits_tags_key(self):
        from integrations.aircall import _upsert_contact

        mock_response = MagicMock()
        mock_response.status_code = 201
        mock_response.json.return_value = {"contact": {"id": 999}}

        with patch("httpx.AsyncClient") as MockClient:
            instance = AsyncMock()
            instance.post.return_value = mock_response
            instance.__aenter__ = AsyncMock(return_value=instance)
            instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = instance

            await _upsert_contact(self.LEAD, tags=None, timeout=5.0)
            payload = instance.post.call_args.kwargs["json"]
            assert "tags" not in payload


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

        with patch("httpx.AsyncClient") as MockClient:
            instance = AsyncMock()
            instance.post.return_value = mock_response
            instance.__aenter__ = AsyncMock(return_value=instance)
            instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = instance

            result = await _push_to_dialer_campaign(self.LEAD, timeout=5.0)
            assert result == {"status": "added", "phone": "+4915112345678"}
            # Verify correct URL with user ID
            url = instance.post.call_args.args[0]
            assert "1492144" in url
            assert "dialer_campaign/phone_numbers" in url

    @pytest.mark.asyncio
    @patch("integrations.aircall.AIRCALL_CLOSER_USER_ID", "1492144")
    @patch("integrations.aircall.AIRCALL_API_ID", "test-id")
    @patch("integrations.aircall.AIRCALL_API_TOKEN", "test-token")
    async def test_already_imported_422_returns_status(self):
        from integrations.aircall import _push_to_dialer_campaign

        mock_response = MagicMock()
        mock_response.status_code = 422
        mock_response.text = '{"error": "Phone number already imported in campaign"}'

        with patch("httpx.AsyncClient") as MockClient:
            instance = AsyncMock()
            instance.post.return_value = mock_response
            instance.__aenter__ = AsyncMock(return_value=instance)
            instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = instance

            result = await _push_to_dialer_campaign(self.LEAD, timeout=5.0)
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

        with patch("httpx.AsyncClient") as MockClient:
            instance = AsyncMock()
            instance.post.return_value = mock_response
            instance.__aenter__ = AsyncMock(return_value=instance)
            instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = instance

            with pytest.raises(httpx.HTTPStatusError):
                await _push_to_dialer_campaign(self.LEAD, timeout=5.0)

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

        with patch("httpx.AsyncClient") as MockClient:
            instance = AsyncMock()
            instance.post.return_value = mock_response
            instance.__aenter__ = AsyncMock(return_value=instance)
            instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = instance

            with pytest.raises(httpx.HTTPStatusError):
                await _push_to_dialer_campaign(self.LEAD, timeout=5.0)


class TestEndToEndFlow:
    """Full flow test: add_to_power_dialer with HTTP-level mocks."""

    LEAD = TestAddToPowerDialer.LEAD

    @pytest.mark.asyncio
    @patch("integrations.aircall.AIRCALL_API_ID", "test-id")
    @patch("integrations.aircall.AIRCALL_API_TOKEN", "test-token")
    @patch("integrations.aircall.AIRCALL_CLOSER_USER_ID", "1492144")
    async def test_full_flow_contact_then_dialer(self):
        """Verify both HTTP calls happen in order: contact creation, then dialer push."""
        contact_response = MagicMock()
        contact_response.status_code = 201
        contact_response.json.return_value = {"contact": {"id": 777}}

        dialer_response = MagicMock()
        dialer_response.status_code = 200
        dialer_response.json.return_value = {}

        call_order = []

        async def mock_post(url, **kwargs):
            if "contacts" in url:
                call_order.append("contact")
                return contact_response
            elif "dialer_campaign" in url:
                call_order.append("dialer")
                return dialer_response
            raise ValueError(f"Unexpected URL: {url}")

        with patch("httpx.AsyncClient") as MockClient:
            instance = AsyncMock()
            instance.post.side_effect = mock_post
            instance.__aenter__ = AsyncMock(return_value=instance)
            instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = instance

            fresh = datetime.now(timezone.utc) - timedelta(hours=1)
            result = await add_to_power_dialer(
                self.LEAD, score=85, created_at=fresh, interest_category="Hypnose"
            )

            assert result == {"status": "added", "phone": "+4915112345678"}
            assert call_order == ["contact", "dialer"]
            assert instance.post.call_count == 2
