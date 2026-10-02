"""A rejected Aircall key must stop the push loop after the first lead.

REGRESSION (batch 23.09.2026 16:26, key rejected since 18.07.2026): Aircall
answers every authenticated call with 403 ("Invalid API key or Bearer access
token" per its API reference). The batch nevertheless walked the whole push
queue — 958 leads — and for each one sent a phone search, an email search and a
contact POST, all 403. Every lead after the first measured nothing new; the
alarm already had its error sample. The fix: 401/403 raises AircallAuthError,
and the batch stops pushing on the first one.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import integrations.aircall as aircall
from integrations.aircall import AircallAuthError, add_to_power_dialer
from integrations.slack import BatchRunStats


def _resp(status):
    r = MagicMock()
    r.status_code = status
    r.json.return_value = {}
    r.text = '{"message":"Forbidden"}'
    request = MagicMock()
    request.url = "https://api.aircall.io/v1/contacts"

    def _raise():
        import httpx
        raise httpx.HTTPStatusError(
            f"Client error '{status} Forbidden' for url 'https://api.aircall.io/v1/contacts'",
            request=request, response=r,
        )
    r.raise_for_status.side_effect = _raise
    return r


LEAD = {"phone": "+41791234567", "firstname": "A", "lastname": "B", "email": "a@sbc-test.dev"}


@pytest.fixture
def creds():
    with patch.object(aircall, "AIRCALL_API_ID", "id"), \
         patch.object(aircall, "AIRCALL_API_TOKEN", "tok"), \
         patch.object(aircall, "AIRCALL_CLOSER_USER_ID", "1"):
        yield


@pytest.mark.asyncio
class TestAuthErrorIsDistinct:

    @pytest.mark.parametrize("status", [401, 403])
    @patch("integrations.aircall._aircall_request", new_callable=AsyncMock)
    async def test_401_403_raise_auth_error(self, mock_req, creds, status):
        mock_req.return_value = _resp(status)
        with pytest.raises(AircallAuthError) as exc:
            await add_to_power_dialer(LEAD, score=90, lead_tier="1_hot")
        # Slack shows this text as the error sample — keep the measured status and url.
        assert f"{status} Forbidden" in str(exc.value)
        assert "/v1/contacts" in str(exc.value)

    @patch("integrations.aircall._aircall_request", new_callable=AsyncMock)
    async def test_other_errors_stay_plain(self, mock_req, creds):
        mock_req.return_value = _resp(500)
        import httpx
        with pytest.raises(httpx.HTTPStatusError) as exc:
            await add_to_power_dialer(LEAD, score=90, lead_tier="1_hot")
        assert not isinstance(exc.value, AircallAuthError)


def _queue(n):
    return [
        {"phone": f"+4179123{i:04d}", "firstname": "A", "lastname": "B",
         "email": f"l{i}@sbc-test.dev", "aircall_card": "", "score": 90,
         "is_fresh": False, "funnel": None, "lead_tier": "1_hot",
         "list_key": "hot", "tier_label": "HOT"}
        for i in range(n)
    ]


@pytest.mark.asyncio
class TestBatchStopsOnAuthError:

    @patch("batch.scorer.is_within_call_window", return_value=True)
    @patch("integrations.aircall._aircall_request", new_callable=AsyncMock)
    async def test_403_stops_after_first_lead(self, mock_req, _win, creds):
        from batch.scorer import _push_aircall_queue
        mock_req.return_value = _resp(403)
        stats = BatchRunStats()
        pushed = await _push_aircall_queue(_queue(50), None, stats)
        assert pushed == 0
        # first lead: phone search, email search, contact POST — then stop
        assert mock_req.await_count <= 3
        assert stats.aircall_auth_skipped == 49
        assert "403 Forbidden" in stats.aircall_push_error_sample

    @patch("batch.scorer.is_within_call_window", return_value=True)
    @patch("integrations.aircall._aircall_request", new_callable=AsyncMock)
    async def test_500_keeps_trying_every_lead(self, mock_req, _win, creds):
        """A server error can be per-lead; only auth errors justify stopping."""
        from batch.scorer import _push_aircall_queue
        mock_req.return_value = _resp(500)
        stats = BatchRunStats()
        await _push_aircall_queue(_queue(5), None, stats)
        assert mock_req.await_count == 5 * 3
        assert stats.aircall_auth_skipped == 0
