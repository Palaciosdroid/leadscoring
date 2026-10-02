"""An unreadable Aircall queue must not look like an empty one.

REGRESSION (live 19.08.2026, Aircall key dead since 18.07): every /v1/* call
answers 403. `_get_dialer_queue` swallowed that and returned [], so both callers
read "the queue holds nothing" where the truth was "the queue could not be read":

  remove_from_power_dialer  -> logged "not in Power Dialer queue — nothing to
                               remove", a claim about queue contents nobody measured.
  remove_many_from_power_dialer -> returned 0, which the batch report printed as
                               "0 removed" — indistinguishable from a clean run
                               with nothing to do.

Four weeks of runs therefore reported a healthy removal path while no removal
was possible at all. The fix separates the two states; these tests hold them apart.
"""

import logging
from unittest.mock import patch, AsyncMock, MagicMock

import pytest

from integrations.aircall import (
    QUEUE_UNREADABLE,
    _get_dialer_queue,
    remove_from_power_dialer,
    remove_many_from_power_dialer,
)

QUEUE = [
    {"id": 326570506, "number": "41794351803", "called": False},
    {"id": 326570507, "number": "491601500545", "called": False},
]


def _resp(status, payload=None):
    r = MagicMock()
    r.status_code = status
    r.json.return_value = payload if payload is not None else {}
    r.text = "" if status == 200 else '{"message":"Forbidden"}'
    return r


# ── _get_dialer_queue: the two states must differ ────────────────────────────


@pytest.mark.asyncio
class TestQueueReadDistinguishesUnreadableFromEmpty:

    @patch("integrations.aircall._aircall_request", new_callable=AsyncMock)
    async def test_403_returns_none_not_empty_list(self, mock_req):
        """403 is 'could not read', never 'is empty'."""
        mock_req.return_value = _resp(403)
        assert await _get_dialer_queue(MagicMock()) is None

    @patch("integrations.aircall._aircall_request", new_callable=AsyncMock)
    async def test_401_and_500_also_return_none(self, mock_req):
        for status in (401, 500, 502):
            mock_req.return_value = _resp(status)
            assert await _get_dialer_queue(MagicMock()) is None, status

    @patch("integrations.aircall._aircall_request", new_callable=AsyncMock)
    async def test_genuinely_empty_queue_returns_empty_list(self, mock_req):
        """200 with no numbers is a real answer — the queue is empty."""
        mock_req.return_value = _resp(200, {"numbers": []})
        assert await _get_dialer_queue(MagicMock()) == []

    @patch("integrations.aircall._aircall_request", new_callable=AsyncMock)
    async def test_404_no_active_campaign_is_empty(self, mock_req):
        """Aircall docs: 404 = 'User has no active campaign' — a real answer."""
        mock_req.return_value = _resp(404)
        assert await _get_dialer_queue(MagicMock()) == []

    @patch("integrations.aircall._aircall_request", new_callable=AsyncMock)
    async def test_populated_queue_returns_numbers(self, mock_req):
        mock_req.return_value = _resp(200, {"numbers": QUEUE})
        assert await _get_dialer_queue(MagicMock()) == QUEUE


# ── remove_from_power_dialer: stop claiming the number is absent ─────────────


@patch("integrations.aircall.AIRCALL_API_ID", "id")
@patch("integrations.aircall.AIRCALL_API_TOKEN", "tok")
@patch("integrations.aircall.AIRCALL_CLOSER_USER_ID", "1492144")
@pytest.mark.asyncio
class TestRemoveOneWithUnreadableQueue:

    @patch("integrations.aircall._get_dialer_queue", new_callable=AsyncMock)
    @patch("integrations.aircall._aircall_request", new_callable=AsyncMock)
    async def test_unreadable_queue_returns_false_without_delete(self, mock_req, mock_queue):
        mock_queue.return_value = None
        assert await remove_from_power_dialer("+41794351803") is False
        mock_req.assert_not_called()

    @patch("integrations.aircall._get_dialer_queue", new_callable=AsyncMock)
    @patch("integrations.aircall._aircall_request", new_callable=AsyncMock)
    async def test_unreadable_queue_does_not_claim_number_is_absent(
        self, mock_req, mock_queue, caplog
    ):
        """The old log line asserted a fact about queue contents that was never read."""
        mock_queue.return_value = None
        with caplog.at_level(logging.INFO, logger="integrations.aircall"):
            await remove_from_power_dialer("+41794351803")
        joined = " ".join(r.getMessage() for r in caplog.records)
        assert "nothing to remove" not in joined
        assert "not in Power Dialer queue" not in joined
        assert "queue unreadable" in joined.lower()

    @patch("integrations.aircall._get_dialer_queue", new_callable=AsyncMock)
    @patch("integrations.aircall._aircall_request", new_callable=AsyncMock)
    async def test_empty_queue_still_says_not_in_queue(self, mock_req, mock_queue, caplog):
        """A real empty queue keeps the old, correct message."""
        mock_queue.return_value = []
        with caplog.at_level(logging.INFO, logger="integrations.aircall"):
            assert await remove_from_power_dialer("+41794351803") is False
        assert "nothing to remove" in " ".join(r.getMessage() for r in caplog.records)
        mock_req.assert_not_called()


# ── remove_many: 'unknown' must not be reported as 'zero' ────────────────────


@patch("integrations.aircall.AIRCALL_API_ID", "id")
@patch("integrations.aircall.AIRCALL_API_TOKEN", "tok")
@patch("integrations.aircall.AIRCALL_CLOSER_USER_ID", "1492144")
@pytest.mark.asyncio
class TestRemoveManyWithUnreadableQueue:

    @patch("integrations.aircall._get_dialer_queue", new_callable=AsyncMock)
    @patch("integrations.aircall._aircall_request", new_callable=AsyncMock)
    async def test_unreadable_queue_returns_sentinel_not_zero(self, mock_req, mock_queue):
        mock_queue.return_value = None
        assert await remove_many_from_power_dialer({"+41794351803"}) == QUEUE_UNREADABLE
        mock_req.assert_not_called()

    @patch("integrations.aircall._get_dialer_queue", new_callable=AsyncMock)
    @patch("integrations.aircall._aircall_request", new_callable=AsyncMock)
    async def test_empty_queue_returns_zero(self, mock_req, mock_queue):
        """Nothing in the queue means nothing was removed — that really is 0."""
        mock_queue.return_value = []
        assert await remove_many_from_power_dialer({"+41794351803"}) == 0

    @patch("integrations.aircall._get_dialer_queue", new_callable=AsyncMock)
    @patch("integrations.aircall._aircall_request", new_callable=AsyncMock)
    async def test_sentinel_is_negative_so_it_can_never_be_a_real_count(
        self, mock_req, mock_queue
    ):
        assert QUEUE_UNREADABLE < 0

    @patch("integrations.aircall._get_dialer_queue", new_callable=AsyncMock)
    @patch("integrations.aircall._aircall_request", new_callable=AsyncMock)
    async def test_normal_removal_still_counts(self, mock_req, mock_queue):
        mock_queue.return_value = QUEUE
        mock_req.return_value = _resp(204)
        assert await remove_many_from_power_dialer({"+41794351803"}) == 1
