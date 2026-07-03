"""Tests for the dialer exclusion-leak fix (2026-06-30):
- _should_dial rejects disqualified leads even when fresh (C2)
- remove_from_power_dialer / remove_many use DELETE-by-id (verified Aircall method)
- _find_number_id matches safely (full digits, unique last-9, refuse ambiguous)
- dialer_suppressed gates webhook pushes against stored HubSpot lifecycle state (C1)
"""

from datetime import datetime, timezone, timedelta
from unittest.mock import patch, AsyncMock

import pytest

from integrations.aircall import (
    _should_dial,
    _find_number_id,
    remove_from_power_dialer,
    remove_many_from_power_dialer,
)
from batch.dialer_gate import dialer_suppressed, _is_paused


def _resp(status):
    class _R:
        status_code = status
        text = ""
    return _R()


# ── C2: _should_dial ─────────────────────────────────────────────────────────

class TestShouldDialHardExclusions:
    def test_fresh_disqualified_is_rejected(self):
        # Regression C2: a "fresh" disqualified lead must NOT dial.
        created = datetime.now(timezone.utc) - timedelta(hours=1)
        assert _should_dial(50, created, lead_tier="4_disqualified", is_fresh=True) is False

    def test_booked_rejected_even_if_fresh(self):
        created = datetime.now(timezone.utc) - timedelta(hours=1)
        assert _should_dial(99, created, lead_tier="0_booked", is_fresh=True) is False

    def test_fresh_warm_still_dials(self):
        created = datetime.now(timezone.utc) - timedelta(hours=1)
        assert _should_dial(15, created, lead_tier="2_warm", is_fresh=True) is True


# ── Phone → queue-id matching ────────────────────────────────────────────────

class TestFindNumberId:
    QUEUE = [
        {"id": 111, "number": "41794351803", "called": False},
        {"id": 222, "number": "491601500545", "called": False},
        {"id": 333, "number": "491601500999", "called": False},
    ]

    def test_full_digit_match_with_plus(self):
        assert _find_number_id(self.QUEUE, "+41794351803") == 111

    def test_national_format_matches_via_last9(self):
        # HubSpot national "0794351803" → last9 796... wait: CH local maps by last-9
        assert _find_number_id(self.QUEUE, "0041601500545") == 222

    def test_not_in_queue_returns_none(self):
        assert _find_number_id(self.QUEUE, "+49999999999") is None

    def test_ambiguous_last9_refused(self):
        # Two entries differ only beyond the last 9 digits → refuse rather than guess.
        q = [
            {"id": 1, "number": "491601500545"},
            {"id": 2, "number": "501601500545"},
        ]
        assert _find_number_id(q, "01601500545") is None

    def test_empty_phone(self):
        assert _find_number_id(self.QUEUE, "") is None


# ── remove_from_power_dialer (by-id) ─────────────────────────────────────────

@patch("integrations.aircall.AIRCALL_API_ID", "id")
@patch("integrations.aircall.AIRCALL_API_TOKEN", "tok")
@patch("integrations.aircall.AIRCALL_CLOSER_USER_ID", "1492144")
class TestRemoveFromPowerDialer:
    QUEUE = [{"id": 555, "number": "41794351803", "called": False}]

    @pytest.mark.asyncio
    @patch("integrations.aircall._get_dialer_queue", new_callable=AsyncMock)
    @patch("integrations.aircall._aircall_request", new_callable=AsyncMock)
    async def test_removes_by_id_returns_true(self, mock_req, mock_queue):
        mock_queue.return_value = self.QUEUE
        mock_req.return_value = _resp(204)
        ok = await remove_from_power_dialer("+41794351803")
        assert ok is True
        # DELETE must target the id-specific URL, not the collection.
        called_url = mock_req.call_args.args[2]
        assert called_url.endswith("/dialer_campaign/phone_numbers/555")

    @pytest.mark.asyncio
    @patch("integrations.aircall._get_dialer_queue", new_callable=AsyncMock)
    @patch("integrations.aircall._aircall_request", new_callable=AsyncMock)
    async def test_not_in_queue_returns_false_no_delete(self, mock_req, mock_queue):
        # Regression: previously a missing number was reported as success (404→True).
        mock_queue.return_value = self.QUEUE
        ok = await remove_from_power_dialer("+49999999999")
        assert ok is False
        mock_req.assert_not_called()

    @pytest.mark.asyncio
    @patch("integrations.aircall._get_dialer_queue", new_callable=AsyncMock)
    @patch("integrations.aircall._aircall_request", new_callable=AsyncMock)
    async def test_delete_404_is_failure_not_success(self, mock_req, mock_queue):
        mock_queue.return_value = self.QUEUE
        mock_req.return_value = _resp(404)
        ok = await remove_from_power_dialer("+41794351803")
        assert ok is False  # 404 on the by-id delete is a real failure now


@patch("integrations.aircall.AIRCALL_API_ID", "id")
@patch("integrations.aircall.AIRCALL_API_TOKEN", "tok")
@patch("integrations.aircall.AIRCALL_CLOSER_USER_ID", "1492144")
class TestRemoveMany:
    @pytest.mark.asyncio
    @patch("integrations.aircall._get_dialer_queue", new_callable=AsyncMock)
    @patch("integrations.aircall._aircall_request", new_callable=AsyncMock)
    async def test_removes_only_matches(self, mock_req, mock_queue):
        mock_queue.return_value = [
            {"id": 1, "number": "41794351803"},
            {"id": 2, "number": "491601500545"},
            {"id": 3, "number": "491111111111"},
        ]
        mock_req.return_value = _resp(204)
        n = await remove_many_from_power_dialer({"+41794351803", "+491601500545", "+49000000000"})
        assert n == 2  # third phone not in queue

    @pytest.mark.asyncio
    @patch("integrations.aircall._get_dialer_queue", new_callable=AsyncMock)
    @patch("integrations.aircall._aircall_request", new_callable=AsyncMock)
    async def test_mass_removal_guardrail(self, mock_req, mock_queue):
        # MATCHED targets exceed half the queue (and the floor of 20) → refuse.
        mock_queue.return_value = [{"id": i, "number": f"49160000{i:04d}"} for i in range(30)]
        phones = {f"+49160000{i:04d}" for i in range(21)}  # 21 matches > max(20, 30//2=15)
        n = await remove_many_from_power_dialer(phones)
        assert n == 0
        mock_req.assert_not_called()

    @pytest.mark.asyncio
    @patch("integrations.aircall._get_dialer_queue", new_callable=AsyncMock)
    @patch("integrations.aircall._aircall_request", new_callable=AsyncMock)
    async def test_large_excluded_set_with_few_matches_is_not_refused(self, mock_req, mock_queue):
        # Regression 03.07: the excluded set legitimately holds hundreds of
        # paused leads NOT in the queue. Guardrail must size on MATCHES, not on
        # the raw set — 484 targets vs 385 queue refused every batch removal.
        mock_queue.return_value = [{"id": i, "number": f"49160000{i:04d}"} for i in range(30)]
        phones = {f"+49170999{i:04d}" for i in range(500)}   # 500 targets, none in queue
        phones.add("+491600000001")                           # exactly 1 real match
        mock_req.return_value = _resp(204)
        n = await remove_many_from_power_dialer(phones)
        assert n == 1
        assert mock_req.await_count == 1


# ── dialer_suppressed gate (C1) ──────────────────────────────────────────────

class TestIsPaused:
    def test_dialer_removed(self):
        assert _is_paused({"lead_dialer_removed": "true"}, datetime.now(timezone.utc)) is True

    def test_active_pause(self):
        future = (datetime.now(timezone.utc) + timedelta(days=10)).isoformat()
        assert _is_paused({"lead_pause_until": future}, datetime.now(timezone.utc)) is True

    def test_expired_pause(self):
        past = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
        assert _is_paused({"lead_pause_until": past}, datetime.now(timezone.utc)) is False

    def test_no_pause(self):
        assert _is_paused({}, datetime.now(timezone.utc)) is False


class TestDialerSuppressed:
    @pytest.mark.asyncio
    @patch("batch.dialer_gate.get_contact_id", new_callable=AsyncMock, return_value="hs1")
    @patch("batch.dialer_gate.has_upcoming_hubspot_meeting", new_callable=AsyncMock, return_value=False)
    @patch("batch.dialer_gate.get_contact_properties", new_callable=AsyncMock)
    async def test_paused_lead_suppressed(self, mock_props, *_):
        future = (datetime.now(timezone.utc) + timedelta(days=30)).isoformat()
        mock_props.return_value = {"lead_pause_until": future}
        suppressed, reason = await dialer_suppressed(email="x@y.com", phone="+4179")
        assert suppressed is True
        assert reason == "paused_or_removed"

    @pytest.mark.asyncio
    @patch("batch.dialer_gate.get_contact_id", new_callable=AsyncMock, return_value="hs1")
    @patch("batch.dialer_gate.has_upcoming_hubspot_meeting", new_callable=AsyncMock, return_value=True)
    @patch("batch.dialer_gate.get_contact_properties", new_callable=AsyncMock, return_value={})
    async def test_upcoming_meeting_suppressed(self, *_):
        suppressed, reason = await dialer_suppressed(email="x@y.com")
        assert suppressed is True
        assert reason == "call_booked"

    @pytest.mark.asyncio
    @patch("batch.dialer_gate.get_contact_id", new_callable=AsyncMock, return_value="hs1")
    @patch("batch.dialer_gate.has_upcoming_hubspot_meeting", new_callable=AsyncMock, return_value=False)
    @patch("batch.dialer_gate.get_contact_properties", new_callable=AsyncMock, return_value={"hs_email_optout": "true"})
    async def test_unsubscribed_suppressed(self, *_):
        suppressed, reason = await dialer_suppressed(email="x@y.com")
        assert suppressed is True
        assert reason == "unsubscribed"

    @pytest.mark.asyncio
    @patch("batch.dialer_gate.get_contact_id", new_callable=AsyncMock, return_value="hs1")
    @patch("batch.dialer_gate.has_upcoming_hubspot_meeting", new_callable=AsyncMock, return_value=False)
    @patch("batch.dialer_gate.get_contact_properties", new_callable=AsyncMock, return_value={})
    async def test_clean_lead_not_suppressed(self, *_):
        suppressed, reason = await dialer_suppressed(email="x@y.com", phone="+4179")
        assert suppressed is False

    @pytest.mark.asyncio
    @patch("batch.dialer_gate.get_contact_id", new_callable=AsyncMock, return_value=None)
    async def test_unknown_contact_is_new_not_suppressed(self, _):
        suppressed, reason = await dialer_suppressed(email="new@y.com", phone="+4179")
        assert suppressed is False
        assert reason == "new"
