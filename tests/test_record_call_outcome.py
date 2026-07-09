"""Tests for the shared call-outcome writer (lifecycle + immediate dialer removal)."""
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest

from batch.call_poller import record_call_outcome

NOW = datetime(2026, 7, 3, 12, 0, tzinfo=timezone.utc)

EMPTY_STATE = {
    "lead_no_answer_streak": "0",
    "lead_no_answer_cycles": "0",
    "lead_pause_until": "",
    "lead_dialer_removed": "false",
    "phone": "+41791234567",
}


@pytest.mark.asyncio
@patch("batch.call_poller.remove_from_power_dialer", new_callable=AsyncMock, return_value=True)
@patch("batch.call_poller.update_contact_properties", new_callable=AsyncMock, return_value=True)
@patch("batch.call_poller.get_contact_properties", new_callable=AsyncMock, return_value=dict(EMPTY_STATE))
async def test_reached_sets_pause_and_removes_immediately(mock_get, mock_update, mock_remove):
    await record_call_outcome("123", "Kontakt aufgenommen", NOW, phone="+41791234567")
    # pause written (90d)
    written = mock_update.call_args[0][1]
    assert written["lead_pause_until"].startswith("2026-10-01")
    assert written["lead_last_call_outcome"] == "Kontakt aufgenommen"
    # immediately removed from the live queue
    mock_remove.assert_awaited_once_with("+41791234567")


@pytest.mark.asyncio
@patch("batch.call_poller.remove_from_power_dialer", new_callable=AsyncMock, return_value=True)
@patch("batch.call_poller.update_contact_properties", new_callable=AsyncMock, return_value=True)
@patch("batch.call_poller.get_contact_properties", new_callable=AsyncMock, return_value=dict(EMPTY_STATE))
async def test_first_no_answer_does_not_remove(mock_get, mock_update, mock_remove):
    await record_call_outcome("123", "Keine Antwort", NOW, phone="+41791234567")
    written = mock_update.call_args[0][1]
    assert written["lead_no_answer_streak"] == "1"
    assert written["lead_pause_until"] == ""
    mock_remove.assert_not_awaited()


@pytest.mark.asyncio
@patch("batch.call_poller.remove_from_power_dialer", new_callable=AsyncMock, return_value=True)
@patch("batch.call_poller.update_contact_properties", new_callable=AsyncMock, return_value=True)
@patch("batch.call_poller.get_contact_properties", new_callable=AsyncMock, return_value=dict(EMPTY_STATE))
async def test_wrong_number_removes_immediately(mock_get, mock_update, mock_remove):
    await record_call_outcome("123", "Falsche Nummer", NOW, phone="+41791234567")
    written = mock_update.call_args[0][1]
    assert written["lead_dialer_removed"] == "true"
    mock_remove.assert_awaited_once()


@pytest.mark.asyncio
@patch("batch.call_poller.remove_from_power_dialer", new_callable=AsyncMock, return_value=True)
@patch("batch.call_poller.update_contact_properties", new_callable=AsyncMock, return_value=True)
@patch("batch.call_poller.get_contact_properties", new_callable=AsyncMock, return_value=dict(EMPTY_STATE))
async def test_webhook_path_without_phone_falls_back_to_hubspot_phone(mock_get, mock_update, mock_remove):
    # webhook payload has no phone — must use the contact's HubSpot phone
    await record_call_outcome("123", "Kontakt aufgenommen", NOW)
    mock_remove.assert_awaited_once_with("+41791234567")


@pytest.mark.asyncio
@patch("batch.call_poller.remove_from_power_dialer", new_callable=AsyncMock, side_effect=Exception("aircall down"))
@patch("batch.call_poller.update_contact_properties", new_callable=AsyncMock, return_value=True)
@patch("batch.call_poller.get_contact_properties", new_callable=AsyncMock, return_value=dict(EMPTY_STATE))
async def test_removal_failure_does_not_break_lifecycle_write(mock_get, mock_update, mock_remove):
    # removal is best-effort — lifecycle write must survive an Aircall outage
    await record_call_outcome("123", "Kontakt aufgenommen", NOW, phone="+41791234567")
    mock_update.assert_awaited_once()


@pytest.mark.asyncio
@patch("batch.call_poller.remove_from_power_dialer", new_callable=AsyncMock, return_value=True)
@patch("batch.call_poller.update_contact_properties", new_callable=AsyncMock, return_value=True)
@patch("batch.call_poller.get_contact_properties", new_callable=AsyncMock, return_value=dict(EMPTY_STATE))
async def test_third_no_answer_pauses_and_removes(mock_get, mock_update, mock_remove):
    state = dict(EMPTY_STATE, lead_no_answer_streak="2")
    mock_get.return_value = state
    await record_call_outcome("123", "Keine Antwort", NOW, phone="+41791234567")
    written = mock_update.call_args[0][1]
    assert written["lead_pause_until"] != ""   # 60d pause triggered
    mock_remove.assert_awaited_once()


@pytest.mark.asyncio
@patch("batch.call_poller.remove_from_power_dialer", new_callable=AsyncMock, return_value=True)
@patch("batch.call_poller.update_contact_properties", new_callable=AsyncMock, return_value=True)
@patch("batch.call_poller.get_contact_properties", new_callable=AsyncMock, return_value=dict(EMPTY_STATE))
async def test_not_interested_permanent_removal(mock_get, mock_update, mock_remove):
    # 7th disposition (UI-created): permanent stop — removed flag + property + immediate pull.
    await record_call_outcome("123", "Nicht interessiert", NOW, phone="+41791234567")
    written = mock_update.call_args[0][1]
    assert written["lead_dialer_removed"] == "true"
    assert written["lead_not_interested"] == "true"
    assert written["lead_last_call_outcome"] == "Nicht interessiert"
    mock_remove.assert_awaited_once_with("+41791234567")


@pytest.mark.asyncio
@patch("batch.call_poller.remove_from_power_dialer", new_callable=AsyncMock, return_value=True)
@patch("batch.call_poller.update_contact_properties", new_callable=AsyncMock, return_value=True)
@patch("batch.call_poller.get_contact_properties", new_callable=AsyncMock, return_value=dict(EMPTY_STATE))
async def test_reached_does_not_set_not_interested(mock_get, mock_update, mock_remove):
    await record_call_outcome("123", "Kontakt aufgenommen", NOW, phone="+41791234567")
    written = mock_update.call_args[0][1]
    assert "lead_not_interested" not in written
