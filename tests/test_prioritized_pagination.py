"""Tests for get_prioritized_contacts pagination (M5) and the bounded
call-poller dedup (H2).

M5 regression guard: the HubSpot search API caps a page at 100. Without
pagination the dialer CSV never saw leads beyond the top 100/tier, and
hard-excluded rows silently ate into that cap.
"""
import asyncio

import pytest

import integrations.hubspot as hs
from batch import call_poller


# ---------------------------------------------------------------- fakes

class _FakeResp:
    status_code = 200

    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload


def _contact(i, tier, paused=False):
    props = {"phone": f"+4179{i:07d}", "lead_tier": tier}
    if paused:
        props["lead_pause_until"] = "2099-01-01T00:00:00Z"
    return {"id": str(i), "properties": props}


class _FakeClient:
    """Serves per-tier page sequences and records how often it was called."""

    def __init__(self, pages_by_tier):
        self._pages = pages_by_tier          # tier -> list of (results, next_after)
        self.calls_by_tier = {t: 0 for t in pages_by_tier}

    def __call__(self, *a, **kw):  # constructor stand-in: AsyncClient(timeout=...)
        return self

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def post(self, url, headers=None, json=None):
        tier = json["filterGroups"][0]["filters"][0]["value"]
        idx = self.calls_by_tier[tier]
        self.calls_by_tier[tier] += 1
        pages = self._pages[tier]
        results, next_after = pages[min(idx, len(pages) - 1)]
        payload = {"results": results}
        if next_after:
            payload["paging"] = {"next": {"after": next_after}}
        return _FakeResp(payload)


def _run(client, limit):
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(hs, "ACCESS_TOKEN", "test-token")
        mp.setattr(hs.httpx, "AsyncClient", client)
        return asyncio.run(hs.get_prioritized_contacts(limit=limit))


# ---------------------------------------------------------------- M5 tests

def test_paginates_beyond_first_page():
    # Hot tier: page1 = 100 rows (30 paused), page2 = 60 clean rows.
    page1 = [_contact(i, "1_hot", paused=(i < 30)) for i in range(100)]
    page2 = [_contact(100 + i, "1_hot") for i in range(60)]
    client = _FakeClient({
        "1_hot":  [(page1, "cursor-2"), (page2, None)],
        "2_warm": [([], None)],
        "3_cold": [([], None)],
    })
    contacts = _run(client, limit=200)
    hot = [c for c in contacts if c["_tier"] == "1_hot"]
    # 70 kept from page1 + 60 from page2 — pre-fix this returned only 70.
    assert len(hot) == 130
    assert client.calls_by_tier["1_hot"] == 2


def test_stops_at_limit_and_truncates():
    # Endless pages of 100 clean rows -> must stop once `limit` kept.
    page = [_contact(i, "2_warm") for i in range(100)]
    client = _FakeClient({
        "1_hot":  [([], None)],
        "2_warm": [(page, "more"), (page, "more"), (page, "more"), (page, "more")],
        "3_cold": [([], None)],
    })
    contacts = _run(client, limit=150)
    warm = [c for c in contacts if c["_tier"] == "2_warm"]
    assert len(warm) == 150
    assert client.calls_by_tier["2_warm"] == 2  # 100 + 100 -> truncated to 150


def test_no_paging_key_single_call():
    page = [_contact(i, "3_cold") for i in range(40)]
    client = _FakeClient({
        "1_hot":  [([], None)],
        "2_warm": [([], None)],
        "3_cold": [(page, None)],
    })
    contacts = _run(client, limit=200)
    assert len([c for c in contacts if c["_tier"] == "3_cold"]) == 40
    assert client.calls_by_tier["3_cold"] == 1


def test_excluded_still_filtered_across_pages():
    # All rows paused -> nothing returned, no matter how many pages.
    page = [_contact(i, "1_hot", paused=True) for i in range(100)]
    client = _FakeClient({
        "1_hot":  [(page, "next"), (page, None)],
        "2_warm": [([], None)],
        "3_cold": [([], None)],
    })
    contacts = _run(client, limit=200)
    assert [c for c in contacts if c["_tier"] == "1_hot"] == []


# ------------------------------------------------- number-level exclusion

class _FakeExclClient(_FakeClient):
    """Single page sequence regardless of filter shape."""

    def __init__(self, pages):
        self._seq = pages
        self.calls = 0

    async def post(self, url, headers=None, json=None):
        results, next_after = self._seq[min(self.calls, len(self._seq) - 1)]
        self.calls += 1
        payload = {"results": results}
        if next_after:
            payload["paging"] = {"next": {"after": next_after}}
        return _FakeResp(payload)


def test_excluded_digits_include_mobilephone():
    # Verified leak 09.07: paused contact carries the number ONLY in
    # `mobilephone` -> a duplicate contact with the same number in `phone`
    # re-entered the CSV. The digit set must cover BOTH fields.
    rows = [
        {"id": "1", "properties": {
            "mobilephone": "+41 79 555 55 77",
            "lead_pause_until": "2099-01-01T00:00:00Z",
        }},
        {"id": "2", "properties": {
            "phone": "+491511000092",
            "lead_pause_until": "2020-01-01T00:00:00Z",  # expired -> NOT excluded
        }},
    ]
    client = _FakeExclClient([(rows, None)])
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(hs, "ACCESS_TOKEN", "test-token")
        mp.setattr(hs.httpx, "AsyncClient", client)
        digits = asyncio.run(hs.fetch_excluded_phone_digits())
    assert "41795555577" in digits          # mobilephone captured
    assert "795555577" in digits            # last-9 suffix too
    assert "491511000092" not in digits     # expired pause stays callable


# ---------------------------------------------------------------- H2 tests

def test_processed_call_ids_bounded():
    call_poller._processed_call_ids.clear()
    overflow = 25
    for i in range(call_poller._PROCESSED_CALL_IDS_MAX + overflow):
        call_poller._mark_call_processed(f"call-{i}")
    assert len(call_poller._processed_call_ids) == call_poller._PROCESSED_CALL_IDS_MAX
    # Oldest evicted, newest retained.
    assert "call-0" not in call_poller._processed_call_ids
    assert f"call-{overflow - 1}" not in call_poller._processed_call_ids
    assert f"call-{overflow}" in call_poller._processed_call_ids
    assert f"call-{call_poller._PROCESSED_CALL_IDS_MAX + overflow - 1}" in call_poller._processed_call_ids
    call_poller._processed_call_ids.clear()
