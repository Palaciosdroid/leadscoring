"""Regression guard: the batch lead-pool fetch must survive >10,000 contacts.

Outage 18.-26.07 (~8 days, zero scoring): HubSpot's search API caps one result
set at 10,000 and returns a bare HTTP 400 when the `after` cursor walks past it.
SCORE_ACTIVE_UNSCORED scored ~1,500 extra contacts, the scored pool reached
10,678, and every batch died in Step 1 — which aborts the entire run.

_fetch_active_hubspot_leads now restarts the query per chunk
(`hs_object_id GT last_seen`, sorted ascending) so no single request can exceed
the cap.
"""
import asyncio

import pytest

import batch.scorer as scorer


class _Resp:
    def __init__(self, status, payload=None):
        self.status_code = status
        self._payload = payload or {}
        self.text = ""

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class _CappedClient:
    """Emulates HubSpot: serves ids in ascending order, 400s past 10k offset.

    Rejects any request carrying an `after` cursor beyond the cap — exactly the
    behaviour that killed the batch.
    """

    CAP = 10_000

    def __init__(self, total):
        self.total = total
        self.requests = []

    def __call__(self, *a, **kw):
        return self

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def post(self, url, headers=None, json=None):
        self.requests.append(json)

        # The old cursor style must not reappear.
        if "after" in json:
            offset = int(json["after"])
            if offset >= self.CAP:
                return _Resp(400, {"status": "error"})

        gt = 0
        for f in json["filterGroups"][0]["filters"]:
            if f["propertyName"] == "hs_object_id" and f["operator"] == "GT":
                gt = int(f["value"])
        start = gt + 1
        ids = [i for i in range(start, min(start + 100, self.total + 1))]
        return _Resp(200, {"results": [{"id": str(i), "properties": {"email": f"c{i}@x.de"}} for i in ids]})


def _run(client):
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(scorer, "HUBSPOT_TOKEN", "test-token")
        mp.setattr(scorer.httpx, "AsyncClient", client)

        async def _no_sleep(*a, **k):
            return None

        mp.setattr(scorer.asyncio, "sleep", _no_sleep)
        return asyncio.run(scorer._fetch_active_hubspot_leads())


def test_fetches_full_pool_beyond_10k_cap():
    # 10,678 = the real pool size on the day the batch broke.
    client = _CappedClient(total=10_678)
    contacts = _run(client)
    assert len(contacts) == 10_678, "pool truncated — the 10k cap regression is back"
    # No request may rely on the `after` cursor (that is what hit the cap).
    assert all("after" not in req for req in client.requests)


def test_pool_walks_ascending_ids_without_gaps():
    client = _CappedClient(total=250)
    contacts = _run(client)
    ids = [int(c["id"]) for c in contacts]
    assert ids == sorted(ids)
    assert len(set(ids)) == len(ids)          # no duplicates
    assert ids == list(range(1, 251))         # no gaps


def test_stops_when_pool_exhausted():
    client = _CappedClient(total=42)
    contacts = _run(client)
    assert len(contacts) == 42
    assert len(client.requests) == 1          # short page ends the walk
