"""/debug/aircall-status must look the user up via User V2.

Aircall removes the User V1 endpoints (list/retrieve/create/update a user) on
30.09.2026 (developers.aircall.io/api-references, changelog 25-02-2026, read
23.09.2026). The debug endpoint called GET /v1/users/{id} first and returned
early on any non-200, so after the cutoff it would report "User lookup failed"
and never reach the dialer-queue check. The dialer campaign itself has no v2
and stays on v1.
"""

import os
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

# main.py refuses to import without these; placeholders only, nothing is called.
for _k in ("HUBSPOT_ACCESS_TOKEN", "SUPABASE_URL", "SUPABASE_SERVICE_KEY"):
    os.environ.setdefault(_k, "http://localhost" if _k == "SUPABASE_URL" else "test")

import integrations.aircall as aircall  # noqa: E402
import main  # noqa: E402

USER_ID = "123"
V2_USER = {
    "user": {
        "id": 123,
        "name": "Test Closer",
        "email": "closer@sbc-test.dev",
        "available": True,
        "availability_status": "available",
    }
}


def _resp(status, payload):
    r = MagicMock()
    r.status_code = status
    r.json.return_value = payload
    r.text = ""
    return r


class _FakeClient:
    """Stands in for httpx.AsyncClient and records every GET URL."""

    def __init__(self, calls):
        self.calls = calls

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def get(self, url, headers=None):
        self.calls.append(url)
        if url.endswith("/dialer_campaign/phone_numbers"):
            return _resp(200, {"numbers": [{"id": 1}, {"id": 2}]})
        if url == f"https://api.aircall.io/v2/users/{USER_ID}":
            return _resp(200, V2_USER)
        return _resp(404, {"message": "Not Found"})


def _call(calls):
    with patch.object(main, "DEBUG_API_KEY", "k"), \
         patch.object(aircall, "AIRCALL_API_ID", "id"), \
         patch.object(aircall, "AIRCALL_API_TOKEN", "tok"), \
         patch.object(aircall, "AIRCALL_CLOSER_USER_ID", USER_ID), \
         patch("httpx.AsyncClient", lambda *a, **kw: _FakeClient(calls)):
        client = TestClient(main.app, raise_server_exceptions=False)
        return client.get("/debug/aircall-status", headers={"X-Api-Key": "k"}).json()


def test_user_lookup_uses_v2_endpoint():
    calls = []
    body = _call(calls)
    assert calls[0] == f"https://api.aircall.io/v2/users/{USER_ID}"
    assert not any(u.startswith("https://api.aircall.io/v1/users/") and u.endswith(f"/{USER_ID}") for u in calls)
    assert body["error"] is None


def test_v2_user_fields_are_read():
    body = _call([])
    assert body["user_info"] == {
        "id": 123,
        "name": "Test Closer",
        "email": "closer@sbc-test.dev",
        "available": "available",
    }


def test_dialer_queue_check_still_on_v1():
    calls = []
    body = _call(calls)
    assert calls[1] == f"https://api.aircall.io/v1/users/{USER_ID}/dialer_campaign/phone_numbers"
    assert body["dialer_campaign"]["contacts_count"] == 2
