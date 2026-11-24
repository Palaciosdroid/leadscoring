"""Customer.io reporting webhook signature verification.

Customer.io signs reporting webhooks with HMAC-SHA256 over
``v0:<X-CIO-Timestamp>:<raw body>`` (docs.customer.io, reporting webhooks).
The endpoint used to hash the body alone, so every real Customer.io delivery
was rejected with 401 once CIO_WEBHOOK_SECRET was set.
"""

import hashlib
import hmac
import os
from unittest.mock import patch

import pytest

SECRET = "test-signing-key"
# Unrecognised payload: the endpoint acknowledges it with 200 and touches nothing.
BODY = b'{"ping": true}'
TS = "1795521294"


def _sign(message: bytes) -> str:
    return hmac.new(SECRET.encode(), message, hashlib.sha256).hexdigest()


def _v0(ts: str, body: bytes) -> str:
    return _sign(b"v0:" + ts.encode() + b":" + body)


@pytest.fixture
def client():
    with patch.dict(os.environ, {
        "HUBSPOT_ACCESS_TOKEN": "test",
        "SUPABASE_URL": "http://localhost",
        "SUPABASE_SERVICE_KEY": "test",
        "AIRCALL_API_ID": "test",
        "AIRCALL_API_TOKEN": "test",
    }):
        import main as main_module
        from fastapi.testclient import TestClient
        with patch.object(main_module, "WEBHOOK_SECRET", SECRET):
            yield TestClient(main_module.app, raise_server_exceptions=False)


def _post(client, headers):
    return client.post(
        "/webhook/customerio",
        content=BODY,
        headers={"Content-Type": "application/json", **headers},
    )


def test_accepts_customerio_v0_signature(client):
    resp = _post(client, {"X-CIO-Timestamp": TS, "X-CIO-Signature": _v0(TS, BODY)})
    assert resp.status_code == 200


def test_accepts_v0_signature_with_sha256_prefix(client):
    resp = _post(client, {"X-CIO-Timestamp": TS, "X-CIO-Signature": "sha256=" + _v0(TS, BODY)})
    assert resp.status_code == 200


def test_rejects_v0_signature_with_other_timestamp(client):
    resp = _post(client, {"X-CIO-Timestamp": "1795521295", "X-CIO-Signature": _v0(TS, BODY)})
    assert resp.status_code == 401


def test_rejects_v0_signature_for_tampered_body(client):
    resp = _post(client, {"X-CIO-Timestamp": TS, "X-CIO-Signature": _v0(TS, b'{"ping": false}')})
    assert resp.status_code == 401


def test_rejects_wrong_signature(client):
    resp = _post(client, {"X-CIO-Timestamp": TS, "X-CIO-Signature": "00"})
    assert resp.status_code == 401


def test_rejects_missing_signature(client):
    resp = _post(client, {"X-CIO-Timestamp": TS})
    assert resp.status_code == 401


def test_body_only_signature_still_accepted_without_timestamp(client):
    """Manual/direct triggers sign the body alone and send no timestamp."""
    resp = _post(client, {"X-CIO-Signature": _sign(BODY)})
    assert resp.status_code == 200


def test_body_only_signature_rejected_when_timestamp_sent(client):
    """With a timestamp present only the v0 form counts, so a body-only
    signature cannot be replayed under an arbitrary timestamp."""
    resp = _post(client, {"X-CIO-Timestamp": TS, "X-CIO-Signature": _sign(BODY)})
    assert resp.status_code == 401
