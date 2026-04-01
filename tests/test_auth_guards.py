"""Tests for auth guards on sensitive endpoints and module-level constants."""

import os
import pytest
from unittest.mock import patch


class TestFreshMinScoreIsModuleLevel:
    """FRESH_MIN_SCORE must be accessible as a module-level constant.
    Previously it was defined inside the else-branch of 'if call_booked:', causing
    a NameError for booked+fresh leads with score < SCORE_WARM."""

    def test_fresh_min_score_importable(self):
        from batch.scorer import FRESH_MIN_SCORE
        assert FRESH_MIN_SCORE == 10

    def test_fresh_min_score_below_score_warm(self):
        from batch.scorer import FRESH_MIN_SCORE, SCORE_WARM
        assert FRESH_MIN_SCORE < SCORE_WARM, (
            "FRESH_MIN_SCORE must be below SCORE_WARM — fresh leads with 10-29 "
            "score need to reach Aircall even though they don't hit the warm threshold"
        )


class TestBatchRunAuthGuard:
    """POST /batch/run must require DEBUG_API_KEY — previously it had no auth check
    and any unauthenticated caller could trigger a full HubSpot/Supabase/Aircall batch."""

    def _get_client(self, debug_api_key: str):
        """Return a TestClient with the app patched to use the given DEBUG_API_KEY."""
        with patch.dict(os.environ, {
            "DEBUG_API_KEY": debug_api_key,
            "HUBSPOT_ACCESS_TOKEN": "test",
            "SUPABASE_URL": "http://localhost",
            "SUPABASE_SERVICE_KEY": "test",
            "AIRCALL_API_ID": "test",
            "AIRCALL_API_TOKEN": "test",
        }):
            # Import inside patch so env vars are set before module-level reads
            import importlib
            import main as main_module
            importlib.reload(main_module)
            from fastapi.testclient import TestClient
            return TestClient(main_module.app, raise_server_exceptions=False)

    def test_batch_run_rejects_missing_key(self):
        client = self._get_client("secret-key")
        resp = client.post("/batch/run")
        assert resp.status_code == 401

    def test_batch_run_rejects_wrong_key(self):
        client = self._get_client("secret-key")
        resp = client.post("/batch/run", headers={"X-Api-Key": "wrong"})
        assert resp.status_code == 401

    def test_batch_run_rejects_when_key_not_configured(self):
        """When DEBUG_API_KEY is not set, /batch/run must be locked (fail-closed)."""
        client = self._get_client("")  # empty = not configured
        resp = client.post("/batch/run", headers={"X-Api-Key": "anything"})
        assert resp.status_code == 401
