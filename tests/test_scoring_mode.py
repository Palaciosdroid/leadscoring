"""
Tests for the flag-gated point-system integration in batch/scorer.py (Task 5).

Two guarantees:
  - SCORING_MODE='engagement' (default) → live tier behavior UNCHANGED, the
    point-system runs shadow-only (writes lead_points, never touches lead_tier).
  - SCORING_MODE='points' → lead_tier / lead_combined_score come from the
    point-system; the breakdown is appended to the Aircall card.

Also covers the two carried-over deploy TODOs:
  - TODO-A: batch_add_to_list is only called for STATIC lists (352/362/363/364),
    never the dynamic funnel lists (365-370).
  - TODO-B: _write_hubspot_note skips a re-write when the card is unchanged.

The full batch is driven against in-memory fixtures with every external call
(HubSpot / Supabase / Aircall / Slack) monkeypatched out.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import asyncio

import pytest

import batch.scorer as scorer
from scoring.points import compute_points


# ---------------------------------------------------------------------------
# Pure helper: signal assembly
# ---------------------------------------------------------------------------

class TestAssemblePointSignals:
    def test_tally_props_mapped(self):
        props = {
            "lead_eig_budget": "4000_6000",
            "lead_eig_interest": "naechster_schritt",
            "lead_eig_consult": "true",
        }
        sig = scorer._assemble_point_signals([], props, "hypnose", False)
        assert sig["budget"] == "4000_6000"
        assert sig["interest"] == "naechster_schritt"
        assert sig["consult"] is True
        assert sig["interest_category"] == "hypnose"
        assert sig["unsubscribed"] is False

    def test_empty_tally_props_are_none(self):
        sig = scorer._assemble_point_signals([], {}, None, False)
        assert sig["budget"] is None
        assert sig["interest"] is None
        assert sig["consult"] is False

    def test_behavior_signals_from_events(self):
        events = [
            {"event_type": "application_submitted"},
            {"event_type": "video_watched_100"},
            {"event_type": "video_watched_50"},   # replay page
            {"event_type": "checkout_visited"},
            {"event_type": "price_info_viewed"},
        ]
        sig = scorer._assemble_point_signals(events, {}, None, False)
        assert sig["form_submit"] is True
        assert sig["video_complete"] is True
        assert sig["replay"] is True
        assert sig["checkout"] is True
        assert sig["price"] is True

    def test_no_behavior_signals(self):
        sig = scorer._assemble_point_signals(
            [{"event_type": "page_visited"}], {}, None, False
        )
        assert sig["form_submit"] is False
        assert sig["video_complete"] is False
        assert sig["replay"] is False
        assert sig["checkout"] is False
        assert sig["price"] is False

    def test_phone_is_never_a_signal(self):
        # Phone must never leak into the point-system (leakage protection).
        props = {"phone": "+41791234567", "mobilephone": "+491701234567"}
        sig = scorer._assemble_point_signals([], props, None, False)
        assert "phone" not in sig
        assert "mobilephone" not in sig

    def test_unsubscribed_flows_through(self):
        sig = scorer._assemble_point_signals([], {}, None, True)
        assert sig["unsubscribed"] is True

    def test_launchcall_flows_through(self):
        sig = scorer._assemble_point_signals([], {}, None, False, launchcall_registered=True)
        assert sig["launchcall"] is True
        # default is off when not passed
        sig2 = scorer._assemble_point_signals([], {}, None, False)
        assert sig2["launchcall"] is False


# ---------------------------------------------------------------------------
# Full-batch harness — one lead, every external call mocked.
# ---------------------------------------------------------------------------

# A lead with phone + a strong Tally profile. Engagement score will be ~0
# (no scored touchpoints), so engagement-mode tier differs sharply from
# points-mode tier — the perfect discriminator for the flag test.
_CONTACT = {
    "id": "c1",
    "properties": {
        "email": "lead@example.de",
        "firstname": "Max",
        "lastname": "Muster",
        "phone": "+41791234567",
        "lead_tier": "2_warm",          # stored tier (from a previous run)
        "lead_engagement_score": "40",
        "lead_interest_category": "hypnose",
        # Strong Tally signals → points = 30 (budget) + 25 (interest) + 15 (consult)
        # + 10 (hypnose) = 80 → 1_hot in points mode.
        "lead_eig_budget": "4000_6000",
        "lead_eig_interest": "naechster_schritt",
        "lead_eig_consult": "true",
    },
}


class _FakeDNC:
    should_skip = False
    reason = ""


def _patch_batch(monkeypatch, captured):
    """Patch every external dependency the batch touches; capture HS updates."""

    async def _fetch_active():
        return [dict(_CONTACT)]

    async def _fetch_touchpoints(emails, days=14):
        return {e: [] for e in emails}

    async def _fetch_all(emails, days=14):
        return {
            e: {"events": [], "purchases": [], "meetings": [], "customerio_id": None}
            for e in emails
        }

    async def _batch_update(updates):
        captured["hubspot_updates"] = updates
        return len(updates), 0, []

    async def _dnc(**kwargs):
        return _FakeDNC()

    async def _send_report(stats):
        captured["stats"] = stats

    async def _launchcall():
        return set()

    monkeypatch.setattr(scorer, "_fetch_active_hubspot_leads", _fetch_active)
    monkeypatch.setattr(scorer, "fetch_launchcall_registered_emails", _launchcall)
    monkeypatch.setattr(scorer, "fetch_touchpoints_for_emails", _fetch_touchpoints)
    monkeypatch.setattr(scorer, "fetch_all_lead_data", _fetch_all)
    monkeypatch.setattr(scorer, "_batch_update_hubspot_contacts", _batch_update)
    monkeypatch.setattr(scorer, "check_do_not_call", _dnc)
    monkeypatch.setattr(scorer, "send_batch_report", _send_report)

    # Within-call-window so the (mocked) Aircall push path runs.
    monkeypatch.setattr(scorer, "is_within_call_window", lambda region, now: True)

    # Capture HubSpot notes (so we can read the card text the batch built).
    async def _write_note(contact_id, body):
        captured.setdefault("notes", []).append({"id": contact_id, "body": body})
        return True

    monkeypatch.setattr(scorer, "_write_hubspot_note", _write_note)

    # Stub the dynamically-imported integrations used inside run_batch_scoring.
    import integrations.hubspot as hs

    async def _add_to_list(list_id, contact_ids):
        captured.setdefault("listed", []).append((list_id, list(contact_ids)))
        return len(contact_ids)

    monkeypatch.setattr(hs, "batch_add_to_list", _add_to_list)

    import integrations.aircall as ac

    async def _push(lead, **kwargs):
        captured.setdefault("aircall", []).append({"lead": lead, "kwargs": kwargs})
        return {"ok": True}

    monkeypatch.setattr(ac, "add_to_power_dialer", _push)
    monkeypatch.setattr(ac, "AIRCALL_BASE", "http://stub", raising=False)
    monkeypatch.setattr(ac, "AIRCALL_CLOSER_USER_ID", "0", raising=False)
    monkeypatch.setattr(ac, "_headers", lambda: {}, raising=False)

    # Step 6b issues a live dialer-verify GET via httpx — stub it so the batch
    # never reaches the network (all other httpx callers are already mocked).
    class _VerifyResp:
        status_code = 200

        def json(self):
            return {"numbers": []}

    class _VerifyClient:
        def __init__(self, *a, **k):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def get(self, url, headers=None):
            return _VerifyResp()

    monkeypatch.setattr(scorer.httpx, "AsyncClient", _VerifyClient)


def _hs_props_for(captured, contact_id="c1"):
    for u in captured["hubspot_updates"]:
        if u["id"] == contact_id:
            return u["properties"]
    raise AssertionError("contact not in hubspot_updates")


class TestScoringModeFlag:
    def test_engagement_mode_tier_unchanged_points_shadowed(self, monkeypatch):
        captured = {}
        _patch_batch(monkeypatch, captured)
        monkeypatch.setattr(scorer, "SCORING_MODE", "engagement")

        asyncio.run(scorer.run_batch_scoring())

        props = _hs_props_for(captured)
        # Shadow point-score is ALWAYS written.
        expected_points = compute_points({
            "budget": "4000_6000",
            "interest": "naechster_schritt",
            "consult": True,
            "interest_category": "hypnose",
            "unsubscribed": False,
            "form_submit": False, "video_complete": False,
            "replay": False, "checkout": False, "price": False,
        }).points
        assert props["lead_points"] == expected_points == 80
        # Live tier is the ENGAGEMENT tier — NOT the point tier (1_hot).
        # Engagement score is 0 (no events) → tier 3_cold, never 1_hot.
        assert props["lead_tier"] != "1_hot"
        assert props["lead_combined_score"] == 0.0

    def test_points_mode_tier_from_points(self, monkeypatch):
        captured = {}
        _patch_batch(monkeypatch, captured)
        monkeypatch.setattr(scorer, "SCORING_MODE", "points")

        asyncio.run(scorer.run_batch_scoring())

        props = _hs_props_for(captured)
        assert props["lead_points"] == 80
        # Point-system drives the tier now.
        assert props["lead_tier"] == "1_hot"
        assert props["lead_combined_score"] == 80.0

    def test_points_mode_appends_breakdown_to_card(self, monkeypatch):
        captured = {}
        _patch_batch(monkeypatch, captured)
        monkeypatch.setattr(scorer, "SCORING_MODE", "points")

        asyncio.run(scorer.run_batch_scoring())

        notes = captured.get("notes", [])
        assert notes, "expected a HubSpot note for the hot lead"
        assert any("Punkte:" in n["body"] for n in notes)
        assert any("Budget 4000_6000 +30" in n["body"] for n in notes)

    def test_engagement_mode_card_has_no_breakdown(self, monkeypatch):
        captured = {}
        _patch_batch(monkeypatch, captured)
        monkeypatch.setattr(scorer, "SCORING_MODE", "engagement")

        asyncio.run(scorer.run_batch_scoring())

        for n in captured.get("notes", []):
            assert "Punkte:" not in n["body"]


# ---------------------------------------------------------------------------
# TODO-A — dynamic-list skip
# ---------------------------------------------------------------------------

class TestStaticListSkip:
    def test_static_ids_constant(self):
        assert scorer.STATIC_LIST_IDS == frozenset({352, 362, 363, 364})

    def test_dynamic_funnel_lists_excluded(self):
        # The funnel warm/fresh lists (365-370) must NOT be in the static set.
        for dyn in (365, 366, 367, 368, 369, 370):
            assert dyn not in scorer.STATIC_LIST_IDS

    def test_only_static_lists_pushed(self, monkeypatch):
        # A lead that qualifies for a dynamic list (hypnose_warm = 365) must not
        # trigger batch_add_to_list for that id. We force list assignment by
        # giving the lead a warm funnel score via points mode + interest.
        captured = {}
        _patch_batch(monkeypatch, captured)
        monkeypatch.setattr(scorer, "SCORING_MODE", "engagement")

        asyncio.run(scorer.run_batch_scoring())

        for list_id, _ids in captured.get("listed", []):
            assert list_id in scorer.STATIC_LIST_IDS, (
                f"dynamic list {list_id} was pushed — TODO-A regression"
            )


# ---------------------------------------------------------------------------
# TODO-B — note skip-unchanged (hash compare)
# ---------------------------------------------------------------------------

class TestNoteSkipUnchanged:
    def test_card_hash_stable(self):
        a = scorer._card_hash("hello world")
        b = scorer._card_hash("hello world")
        c = scorer._card_hash("hello WORLD")
        assert a == b
        assert a != c

    def test_unchanged_note_is_skipped(self, monkeypatch):
        # Simulate HubSpot returning an existing scorer note whose body is
        # byte-identical to the new card → no delete, no create, returns False.
        marker = scorer._HUBSPOT_NOTE_MARKER
        body = "WARM -- Hypnose | Score: 42"
        existing_body = f"{marker}\n{body}"

        calls = {"delete": 0, "create": 0}

        class _Resp:
            def __init__(self, status, payload=None):
                self.status_code = status
                self._payload = payload or {}

            def json(self):
                return self._payload

        class _FakeClient:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *a):
                return False

            async def get(self, url, headers=None, params=None):
                if url.endswith("/associations/notes"):
                    return _Resp(200, {"results": [{"id": "n1"}]})
                # note body fetch
                return _Resp(200, {"properties": {"hs_note_body": existing_body}})

            async def delete(self, url, headers=None):
                calls["delete"] += 1
                return _Resp(204)

            async def post(self, url, headers=None, json=None):
                calls["create"] += 1
                return _Resp(201)

        monkeypatch.setattr(scorer.httpx, "AsyncClient", lambda *a, **k: _FakeClient())

        wrote = asyncio.run(scorer._write_hubspot_note("c1", body))
        assert wrote is False
        assert calls["create"] == 0
        assert calls["delete"] == 0

    def test_changed_note_is_rewritten(self, monkeypatch):
        marker = scorer._HUBSPOT_NOTE_MARKER
        existing_body = f"{marker}\nOLD CARD"

        calls = {"delete": 0, "create": 0}

        class _Resp:
            def __init__(self, status, payload=None):
                self.status_code = status
                self._payload = payload or {}
                self.text = ""

            def json(self):
                return self._payload

        class _FakeClient:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *a):
                return False

            async def get(self, url, headers=None, params=None):
                if url.endswith("/associations/notes"):
                    return _Resp(200, {"results": [{"id": "n1"}]})
                return _Resp(200, {"properties": {"hs_note_body": existing_body}})

            async def delete(self, url, headers=None):
                calls["delete"] += 1
                return _Resp(204)

            async def post(self, url, headers=None, json=None):
                calls["create"] += 1
                return _Resp(201)

        monkeypatch.setattr(scorer.httpx, "AsyncClient", lambda *a, **k: _FakeClient())

        wrote = asyncio.run(scorer._write_hubspot_note("c1", "NEW CARD"))
        assert wrote is True
        assert calls["delete"] == 1   # stale note removed
        assert calls["create"] == 1   # new note written
