"""Tests for the bookmarkable dialer CSV export helper (Aircall Power Dialer).

Aircall Workspace Power Dialer reads ONLY column A (phone, E.164, with header).
These tests guard that contract: E.164-only, deduped, priority order preserved.
"""
from main import _build_dialer_csv


def _c(phone, **props):
    return {"properties": {"phone": phone, **props}}


def test_header_and_e164_phone_first():
    csv = _build_dialer_csv([_c("+41791234567", firstname="Ina", lastname="Muster",
                                lead_tier="1_hot", lead_combined_score="80")])
    lines = csv.strip().splitlines()
    assert lines[0].split(",")[0] == "phone_number"   # Aircall reads column A
    assert lines[1].startswith("+41791234567,")
    assert "Ina" in lines[1]


def test_non_e164_dropped():
    # number without '+' (not E.164) -> Aircall rejects -> drop it
    csv = _build_dialer_csv([_c("0041791234567"), _c("+4915112345678")])
    body = csv.strip().splitlines()[1:]
    assert len(body) == 1
    assert body[0].startswith("+4915112345678")


def test_malformed_e164_dropped():
    # too-short / empty after '+' -> Aircall rejects (crashed the live bulk import) -> drop
    csv = _build_dialer_csv([_c("+41"), _c("+"), _c("+49"), _c("+436876791337FN"),
                             _c("+41794674925")])
    body = csv.strip().splitlines()[1:]
    # "+41"/"+"/"+49" dropped; "+436876791337FN" keeps digits -> valid; "+41794674925" valid
    assert all(not r.startswith(("+41,", "+,", "+49,")) for r in body)
    phones = [r.split(",")[0] for r in body]
    assert "+436876791337" in phones
    assert "+41794674925" in phones
    assert "+41" not in phones and "+" not in phones and "+49" not in phones


def test_dedup_by_normalized_phone():
    csv = _build_dialer_csv([_c("+41 79 123 45 67"), _c("+41791234567")])
    body = csv.strip().splitlines()[1:]
    assert len(body) == 1  # same number after whitespace strip


EXPECTED_HEADER = (
    "phone_number,first_name,last_name,tier,score,interest,intent_funnel,"
    "engagement_level,payment_page_visited,workshop_registered,"
    "masterclass_pct,survey_einwand,offer_dwell_min"
)


def test_empty_contacts_yields_header_only():
    csv = _build_dialer_csv([])
    assert csv.strip() == EXPECTED_HEADER


def test_intent_funnel_position_and_inert_when_missing():
    # PostHog-sync contract (posthog-CC, 20.07): intent_funnel is display/routing
    # only; new columns are appended AFTER it so Aircall's column-A phone
    # contract and Kevin's existing column order stay untouched. Absent
    # property -> empty cell, never a crash.
    with_funnel = _build_dialer_csv(
        [_c("+41791234567", intent_funnel="AL (Ausbildung deines Lebens)")]
    ).strip().splitlines()
    assert with_funnel[0].split(",")[0] == "phone_number"          # column A unchanged
    assert with_funnel[0].split(",")[6] == "intent_funnel"         # position stable
    assert with_funnel[1].startswith("+41791234567,")
    assert "AL (Ausbildung deines Lebens)" in with_funnel[1]

    without = _build_dialer_csv([_c("+41791234567")]).strip().splitlines()
    assert without[1].startswith("+41791234567,")
    assert without[1].endswith(",")                                 # empty trailing cells


def test_behavior_signal_columns_display_only():
    # 24.07: behaviour signals (posthog daily sync) as trailing DISPLAY columns —
    # call-prep context for Kevin. Sort/score influence is contractually deferred
    # to the 17.08 re-calibration (feature/posthog-signal-points, flag off).
    rows = _build_dialer_csv([_c(
        "+41791234567",
        engagement_level="hot",
        payment_page_visited="2026-07-23",
        masterclass_watched_percent="100",
        survey_objection_last="Preis",
        offer_dwell_minutes="7",
    )]).strip().splitlines()
    header = rows[0].split(",")
    row = rows[1].split(",")
    assert header[-6:] == ["engagement_level", "payment_page_visited",
                           "workshop_registered", "masterclass_pct",
                           "survey_einwand", "offer_dwell_min"]
    assert row[header.index("engagement_level")] == "hot"
    assert row[header.index("payment_page_visited")] == "2026-07-23"
    assert row[header.index("workshop_registered")] == ""            # sparse = empty
    assert row[header.index("masterclass_pct")] == "100"
    assert row[header.index("survey_einwand")] == "Preis"
    assert row[header.index("offer_dwell_min")] == "7"


def test_priority_input_order_preserved():
    csv = _build_dialer_csv([_c("+41791111111", lead_tier="1_hot"),
                             _c("+41792222222", lead_tier="2_warm")])
    body = csv.strip().splitlines()[1:]
    assert body[0].startswith("+41791111111")  # hot first (caller already sorted)
    assert body[1].startswith("+41792222222")


# --- Number-level exclusion (duplicate-contact leak, 07.07) ------------------

def test_csv_drops_excluded_numbers_full_and_suffix():
    from main import _build_dialer_csv
    contacts = [
        {"properties": {"phone": "+491722647346", "firstname": "Silke", "lastname": "Selent",
                        "lead_tier": "2_warm", "lead_combined_score": "40", "lead_interest_category": "hypnose"}},
        {"properties": {"phone": "+41791234567", "firstname": "Clean", "lastname": "Lead",
                        "lead_tier": "1_hot", "lead_combined_score": "90", "lead_interest_category": "hypnose"}},
    ]
    # excluded via full digits (as stored on the paused duplicate contact)
    csv_text = _build_dialer_csv(contacts, excluded_digits={"491722647346", "722647346"})
    assert "+491722647346" not in csv_text
    assert "+41791234567" in csv_text


def test_csv_without_exclusions_unchanged():
    from main import _build_dialer_csv
    contacts = [{"properties": {"phone": "+41791234567", "firstname": "A", "lastname": "B",
                                "lead_tier": "1_hot", "lead_combined_score": "90",
                                "lead_interest_category": "hypnose"}}]
    assert "+41791234567" in _build_dialer_csv(contacts)


# --- get_prioritized_contacts pagination + score-sort (audit fix 09.07) ------

import pytest
from unittest.mock import patch, AsyncMock, MagicMock


def _mk_response(results, after=None):
    r = MagicMock()
    r.status_code = 200
    paging = {"next": {"after": after}} if after else {}
    r.json = MagicMock(return_value={"results": results, "paging": paging})
    return r


@pytest.mark.asyncio
async def test_get_prioritized_paginates_full_pool_and_sorts_by_score():
    """Cursor must page past 100/tier (the audit bug) and output must be
    score-DESC despite being fetched in hs_object_id order; paused rows drop."""
    import integrations.hubspot as hs

    # Two pages for the FIRST tier fetched, empty for the others.
    page1 = [{"id": str(i), "properties": {"phone": f"+4179000{i:04d}",
              "lead_combined_score": str(i % 50), "lead_tier": "1_hot"}} for i in range(100)]
    # include a paused contact that must be filtered out
    page1.append({"id": "999", "properties": {"phone": "+41790009999",
                  "lead_combined_score": "99", "lead_tier": "1_hot",
                  "lead_pause_until": "2099-01-01T00:00:00+00:00"}})
    page2 = [{"id": str(i), "properties": {"phone": f"+4179111{i:04d}",
              "lead_combined_score": str(i), "lead_tier": "1_hot"}} for i in range(100, 140)]

    calls = {"n": 0}
    async def fake_post(url, headers=None, json=None):
        # only the hot tier gets 2 pages; warm/cold return empty immediately
        is_hot = any(f.get("value") == "1_hot" for g in json["filterGroups"] for f in g["filters"])
        if not is_hot:
            return _mk_response([])
        if "after" not in json:
            return _mk_response(page1, after="CURSOR")
        return _mk_response(page2)

    client = MagicMock()
    client.post = AsyncMock(side_effect=fake_post)
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=False)

    with patch.object(hs, "ACCESS_TOKEN", "tok"), \
         patch.object(hs.httpx, "AsyncClient", return_value=client):
        out = await hs.get_prioritized_contacts(limit=1000)

    # page1(100)+page2(40) non-paused kept; the 1 paused row filtered (proves paging past page 1)
    assert len(out) == 140
    scores = [float(c["properties"]["lead_combined_score"]) for c in out]
    assert scores == sorted(scores, reverse=True)      # score-DESC restored
    assert all("2099" not in (c["properties"].get("lead_pause_until") or "") for c in out)  # paused dropped
