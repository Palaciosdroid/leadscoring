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


def test_empty_contacts_yields_header_only():
    csv = _build_dialer_csv([])
    assert csv.strip() == "phone_number,first_name,last_name,tier,score,interest"


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
