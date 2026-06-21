"""Tests for Tally Eignungscheck answer mapping (form nPJzEe)."""

from integrations.tally import (
    TALLY_FORM_ID,
    map_eignungscheck,
    _answer_text,
    _normalize_budget,
    _normalize_interest,
)


# Realistic form-level questions (titles drive the mapping, IDs are arbitrary).
QUESTIONS = [
    {"id": "qEMAIL", "type": "INPUT_EMAIL", "title": "E-Mail-Adresse"},
    {"id": "qBUDGET", "type": "MULTIPLE_CHOICE", "title": "Wie hoch ist dein Budget?"},
    {"id": "qINTEREST", "type": "MULTIPLE_CHOICE", "title": "Wie gross ist dein Interesse?"},
    {"id": "qCONSULT", "type": "MULTIPLE_CHOICE", "title": "Moechtest du dich beraten lassen?"},
    {"id": "qGOAL", "type": "TEXTAREA", "title": "Was ist dein groesstes Ziel?"},
    {"id": "qSCORE", "type": "LINEAR_SCALE", "title": "Eignungs-Punkte (0-10)"},
]


def _submission(responses):
    return {
        "id": "subABC",
        "formId": TALLY_FORM_ID,
        "isCompleted": True,
        "submittedAt": "2026-06-01T09:00:00.000Z",
        "responses": responses,
    }


def test_form_id_constant():
    assert TALLY_FORM_ID == "nPJzEe"


def test_maps_full_submission():
    sub = _submission([
        {"questionId": "qEMAIL", "answer": "lead@example.com"},
        {"questionId": "qBUDGET", "answer": "4000 - 6000 CHF"},
        {"questionId": "qINTEREST", "answer": "Ja, der richtige naechster Schritt fuer mich"},
        {"questionId": "qCONSULT", "answer": "Ja, gerne!"},
        {"questionId": "qGOAL", "answer": "Eigene Praxis aufbauen"},
        {"questionId": "qSCORE", "answer": 8},
    ])
    mapped = map_eignungscheck(sub["responses"], QUESTIONS)
    assert mapped == {
        "budget": "4000_6000",
        "interest": "naechster_schritt",
        "consult": True,
        "goal": "Eigene Praxis aufbauen",
        "eig_score": 8,
    }


def test_budget_enums():
    assert _normalize_budget("unter 2000 chf") == "unter_2000"
    assert _normalize_budget("2000 - 4000") == "2000_4000"
    assert _normalize_budget("4.000 bis 6.000") == "4000_6000"
    assert _normalize_budget("6000 - 8000 chf") == "6000_8000"
    assert _normalize_budget("keine angabe") is None


def test_interest_enums():
    assert _normalize_interest("der richtige naechster schritt") == "naechster_schritt"
    assert _normalize_interest("grundsätzlich interessiert") == "grundsaetzlich"
    assert _normalize_interest("gar nicht interessiert") == "keines"


def test_consult_false_on_no():
    sub = _submission([{"questionId": "qCONSULT", "answer": "Nein, danke"}])
    mapped = map_eignungscheck(sub["responses"], QUESTIONS)
    assert mapped["consult"] is False


def test_missing_answers_default_safely():
    # Only email answered — everything else defaults, no crash.
    sub = _submission([{"questionId": "qEMAIL", "answer": "x@y.com"}])
    mapped = map_eignungscheck(sub["responses"], QUESTIONS)
    assert mapped["budget"] is None
    assert mapped["interest"] is None
    assert mapped["consult"] is False
    assert mapped["goal"] == ""
    assert mapped["eig_score"] is None


def test_unknown_question_id_ignored():
    sub = _submission([{"questionId": "qGHOST", "answer": "whatever"}])
    mapped = map_eignungscheck(sub["responses"], QUESTIONS)
    assert mapped["budget"] is None


def test_choice_answer_as_list():
    # Multiple-choice answers can arrive as a list of labels.
    sub = _submission([{"questionId": "qBUDGET", "answer": ["6000 - 8000 CHF"]}])
    mapped = map_eignungscheck(sub["responses"], QUESTIONS)
    assert mapped["budget"] == "6000_8000"


def test_choice_answer_as_dict():
    # Or as a {"value": [...]} object.
    sub = _submission([
        {"questionId": "qINTEREST", "answer": {"value": ["grundsätzlich interessiert"]}},
    ])
    mapped = map_eignungscheck(sub["responses"], QUESTIONS)
    assert mapped["interest"] == "grundsaetzlich"


def test_eig_score_non_numeric_is_none():
    sub = _submission([{"questionId": "qSCORE", "answer": "weiss nicht"}])
    mapped = map_eignungscheck(sub["responses"], QUESTIONS)
    assert mapped["eig_score"] is None


def test_answer_text_helpers():
    assert _answer_text(True) == "ja"
    assert _answer_text(False) == "nein"
    assert _answer_text(None) == ""
    assert _answer_text(["A", "B"]) == "a b"
    assert _answer_text(7) == "7"
