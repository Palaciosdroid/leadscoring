from integrations.phone import validate_and_normalize, region_for


def test_clean_international_number_is_valid():
    assert validate_and_normalize("+41446681800") == ("+41446681800", "valid")


def test_spaces_in_international_number_still_valid():
    assert validate_and_normalize("+41 44 668 18 00") == ("+41446681800", "valid")


def test_national_format_is_corrected_with_default_region():
    assert validate_and_normalize("044 668 18 00") == ("+41446681800", "corrected")


def test_double_zero_prefix_is_corrected():
    assert validate_and_normalize("0041446681800") == ("+41446681800", "corrected")


def test_apostrophe_artefact_is_stripped():
    e164, status = validate_and_normalize("'+41 44 668 18 00")
    assert e164 == "+41446681800"
    assert status in ("valid", "corrected")


def test_too_short_is_invalid():
    assert validate_and_normalize("123") == (None, "invalid")


def test_empty_is_invalid():
    assert validate_and_normalize("") == (None, "invalid")


def test_garbage_is_invalid():
    assert validate_and_normalize("keine nummer") == (None, "invalid")


def test_region_for_returns_iso_code():
    assert region_for("+41446681800") == "CH"


def test_region_for_invalid_returns_none():
    assert region_for("nonsense") is None
