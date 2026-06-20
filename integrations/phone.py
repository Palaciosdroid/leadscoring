"""
Phone validation + normalization via Google's libphonenumber (`phonenumbers`).

Used by the batch scorer before pushing a lead to the Aircall dialer:
valid / corrected numbers are dialed; invalid ones are flagged for manual fix.
"""
from __future__ import annotations

import phonenumbers

# Gabriel's primary market — used to interpret national-format numbers without
# a country code (e.g. "044 668 18 00" -> "+41446681800").
DEFAULT_REGION = "CH"


def validate_and_normalize(
    raw: str, default_region: str = DEFAULT_REGION
) -> tuple[str | None, str]:
    """Return (e164, status) for a raw phone string.

    status:
      "valid"     -> raw was already a clean international number
      "corrected" -> made valid by normalization (region inferred, 00->+ , cleanup)
      "invalid"   -> could not be parsed into a valid number
    Returns (None, "invalid") when no valid number can be produced.
    """
    if not raw or not raw.strip():
        return None, "invalid"

    stripped = raw.strip().lstrip("'")          # drop Excel CSV apostrophe artefact
    compare = stripped.replace(" ", "")          # original minus cosmetic spaces

    # Convert a leading "00" international prefix to "+" ourselves — phonenumbers
    # only resolves "00" when a region with that IDD is supplied; we normalize it
    # up front so it parses without a region (standard IDD across DACH).
    to_parse = "+" + stripped[2:] if stripped.startswith("00") else stripped
    started_intl = to_parse.startswith("+")

    try:
        region = None if started_intl else default_region
        parsed = phonenumbers.parse(to_parse, region)
    except phonenumbers.NumberParseException:
        return None, "invalid"

    if not phonenumbers.is_valid_number(parsed):
        return None, "invalid"

    e164 = phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
    # Already-clean international input (only cosmetic spaces) counts as "valid";
    # anything we had to reshape (national form, 00 prefix) counts as "corrected".
    status = "valid" if compare == e164 else "corrected"
    return e164, status


def region_for(e164: str) -> str | None:
    """Return the ISO region code (e.g. 'CH', 'DE', 'AT') for an E.164 number."""
    try:
        parsed = phonenumbers.parse(e164, None)
    except phonenumbers.NumberParseException:
        return None
    return phonenumbers.region_code_for_number(parsed)
