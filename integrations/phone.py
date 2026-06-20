"""
Phone validation + normalization via Google's libphonenumber (`phonenumbers`).

Used by the batch scorer before pushing a lead to the Aircall dialer:
valid / corrected numbers are dialed; invalid ones are flagged for manual fix.
"""
from __future__ import annotations

import re

import phonenumbers

# Gabriel's primary market — used to interpret national-format numbers without
# a country code (e.g. "044 668 18 00" -> "+41446681800").
DEFAULT_REGION = "CH"

_SEP_RE = re.compile(r"[\s\-()/.]")


def _predial_dach(stripped: str) -> str:
    """Normalize a DACH national-format number to +CC form when recognizable.

    Ported from the original _normalize_phone heuristics so that leads stored
    in national mobile format (without a country code) are not dropped:
      015x/016x/017x -> +49   (German mobile)
      07[5-9]x       -> +41   (Swiss mobile)
      06[5-9]x       -> +43   (Austrian mobile)
      00XX           -> +XX   (international prefix)
    Returns a "+"-prefixed string when a rule matches, otherwise the cleaned
    national digits (left for region-based parsing).
    """
    d = _SEP_RE.sub("", stripped)
    if d.startswith("+"):
        return d
    if d.startswith("00") and len(d) >= 5:
        return "+" + d[2:]
    if re.match(r"^01[5-7]\d{7,9}$", d):
        return "+49" + d[1:]
    if re.match(r"^07[5-9]\d{6,7}$", d):
        return "+41" + d[1:]
    if re.match(r"^06[5-9]\d{7,9}$", d):
        return "+43" + d[1:]
    return d


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

    # Pre-normalize DACH national formats (incl. 00 prefix) to +CC so phonenumbers
    # parses them without guessing the wrong region.
    to_parse = _predial_dach(stripped)
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
