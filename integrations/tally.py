"""
Tally API Integration — Hypnose-Eignungscheck (form nPJzEe).

Fetches completed Eignungscheck submissions (read-only) and maps the answers
to normalized signals for the lead scorer. Mapping is by QUESTION TITLE
keyword, not by question ID, so it survives Tally form edits (ID drift).

Env vars required:
    TALLY_API_KEY    — Tally personal API token (Bearer auth)

A User-Agent header is required by the Tally API gateway (403 without one).
"""

import logging
import os
import re
from typing import Any

import httpx

logger = logging.getLogger(__name__)

TALLY_API_BASE = os.environ.get("TALLY_API_BASE", "https://api.tally.so")
TALLY_API_KEY = os.environ.get("TALLY_API_KEY", "")
TALLY_FORM_ID = "nPJzEe"

_USER_AGENT = "sbc-lead-scoring/1.0"

# Interest answer text fragment -> normalized enum.
_INTEREST_ENUMS: list[tuple[str, str]] = [
    ("nächster schritt", "naechster_schritt"),
    ("naechster schritt", "naechster_schritt"),
    ("richtige", "naechster_schritt"),
    ("grundsätzlich", "grundsaetzlich"),
    ("grundsaetzlich", "grundsaetzlich"),
    ("gar nicht", "keines"),
    ("kein", "keines"),
]


def _headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {TALLY_API_KEY}",
        "Content-Type": "application/json",
        "User-Agent": _USER_AGENT,
    }


def _answer_text(answer: Any) -> str:
    """
    Reduce a Tally answer value (string / number / bool / list / dict) to a
    single lowercase text blob for keyword matching.

    Multiple-choice answers may arrive as a list of option labels or a dict.
    """
    if answer is None:
        return ""
    if isinstance(answer, bool):
        return "ja" if answer else "nein"
    if isinstance(answer, (str, int, float)):
        return str(answer).strip().lower()
    if isinstance(answer, list):
        return " ".join(_answer_text(a) for a in answer).strip()
    if isinstance(answer, dict):
        # Tally choice answers can be {"value": [...], "raw": ...} or
        # {"id": ..., "text": ...}. Pull the human-readable bits.
        parts = []
        for key in ("value", "text", "label", "title", "raw"):
            if key in answer:
                parts.append(_answer_text(answer[key]))
        return " ".join(p for p in parts if p).strip()
    return ""


def _normalize_budget(text: str) -> str | None:
    """
    Bucket a budget answer by its LOWER bound. Tally labels are German
    free-text ranges ("4000 - 6000 CHF", "Unter 2000"); we read the first
    number (or "unter") so ranges map unambiguously regardless of formatting.
    """
    if "unter" in text:
        return "unter_2000"
    # First number in the string = lower bound. Strip thousands separators.
    match = re.search(r"\d[\d.\s]*", text.replace("'", ""))
    if not match:
        return None
    digits = re.sub(r"[.\s]", "", match.group(0))
    try:
        lower = int(digits)
    except ValueError:
        return None
    if lower >= 6000:
        return "6000_8000"
    if lower >= 4000:
        return "4000_6000"
    if lower >= 2000:
        return "2000_4000"
    return "unter_2000"


def _normalize_interest(text: str) -> str | None:
    for fragment, enum in _INTEREST_ENUMS:
        if fragment in text:
            return enum
    return None


def _normalize_consult(text: str) -> bool:
    # "Ja, gerne!" / "Ja" -> True; everything else -> False.
    return "ja" in text


def map_eignungscheck(responses: list[dict], questions: list[dict]) -> dict:
    """
    Map a single submission's responses to normalized scoring signals.

    Args:
        responses: submission["responses"] — list of {"questionId", "answer"}.
        questions: form-level "questions" list — {"id", "title", "type"}.

    Returns dict with:
        budget   — enum unter_2000 | 2000_4000 | 4000_6000 | 6000_8000 | None
        interest — enum keines | grundsaetzlich | naechster_schritt | None
        consult  — bool
        goal     — free-text goal answer (str)
        eig_score — int self-assessment score, or None

    Matching is by question TITLE keyword (robust to question-ID drift):
        budget   <- title contains "budget"
        interest <- title contains "interesse"
        consult  <- title contains "beraten"
        goal     <- title contains "ziel"
        eig_score <- title contains "punkt" or "score" (numeric self-rating)
    """
    title_by_id: dict[str, str] = {
        q.get("id", ""): (q.get("title") or "").lower()
        for q in questions
    }

    result: dict[str, Any] = {
        "budget": None,
        "interest": None,
        "consult": False,
        "goal": "",
        "eig_score": None,
    }

    for resp in responses:
        title = title_by_id.get(resp.get("questionId", ""), "")
        if not title:
            continue
        answer = resp.get("answer")
        text = _answer_text(answer)

        if "budget" in title:
            result["budget"] = _normalize_budget(text)
        elif "interesse" in title:
            result["interest"] = _normalize_interest(text)
        elif "beraten" in title:
            result["consult"] = _normalize_consult(text)
        elif "ziel" in title:
            # Keep the original (non-lowercased) free text for the card.
            result["goal"] = (
                answer.strip() if isinstance(answer, str) else _answer_text(answer)
            )
        elif "punkt" in title or "score" in title:
            try:
                result["eig_score"] = int(float(text)) if text else None
            except (ValueError, TypeError):
                result["eig_score"] = None

    return result


async def fetch_submissions(
    form_id: str = TALLY_FORM_ID,
    *,
    max_pages: int = 50,
) -> list[dict]:
    """
    Fetch all COMPLETED submissions for a Tally form (read-only, paginated).

    Returns a list of dicts, one per submission:
        {"submission": <submission obj>, "questions": <form questions>,
         "mapped": <map_eignungscheck output>}

    Fail-soft: returns whatever was collected before an error; never raises
    for an individual page failure.
    """
    if not TALLY_API_KEY:
        logger.warning("Tally: TALLY_API_KEY not set — skipping submission fetch")
        return []

    collected: list[dict] = []
    page = 1

    async with httpx.AsyncClient(timeout=20.0) as client:
        while page <= max_pages:
            try:
                r = await client.get(
                    f"{TALLY_API_BASE}/forms/{form_id}/submissions",
                    headers=_headers(),
                    params={"filter": "completed", "page": page},
                )
            except httpx.HTTPError as exc:
                logger.error("Tally: request error on page %d: %s", page, exc)
                break

            if r.status_code != 200:
                logger.error(
                    "Tally: submissions fetch failed (page %d): %s %s",
                    page, r.status_code, r.text[:300],
                )
                break

            data = r.json()
            questions = data.get("questions", [])
            submissions = data.get("submissions", [])

            for sub in submissions:
                mapped = map_eignungscheck(sub.get("responses", []), questions)
                collected.append(
                    {"submission": sub, "questions": questions, "mapped": mapped}
                )

            if not data.get("hasMore") or not submissions:
                break
            page += 1

    logger.info(
        "Tally: fetched %d completed submission(s) for form %s",
        len(collected), form_id,
    )
    return collected
