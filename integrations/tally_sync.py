"""
Tally Eignungscheck -> HubSpot sync (W7).

Fetches completed Hypnose-Eignungscheck submissions, resolves each by email to a
HubSpot contact, and writes the normalized answers to the `lead_eig_*` contact
properties so the point-system scorer (and Kevin's Aircall card) can use them.

Read-only on Tally; writes ONLY the `lead_eig_*` properties on HubSpot contacts
that match by email. Non-matching submissions (quiz-takers who never became a
HubSpot contact) are skipped, not created.

Run:  TALLY_API_KEY=... HUBSPOT_ACCESS_TOKEN=... python -m integrations.tally_sync [--limit N] [--dry-run]
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx

from integrations.tally import fetch_submissions
from integrations.hubspot import _resolve_hubspot_id, update_contact_properties

logger = logging.getLogger(__name__)

_GOAL_MAXLEN = 500


def _extract_email(submission: dict, questions: list[dict]) -> str:
    """Pull the email answer from a submission (INPUT_EMAIL question or 'email' title)."""
    email_qids = {
        q.get("id")
        for q in questions
        if q.get("type") == "INPUT_EMAIL" or "email" in (q.get("title") or "").lower()
    }
    for resp in submission.get("responses", []):
        if resp.get("questionId") in email_qids:
            answer = resp.get("answer")
            if isinstance(answer, str) and "@" in answer:
                return answer.strip().lower()
    return ""


def props_from_mapped(mapped: dict) -> dict[str, str]:
    """Build the HubSpot lead_eig_* property dict from mapped answers (skip empties)."""
    props: dict[str, str] = {"lead_eig_consult": "true" if mapped.get("consult") else "false"}
    if mapped.get("budget"):
        props["lead_eig_budget"] = mapped["budget"]
    if mapped.get("interest"):
        props["lead_eig_interest"] = mapped["interest"]
    if mapped.get("goal"):
        props["lead_eig_goal"] = str(mapped["goal"])[:_GOAL_MAXLEN]
    if mapped.get("eig_score") is not None:
        props["lead_eig_score"] = str(mapped["eig_score"])
    return props


async def sync_eignungscheck(
    *,
    limit: int | None = None,
    dry_run: bool = False,
    max_scan: int | None = None,
) -> dict[str, Any]:
    """
    Sync Eignungscheck answers into HubSpot lead_eig_* props.

    limit: stop after this many successful writes (None = all). Use limit=1 for
           the 1-record safety test before a bulk run.
    dry_run: resolve + log but never write.
    max_scan: cap on submissions scanned (safety; None = all fetched).

    Returns {"stats": {...}, "written": [{email, cid, props}, ...]}.
    """
    items = await fetch_submissions()
    stats = {
        "submissions": len(items),
        "no_email": 0,
        "duplicate": 0,
        "no_match": 0,
        "written": 0,
        "errors": 0,
        "scanned": 0,
    }
    written: list[dict] = []
    seen: set[str] = set()

    async with httpx.AsyncClient(timeout=15.0) as client:
        for item in items:
            if limit is not None and stats["written"] >= limit:
                break
            if max_scan is not None and stats["scanned"] >= max_scan:
                break
            stats["scanned"] += 1

            email = _extract_email(item["submission"], item["questions"])
            if not email:
                stats["no_email"] += 1
                continue
            if email in seen:
                stats["duplicate"] += 1
                continue
            seen.add(email)

            props = props_from_mapped(item["mapped"])
            cid = await _resolve_hubspot_id(email, client)
            if not cid:
                stats["no_match"] += 1
                continue

            if dry_run:
                logger.info("DRY tally_sync %s -> contact %s %s", email, cid, props)
                stats["written"] += 1
                written.append({"email": email, "cid": cid, "props": props})
                continue

            ok = await update_contact_properties(cid, props)
            if ok:
                stats["written"] += 1
                written.append({"email": email, "cid": cid, "props": props})
                logger.info("tally_sync wrote %s -> contact %s", email, cid)
            else:
                stats["errors"] += 1
                logger.warning("tally_sync write FAILED %s -> contact %s", email, cid)

    return {"stats": stats, "written": written}


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    argv = sys.argv[1:]
    _limit = None
    if "--limit" in argv:
        _limit = int(argv[argv.index("--limit") + 1])
    _dry = "--dry-run" in argv
    result = asyncio.run(sync_eignungscheck(limit=_limit, dry_run=_dry, max_scan=200 if _limit else None))
    print("STATS:", result["stats"])
    for w in result["written"][:5]:
        print("WROTE:", w["email"], "->", w["cid"], w["props"])
