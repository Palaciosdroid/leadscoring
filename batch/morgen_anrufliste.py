"""Morning call list for Kevin, posted to the Palacios Base chat.

Every weekday at 07:00 Europe/Zurich: members of four static HubSpot lists
(Telly-Abbruch MC/GC/HC, Facebook EC) that were never called since they joined
the list, or were not reached and are due again after RETRY_DAYS calendar days.

Rules (briefing 02.10.2026, binding):
  - only OUTBOUND calls since the list join count
  - meeting booked (lead_call_booked or last meeting booked after join) -> out
  - no call since join                                  -> NEU
  - last call reached / wrong number / final outcome    -> out
  - MAX_ATTEMPTS calls since join                       -> out
  - else last call day <= today - RETRY_DAYS (Zurich)   -> WIEDERVORLAGE
On top, every candidate passes the shared dialer gate (dialer_suppressed), so
the list never routes around a pause, removal, DNC or not-interested flag.

Delivery: POST {BASE_URL}/api/webhooks/channel (X-API-Key) as Concierge bot.
Scheduled only when ANRUFLISTE_ENABLED=1.
"""

import asyncio
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import httpx

from batch.dialer_gate import dialer_suppressed
from integrations.hubspot import HUBSPOT_BASE, _headers

logger = logging.getLogger(__name__)

ZONE = ZoneInfo("Europe/Zurich")
PORTAL_ID = os.environ.get("HUBSPOT_PORTAL_ID", "") or "27034546"

LIST_IDS = [s.strip() for s in os.environ.get("ANRUFLISTE_LIST_IDS", "492,491,445,433").split(",") if s.strip()]
MAX_ATTEMPTS = int(os.environ.get("ANRUFLISTE_MAX_ATTEMPTS", "4"))
RETRY_DAYS = int(os.environ.get("ANRUFLISTE_RETRY_DAYS", "3"))

# Disposition GUID -> (label, final). "final" = person reached or number useless.
DISPOSITIONS: dict[str, tuple[str, bool]] = {
    "f240bbac-87c9-4f6e-bf70-924b57d47db7": ("Erreicht", True),
    "17b47fee-58de-441e-a44c-c6300d46f273": ("Falsche Nummer", True),
    "9d9162e7-6cf3-4944-bf63-4dff82258764": ("Besetzt", False),
    "73a0d17f-1163-4015-bdd5-ec830791da20": ("Keine Antwort", False),
    "b2cf5968-551e-4856-9783-52b3da59a7d0": ("Mailbox", False),
    # Not in the briefing, but they mean "reached" in this portal
    # (integrations/hubspot.CONNECTED_DISPOSITIONS, disposition set of 24.09.).
    "a4c4c377-d246-4b32-a13b-75a56a4cd0ff": ("Live-Nachricht hinterlassen", True),
    "3486e24c-aaec-42ce-862e-a5acdb2b5fb3": ("Nicht interessiert", True),
}


def disposition_label(guid: str | None) -> str:
    return DISPOSITIONS.get(guid or "", ("unbekannt", False))[0]


def to_dt(value: Any) -> datetime | None:
    """HubSpot timestamps come as ISO strings or epoch millis."""
    if value in (None, ""):
        return None
    s = str(value).strip()
    try:
        if s.isdigit():
            return datetime.fromtimestamp(int(s) / 1000, tz=ZONE)
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None


def _truthy(value: Any) -> bool:
    return str(value).strip().lower() in ("true", "1", "yes", "ja")


@dataclass
class Result:
    status: str | None  # "neu" | "wiedervorlage" | None
    reason: str
    attempts: int = 0
    last_call: dict | None = None


def evaluate(contact: dict, joined_at: datetime, calls: list[dict], today: datetime,
             max_attempts: int = MAX_ATTEMPTS, retry_days: int = RETRY_DAYS) -> Result:
    """Pure status decision for one list membership."""
    if _truthy(contact.get("lead_call_booked")):
        return Result(None, "termin_gebucht")
    meeting = to_dt(contact.get("engagements_last_meeting_booked"))
    if meeting and meeting >= joined_at:
        return Result(None, "termin_nach_eintritt")

    relevant = []
    for c in calls:
        ts = to_dt(c.get("hs_timestamp"))
        if c.get("hs_call_direction") == "OUTBOUND" and ts and ts >= joined_at:
            relevant.append((ts, c))
    relevant.sort(key=lambda x: x[0])
    attempts = len(relevant)
    if attempts == 0:
        return Result("neu", "kein_anruf")

    last_ts, last = relevant[-1]
    last = {**last, "_ts": last_ts}
    if DISPOSITIONS.get(last.get("hs_call_disposition") or "", ("", False))[1]:
        return Result(None, "abgeschlossen", attempts, last)
    if attempts >= max_attempts:
        return Result(None, "obergrenze", attempts, last)

    last_day = last_ts.astimezone(ZONE).date()
    if last_day <= today.astimezone(ZONE).date() - timedelta(days=retry_days):
        return Result("wiedervorlage", "faellig", attempts, last)
    return Result(None, "noch_nicht_faellig", attempts, last)


@dataclass
class Row:
    contact_id: str
    joined_at: datetime
    contact: dict
    result: Result
    also_in: list[str] = field(default_factory=list)


def build_sections(lists: list[dict], contacts: dict[str, dict], calls_by_contact: dict[str, list[dict]],
                   today: datetime, suppressed: set[str] | None = None) -> list[dict]:
    """One section per list with hits, rows longest-waiting first, 'auch in' hints."""
    suppressed = suppressed or set()
    sections = []
    for lst in lists:
        rows = []
        for m in lst["members"]:
            cid = m["contact_id"]
            if cid in suppressed or cid not in contacts:
                continue
            r = evaluate(contacts[cid], m["joined_at"], calls_by_contact.get(cid, []), today)
            if r.status:
                rows.append(Row(cid, m["joined_at"], contacts[cid], r))
        rows.sort(key=lambda row: row.joined_at)
        sections.append({"id": lst["id"], "name": lst["name"], "rows": rows})

    shown: dict[str, list[str]] = {}
    for s in sections:
        for row in s["rows"]:
            shown.setdefault(row.contact_id, []).append(s["name"])
    for s in sections:
        for row in s["rows"]:
            row.also_in = [n for n in shown[row.contact_id] if n != s["name"]]
    return [s for s in sections if s["rows"]]


def unique_count(sections: list[dict]) -> int:
    return len({row.contact_id for s in sections for row in s["rows"]})


def render_message(sections: list[dict], today: datetime, mention: str = "") -> str:
    """Plain text for the Base chat (it linkifies URLs and renders @mentions)."""
    day = today.astimezone(ZONE).strftime("%d.%m.%Y")
    head = f"📞 Anrufliste {day} ({unique_count(sections)} Kontakte)"
    if mention:
        head = f"{mention} {head}"
    if not sections:
        return f"{head}\nHeute keine offenen Anrufe."
    out = [head]
    for s in sections:
        out += ["", f"— {s['name']} ({len(s['rows'])}) —"]
        for row in s["rows"]:
            c, r = row.contact, row.result
            name = " ".join(x for x in (c.get("firstname"), c.get("lastname")) if x) or "(ohne Namen)"
            phone = c.get("phone") or c.get("mobilephone") or "keine Nummer"
            badge = "🆕 Neu" if r.status == "neu" else "🔁 Nicht erreicht"
            details = [f"in Liste seit {row.joined_at.astimezone(ZONE).strftime('%d.%m.')}",
                       f"Versuche: {r.attempts}/{MAX_ATTEMPTS}"]
            if r.status == "wiedervorlage" and r.last_call:
                ts = r.last_call["_ts"].astimezone(ZONE).strftime("%d.%m. %H:%M")
                details.append(f"letzter Anruf: {ts} · {disposition_label(r.last_call.get('hs_call_disposition'))}")
            if c.get("hs_lead_status"):
                details.append(f"Lead-Status: {c['hs_lead_status']}")
            out.append(f"{badge} · {name} · {phone}")
            out.append("   " + " · ".join(details))
            if row.also_in:
                out.append(f"   ⚠️ auch in: {', '.join(row.also_in)}")
            out.append(f"   https://app.hubspot.com/contacts/{PORTAL_ID}/record/0-1/{row.contact_id}")
    return "\n".join(out)


# ---------------------------------------------------------------------------
# HubSpot (read-only)
# ---------------------------------------------------------------------------
async def _request(client: httpx.AsyncClient, method: str, path: str, body: dict | None = None) -> dict:
    for attempt in range(6):
        resp = await client.request(method, f"{HUBSPOT_BASE}{path}", headers=_headers(), json=body)
        if (resp.status_code == 429 or resp.status_code >= 500) and attempt < 5:
            await asyncio.sleep(float(resp.headers.get("retry-after") or 2 ** attempt))
            continue
        resp.raise_for_status()
        return resp.json()
    return {}


def _chunks(items: list, size: int):
    for i in range(0, len(items), size):
        yield items[i:i + size]


async def fetch_data(list_ids: list[str]) -> tuple[list[dict], dict[str, dict], dict[str, list[dict]]]:
    async with httpx.AsyncClient(timeout=30) as client:
        lists = []
        for lid in list_ids:
            meta = await _request(client, "GET", f"/crm/v3/lists/{lid}")
            members, after = [], None
            while True:
                q = f"?limit=250&after={after}" if after else "?limit=250"
                page = await _request(client, "GET", f"/crm/v3/lists/{lid}/memberships/join-order{q}")
                for r in page.get("results", []):
                    members.append({"contact_id": str(r["recordId"]), "joined_at": to_dt(r["membershipTimestamp"])})
                after = (page.get("paging") or {}).get("next", {}).get("after")
                if not after:
                    break
            lists.append({"id": lid, "name": (meta.get("list") or {}).get("name", f"Liste {lid}"), "members": members})

        ids = sorted({m["contact_id"] for l in lists for m in l["members"]})
        contacts: dict[str, dict] = {}
        for part in _chunks(ids, 100):
            data = await _request(client, "POST", "/crm/v3/objects/contacts/batch/read", {
                "properties": ["firstname", "lastname", "phone", "mobilephone", "hs_lead_status",
                               "engagements_last_meeting_booked", "lead_call_booked"],
                "inputs": [{"id": i} for i in part]})
            for r in data.get("results", []):
                contacts[str(r["id"])] = r.get("properties") or {}

        call_ids: dict[str, list[str]] = {}
        for part in _chunks(ids, 100):
            data = await _request(client, "POST", "/crm/v4/associations/contacts/calls/batch/read",
                                  {"inputs": [{"id": i} for i in part]})
            for r in data.get("results", []):
                cid = str(r["from"]["id"])
                found = [str(t["toObjectId"]) for t in r.get("to", [])]
                after = (r.get("paging") or {}).get("next", {}).get("after")
                while after:
                    more = await _request(client, "GET",
                                          f"/crm/v4/objects/contacts/{cid}/associations/calls?limit=500&after={after}")
                    found += [str(t["toObjectId"]) for t in more.get("results", [])]
                    after = (more.get("paging") or {}).get("next", {}).get("after")
                call_ids[cid] = found

        calls: dict[str, dict] = {}
        for part in _chunks(sorted({c for v in call_ids.values() for c in v}), 100):
            data = await _request(client, "POST", "/crm/v3/objects/calls/batch/read", {
                "properties": ["hs_timestamp", "hs_call_direction", "hs_call_disposition"],
                "inputs": [{"id": i} for i in part]})
            for r in data.get("results", []):
                calls[str(r["id"])] = r.get("properties") or {}

    calls_by_contact = {cid: [calls[c] for c in cs if c in calls] for cid, cs in call_ids.items()}
    return lists, contacts, calls_by_contact


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------
async def build_message(now: datetime | None = None) -> tuple[str, dict]:
    now = now or datetime.now(ZONE)
    lists, contacts, calls_by_contact = await fetch_data(LIST_IDS)

    # Gate only the candidates: a handful of HubSpot calls instead of one per member.
    candidates = {row.contact_id for s in build_sections(lists, contacts, calls_by_contact, now) for row in s["rows"]}
    suppressed: set[str] = set()
    reasons: dict[str, int] = {}
    for cid in sorted(candidates):
        is_suppressed, reason = await dialer_suppressed(contact_id=cid)
        if is_suppressed:
            suppressed.add(cid)
            reasons[reason] = reasons.get(reason, 0) + 1

    sections = build_sections(lists, contacts, calls_by_contact, now, suppressed)
    stats = {
        "lists": {l["id"]: {"name": l["name"], "members": len(l["members"])} for l in lists},
        "candidates": len(candidates),
        "suppressed": len(suppressed),
        "suppressed_reasons": reasons,
        "contacts": unique_count(sections),
    }
    return render_message(sections, now, os.environ.get("ANRUFLISTE_MENTION", "")), stats


async def post_to_base(message: str) -> dict:
    base_url = os.environ["BASE_URL"].rstrip("/")
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(f"{base_url}/api/webhooks/channel",
                                 headers={"X-API-Key": os.environ["BASE_API_KEY"]},
                                 json={"channel": os.environ["ANRUFLISTE_CHANNEL"], "message": message})
        resp.raise_for_status()
        return resp.json()


async def run_morgen_anrufliste(*, dry_run: bool = False) -> dict:
    """Scheduled entry point. Never raises: a failed run logs and returns the error."""
    try:
        message, stats = await build_message()
        if dry_run:
            return {"status": "dry_run", "stats": stats, "message": message}
        if stats["contacts"] == 0 and os.environ.get("ANRUFLISTE_SEND_EMPTY") != "1":
            logger.info("Anrufliste: keine offenen Anrufe, nichts gesendet (%s)", stats)
            return {"status": "empty", "stats": stats}
        posted = await post_to_base(message)
        logger.info("Anrufliste gesendet: %s Kontakte, Base-Nachricht %s", stats["contacts"], posted.get("message_id"))
        return {"status": "sent", "stats": stats, "base": posted}
    except Exception as exc:  # noqa: BLE001 — a scheduler job must not die silently
        logger.exception("Anrufliste fehlgeschlagen")
        return {"status": "error", "error": str(exc)}
