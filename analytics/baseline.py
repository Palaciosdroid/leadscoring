"""
Cohort-aware conversion baseline (re-runnable: `python -m analytics.baseline`).

Reports, against the CANONICAL conversion label (analytics.labels — HubSpot Deal
Won OR Whyros completed purchase), three things the old pooled "1.77%" number
hid:

  1. Conversion PER SIGNUP-COHORT (contacts.created_at month), maturity-aware.
     Young cohorts haven't had time to convert, so they are reported but flagged
     immature — never pooled into one misleading rate.

  2. Signal lift: conversion rate of contacts who fired a given behavioral signal
     (form_submit / video_complete / replay-watch / price-page / Eignungscheck)
     vs. the cohort base rate. Shows which signals actually predict conversion.

  3. Mapping coverage: % of page/click events that classify to a real funnel
     event_type (via the canonical Tracking-Crew taxonomy in main._classify_funnel_url)
     vs. landing in the generic `page_visited` bucket. Measures the Task-2 fix.

All fetches are READ-ONLY. The report is FAIL-SOFT: if HubSpot or Whyros returns
partial/no data, the affected section reports what it has (and says so) instead
of crashing.
"""

import asyncio
import logging
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone

from analytics.labels import (
    fetch_won_contacts,
    fetch_completed_purchase_emails,
    is_converted,
)
from integrations.supabase import get_supabase_client, _EVENT_FIELDS

logger = logging.getLogger(__name__)

# A cohort younger than this (months) hasn't had time to mature — flag it so its
# conversion rate is never read as final. Sales cycles here run months.
COHORT_MATURITY_MONTHS = 2

# Behavioral signals we measure lift for. Each maps to a predicate over a
# contact's raw Whyros events (event_type + page_url, lowercased).
#   form_submit    — submitted any form (Eignungscheck / application / optin form)
#   video_complete — watched a video to 100%
#   replay         — visited a replay/webinar page (canonical REPLAY paths)
#   price          — viewed the price page (kosten-termine)
#   eignungscheck  — hit an Eignungscheck quiz page
SIGNALS = ("form_submit", "video_complete", "replay", "price", "eignungscheck")


# ---------------------------------------------------------------------------
# Pure helpers (unit-testable against fixtures, no I/O)
# ---------------------------------------------------------------------------


def cohort_month(created_at: str | None) -> str | None:
    """Return the signup-cohort key 'YYYY-MM' from an ISO timestamp, or None."""
    if not created_at:
        return None
    raw = created_at.strip()
    if not raw:
        return None
    # PostgREST timestamps end in 'Z' or an offset; fromisoformat handles offsets,
    # not a bare 'Z', so normalize it.
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        # Fall back to the leading 'YYYY-MM' if the timestamp is non-standard.
        if len(raw) >= 7 and raw[4] == "-":
            return raw[:7]
        logger.debug("cohort_month: unparseable created_at %r", created_at)
        return None
    return f"{dt.year:04d}-{dt.month:02d}"


def _months_between(cohort: str, ref: datetime) -> int:
    """Whole months from a 'YYYY-MM' cohort to a reference datetime."""
    try:
        y, m = (int(x) for x in cohort.split("-"))
    except (ValueError, AttributeError):
        return 0
    return (ref.year - y) * 12 + (ref.month - m)


def is_mature(cohort: str, ref: datetime | None = None) -> bool:
    """True once a cohort is old enough that its conversion rate is meaningful."""
    ref = ref or datetime.now(timezone.utc)
    return _months_between(cohort, ref) >= COHORT_MATURITY_MONTHS


def classify_signals(events: list[dict]) -> set[str]:
    """
    Derive the set of fired behavioral signals from a contact's raw Whyros events.

    Uses event_type + page_url (lowercased). Mirrors the canonical funnel-URL
    taxonomy from main so price/replay/eignungscheck align with production mapping.
    """
    # Imported lazily so the pure helpers don't drag main.py's app at import time.
    from main import (
        PRICE_INFO_PATTERNS,
        EIGNUNGSCHECK_PATTERNS,
        REPLAY_URL_PATTERNS,
    )

    fired: set[str] = set()
    for ev in events:
        et = str(ev.get("event_type") or "").lower()
        url = str(ev.get("page_url") or "").lower()

        if et in ("form_submit", "form_submitted"):
            fired.add("form_submit")
        if et in ("video_complete", "video_completed"):
            fired.add("video_complete")
        if url:
            if any(p in url for p in PRICE_INFO_PATTERNS):
                fired.add("price")
            if any(p in url for p in EIGNUNGSCHECK_PATTERNS):
                fired.add("eignungscheck")
            if any(p in url for p in REPLAY_URL_PATTERNS):
                fired.add("replay")
    return fired


def coverage_for_events(events: list[dict]) -> tuple[int, int]:
    """
    (classified, total) for mapping coverage over page/click events.

    A page/click event is "classified" if its page_url resolves to a specific
    funnel event_type (checkout/price/eignungscheck/replay/sales) rather than the
    generic `page_visited` default. Non-page/click events (form_submit,
    video_*, scroll) are not URL-mapped and are excluded from the denominator.
    """
    from main import _classify_funnel_url

    classified = 0
    total = 0
    for ev in events:
        et = str(ev.get("event_type") or "").lower()
        if et not in ("pageview", "page", "page_visited", "click"):
            continue
        total += 1
        url = str(ev.get("page_url") or "").lower()
        mapped = _classify_funnel_url(url)
        # page_visited is what optin/thank-you and unknown URLs collapse to —
        # those count as "generic", everything else as classified.
        if mapped and mapped != "page_visited":
            classified += 1
    return classified, total


# ---------------------------------------------------------------------------
# Report assembly (pure — takes already-fetched data, returns a structured dict)
# ---------------------------------------------------------------------------


@dataclass
class CohortStat:
    cohort: str
    total: int = 0
    converted: int = 0
    mature: bool = True

    @property
    def rate(self) -> float:
        return self.converted / self.total if self.total else 0.0


@dataclass
class SignalStat:
    signal: str
    with_signal: int = 0
    with_signal_converted: int = 0
    without_signal: int = 0
    without_signal_converted: int = 0

    @property
    def rate_with(self) -> float:
        return self.with_signal_converted / self.with_signal if self.with_signal else 0.0

    @property
    def rate_without(self) -> float:
        return self.without_signal_converted / self.without_signal if self.without_signal else 0.0

    @property
    def lift(self) -> float | None:
        """Multiplicative lift of converting given the signal. None if no base."""
        base = self.rate_without
        if base <= 0:
            return None
        return self.rate_with / base


@dataclass
class BaselineReport:
    cohorts: list[CohortStat] = field(default_factory=list)
    signals: list[SignalStat] = field(default_factory=list)
    coverage_classified: int = 0
    coverage_total: int = 0
    contacts_total: int = 0
    contacts_converted: int = 0
    notes: list[str] = field(default_factory=list)

    @property
    def coverage_pct(self) -> float:
        return 100.0 * self.coverage_classified / self.coverage_total if self.coverage_total else 0.0

    @property
    def overall_rate(self) -> float:
        return self.contacts_converted / self.contacts_total if self.contacts_total else 0.0


def build_report(
    contacts: list[dict],
    events_by_visitor: dict[str, list[dict]],
    won_set: set[str],
    completed_set: set[str],
    *,
    ref: datetime | None = None,
) -> BaselineReport:
    """
    Assemble the cohort/signal/coverage baseline from already-fetched data.

    contacts          — Whyros contact rows (need id, email, visitor_id, created_at).
    events_by_visitor — {visitor_id: [event rows]} (event_type, page_url).
    won_set           — HubSpot Won contact ids (analytics.labels.fetch_won_contacts).
    completed_set     — completed-purchase emails (fetch_completed_purchase_emails).

    Pure: no network. Fail-soft — empty inputs yield an empty-but-valid report.
    """
    ref = ref or datetime.now(timezone.utc)
    report = BaselineReport()

    if not contacts:
        report.notes.append("No contacts available — cohort/signal sections empty.")
    if not won_set and not completed_set:
        report.notes.append(
            "No conversion label data (HubSpot + Whyros both empty) — "
            "rates will read 0%; treat as label-fetch failure, not zero conversion."
        )

    cohort_stats: dict[str, CohortStat] = {}
    # signal -> (with_n, with_conv, without_n, without_conv)
    sig_acc: dict[str, list[int]] = {s: [0, 0, 0, 0] for s in SIGNALS}
    cov_classified = 0
    cov_total = 0

    for c in contacts:
        cid = c.get("id")
        email = c.get("email")
        visitor_id = str(c.get("visitor_id") or "")
        cohort = cohort_month(c.get("created_at"))

        converted = is_converted(cid, email, won_set, completed_set)

        report.contacts_total += 1
        if converted:
            report.contacts_converted += 1

        # --- cohort ---
        if cohort:
            stat = cohort_stats.get(cohort)
            if stat is None:
                stat = CohortStat(cohort=cohort, mature=is_mature(cohort, ref))
                cohort_stats[cohort] = stat
            stat.total += 1
            if converted:
                stat.converted += 1

        # --- signals + coverage (need this contact's events) ---
        events = events_by_visitor.get(visitor_id, []) if visitor_id else []
        fired = classify_signals(events)
        for s in SIGNALS:
            acc = sig_acc[s]
            if s in fired:
                acc[0] += 1
                if converted:
                    acc[1] += 1
            else:
                acc[2] += 1
                if converted:
                    acc[3] += 1

        cls, tot = coverage_for_events(events)
        cov_classified += cls
        cov_total += tot

    report.cohorts = [cohort_stats[k] for k in sorted(cohort_stats)]
    report.signals = [
        SignalStat(
            signal=s,
            with_signal=sig_acc[s][0],
            with_signal_converted=sig_acc[s][1],
            without_signal=sig_acc[s][2],
            without_signal_converted=sig_acc[s][3],
        )
        for s in SIGNALS
    ]
    report.coverage_classified = cov_classified
    report.coverage_total = cov_total
    return report


def format_report(report: BaselineReport) -> str:
    """Render a BaselineReport as a plain-text block (CLI / Slack-friendly)."""
    lines: list[str] = []
    bar = "=" * 70
    lines.append(bar)
    lines.append("COHORT BASELINE — canonical label (HubSpot Deal Won OR completed purchase)")
    lines.append(bar)

    # --- overall ---
    lines.append(
        f"\nContacts: {report.contacts_total}  |  converted: {report.contacts_converted}  "
        f"|  overall: {report.overall_rate * 100:.2f}%  (pooled — see cohorts below)"
    )

    # --- conversion per cohort ---
    lines.append("\n" + "-" * 70)
    lines.append("CONVERSION PER SIGNUP-COHORT (created_at month, maturity-aware)")
    lines.append("-" * 70)
    if report.cohorts:
        lines.append(f"  {'cohort':9s} {'n':>6s} {'conv':>6s} {'rate':>8s}  maturity")
        for s in report.cohorts:
            flag = "" if s.mature else "  ⏳ immature (rate not final)"
            lines.append(
                f"  {s.cohort:9s} {s.total:6d} {s.converted:6d} {s.rate * 100:7.2f}%{flag}"
            )
    else:
        lines.append("  (no cohort data)")

    # --- signal lift ---
    lines.append("\n" + "-" * 70)
    lines.append("SIGNAL LIFT (conversion with vs. without each behavioral signal)")
    lines.append("-" * 70)
    lines.append(
        f"  {'signal':14s} {'n+':>6s} {'rate+':>8s} {'rate-':>8s} {'lift':>7s}"
    )
    for s in report.signals:
        lift = "n/a" if s.lift is None else f"{s.lift:.2f}x"
        lines.append(
            f"  {s.signal:14s} {s.with_signal:6d} "
            f"{s.rate_with * 100:7.2f}% {s.rate_without * 100:7.2f}% {lift:>7s}"
        )

    # --- mapping coverage ---
    lines.append("\n" + "-" * 70)
    lines.append("MAPPING COVERAGE (page/click events classified vs. generic page_visited)")
    lines.append("-" * 70)
    if report.coverage_total:
        lines.append(
            f"  {report.coverage_classified}/{report.coverage_total} events classified "
            f"= {report.coverage_pct:.1f}%  "
            f"({report.coverage_total - report.coverage_classified} generic page_visited)"
        )
    else:
        lines.append("  (no page/click events to classify)")

    # --- notes (fail-soft warnings) ---
    if report.notes:
        lines.append("\n" + "-" * 70)
        lines.append("NOTES")
        lines.append("-" * 70)
        for n in report.notes:
            lines.append(f"  ⚠️  {n}")

    lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# I/O layer (fail-soft fetch, then pure assembly)
# ---------------------------------------------------------------------------


async def _fetch_all_contacts() -> list[dict]:
    """Page all contacts (id, email, visitor_id, created_at). READ-ONLY."""
    client = get_supabase_client()
    rows: list[dict] = []
    page = 0
    page_size = 1000
    while True:
        chunk = await client._get("contacts", {
            "select": "id,email,visitor_id,created_at",
            "order": "created_at.asc",
            "offset": str(page * page_size),
            "limit": str(page_size),
        })
        if not chunk:
            break
        rows.extend(chunk)
        if len(chunk) < page_size:
            break
        page += 1
    return rows


async def _fetch_events_by_visitor(visitor_ids: list[str]) -> dict[str, list[dict]]:
    """Bulk-fetch events for the given visitor_ids, grouped by visitor_id."""
    client = get_supabase_client()
    out: dict[str, list[dict]] = defaultdict(list)
    chunk_size = 100
    for i in range(0, len(visitor_ids), chunk_size):
        chunk = visitor_ids[i:i + chunk_size]
        vids_csv = ",".join(chunk)
        evs = await client._get("events", {
            "select": _EVENT_FIELDS,
            "visitor_id": f"in.({vids_csv})",
        })
        for ev in evs:
            vid = str(ev.get("visitor_id") or "")
            if vid:
                out[vid].append(ev)
    return out


async def run() -> BaselineReport:
    """Fetch (fail-soft) + build the baseline report. Returns it for re-use."""
    notes: list[str] = []

    # Conversion label sets (independent fail-soft fetches).
    try:
        won_set = await fetch_won_contacts()
    except Exception as exc:  # noqa: BLE001 — baseline must never crash on partial data
        logger.error("baseline: fetch_won_contacts failed: %s", exc)
        won_set = set()
        notes.append(f"HubSpot Won fetch failed ({exc}) — primary label missing.")

    try:
        completed_set = await fetch_completed_purchase_emails()
    except Exception as exc:  # noqa: BLE001
        logger.error("baseline: fetch_completed_purchase_emails failed: %s", exc)
        completed_set = set()
        notes.append(f"Whyros completed-purchase fetch failed ({exc}) — secondary label missing.")

    # Contacts (cohort base).
    try:
        contacts = await _fetch_all_contacts()
    except Exception as exc:  # noqa: BLE001
        logger.error("baseline: contact fetch failed: %s", exc)
        contacts = []
        notes.append(f"Contact fetch failed ({exc}) — cohort/signal sections empty.")

    # Events (signals + coverage). Best-effort; missing events just reduce coverage.
    visitor_ids = list({str(c.get("visitor_id")) for c in contacts if c.get("visitor_id")})
    try:
        events_by_visitor = await _fetch_events_by_visitor(visitor_ids) if visitor_ids else {}
    except Exception as exc:  # noqa: BLE001
        logger.error("baseline: event fetch failed: %s", exc)
        events_by_visitor = {}
        notes.append(f"Event fetch failed ({exc}) — signal lift + coverage unavailable.")

    report = build_report(contacts, events_by_visitor, won_set, completed_set)
    report.notes = notes + report.notes
    return report


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    # Windows console defaults to cp1252 — force UTF-8 so ⏳/⚠️ render.
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except (AttributeError, ValueError):
        pass
    report = asyncio.run(run())
    print(format_report(report))


if __name__ == "__main__":
    main()
