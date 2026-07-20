"""
PostHog-Intent-Signal calibration vs. the canonical Deal-Won label
(`python -m analytics.calibrate_posthog_signals`).

Measures, per signal bucket of the NEW PostHog-synced HubSpot properties
(INSTRUCTIONS-PostHog-Sync-2026-07-20.md — daily Railway cron, 14d window,
buyers excluded at source):

    offer_dwell_minutes   (number) — 2.0 warm / 5.0 hot
    payment_page_visited  (date)   — set = hot
    vsl_watched_percent   (number) — 50 warm / 90 hot

the n and the Deal-Won overlap (canonical label: HubSpot Deal Won OR Whyros
completed purchase — analytics.labels). `intent_funnel` is routing-only and is
reported as a split, never scored.

HONESTY GUARD: the properties were first populated 2026-07-20 with a 14-day
behavioral window AND the sync excludes recent buyers. A near-zero Deal-Won
overlap is therefore EXPECTED at first run and means "no calibration possible
yet", NOT "signals are worthless". The report says so explicitly instead of
extrapolating lifts from mini-samples. Buckets below MIN_BUCKET_CONVERTED
conversions are marked statistically empty.

All fetches are READ-ONLY. HubSpot pagination follows the repo standard
(fix 6f6922e): retry on 429/5xx with backoff, never break silently on a
non-200 — a failed page raises so partial data is never mistaken for truth.
"""

import asyncio
import logging
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone

import httpx

from analytics.labels import (
    HUBSPOT_BASE,
    _headers,
    fetch_won_contacts,
    fetch_completed_purchase_emails,
    is_converted,
)
from scoring.points import (
    PAYMENT_PAGE_POINTS,
    OFFER_DWELL_HOT_POINTS,
    OFFER_DWELL_WARM_POINTS,
    VSL_HOT_POINTS,
    VSL_WARM_POINTS,
    OFFER_DWELL_HOT_MIN,
    OFFER_DWELL_WARM_MIN,
    VSL_HOT_MIN,
    VSL_WARM_MIN,
)

logger = logging.getLogger(__name__)

SIGNAL_PROPERTIES = [
    "offer_dwell_minutes",
    "payment_page_visited",
    "vsl_watched_percent",
    "intent_funnel",
]

# Minimum conversions inside a bucket before its rate is worth printing as a
# lift. Below this the bucket is reported as "n too small — no lift claimed".
MIN_BUCKET_CONVERTED = 5

# Reference base close-rate from the 18.07 full-base calibration
# (55,301 contacts, canonical label). Context only — NOT recomputed here.
REFERENCE_BASE_RATE = 0.018


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


def parse_number(raw) -> float | None:
    """HubSpot returns number props as strings; tolerate junk."""
    if raw is None or raw == "":
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


@dataclass
class Bucket:
    label: str
    total: int = 0
    converted: int = 0

    @property
    def rate(self) -> float:
        return self.converted / self.total if self.total else 0.0


@dataclass
class SignalReport:
    contacts_total: int = 0
    contacts_converted: int = 0
    buckets: list[Bucket] = field(default_factory=list)
    funnel_split: dict[str, int] = field(default_factory=dict)
    spec_points: dict[int, Bucket] = field(default_factory=dict)
    notes: list[str] = field(default_factory=list)


def spec_points_for(dwell: float | None, payment: bool, vsl: float | None) -> int:
    """Would-be PostHog-signal points under the spec weights (highest step per signal)."""
    pts = 0
    if payment:
        pts += PAYMENT_PAGE_POINTS
    if dwell is not None:
        if dwell >= OFFER_DWELL_HOT_MIN:
            pts += OFFER_DWELL_HOT_POINTS
        elif dwell >= OFFER_DWELL_WARM_MIN:
            pts += OFFER_DWELL_WARM_POINTS
    if vsl is not None:
        if vsl >= VSL_HOT_MIN:
            pts += VSL_HOT_POINTS
        elif vsl >= VSL_WARM_MIN:
            pts += VSL_WARM_POINTS
    return pts


def build_report(
    contacts: list[dict],
    won_set: set[str],
    completed_set: set[str],
) -> SignalReport:
    """Pure assembly from already-fetched data (unit-testable, no I/O)."""
    report = SignalReport()

    buckets = {
        "payment_page_visited (gesetzt)": Bucket("payment_page_visited (gesetzt)"),
        f"offer_dwell >= {OFFER_DWELL_HOT_MIN:g} min": Bucket(f"offer_dwell >= {OFFER_DWELL_HOT_MIN:g} min"),
        f"offer_dwell {OFFER_DWELL_WARM_MIN:g}-<{OFFER_DWELL_HOT_MIN:g} min": Bucket(
            f"offer_dwell {OFFER_DWELL_WARM_MIN:g}-<{OFFER_DWELL_HOT_MIN:g} min"),
        f"vsl >= {VSL_HOT_MIN:g}%": Bucket(f"vsl >= {VSL_HOT_MIN:g}%"),
        f"vsl {VSL_WARM_MIN:g}-<{VSL_HOT_MIN:g}%": Bucket(f"vsl {VSL_WARM_MIN:g}-<{VSL_HOT_MIN:g}%"),
        f"vsl < {VSL_WARM_MIN:g}%": Bucket(f"vsl < {VSL_WARM_MIN:g}%"),
    }

    for c in contacts:
        props = c.get("properties", {})
        cid = c.get("id")
        email = props.get("email")

        dwell = parse_number(props.get("offer_dwell_minutes"))
        vsl = parse_number(props.get("vsl_watched_percent"))
        payment = bool(props.get("payment_page_visited"))
        funnel = (props.get("intent_funnel") or "(leer)").strip() or "(leer)"

        converted = is_converted(cid, email, won_set, completed_set)
        report.contacts_total += 1
        if converted:
            report.contacts_converted += 1
        report.funnel_split[funnel] = report.funnel_split.get(funnel, 0) + 1

        hit_labels: list[str] = []
        if payment:
            hit_labels.append("payment_page_visited (gesetzt)")
        if dwell is not None and dwell >= OFFER_DWELL_HOT_MIN:
            hit_labels.append(f"offer_dwell >= {OFFER_DWELL_HOT_MIN:g} min")
        elif dwell is not None and dwell >= OFFER_DWELL_WARM_MIN:
            hit_labels.append(f"offer_dwell {OFFER_DWELL_WARM_MIN:g}-<{OFFER_DWELL_HOT_MIN:g} min")
        if vsl is not None:
            if vsl >= VSL_HOT_MIN:
                hit_labels.append(f"vsl >= {VSL_HOT_MIN:g}%")
            elif vsl >= VSL_WARM_MIN:
                hit_labels.append(f"vsl {VSL_WARM_MIN:g}-<{VSL_HOT_MIN:g}%")
            else:
                hit_labels.append(f"vsl < {VSL_WARM_MIN:g}%")

        for label in hit_labels:
            b = buckets[label]
            b.total += 1
            if converted:
                b.converted += 1

        pts = spec_points_for(dwell, payment, vsl)
        pb = report.spec_points.setdefault(pts, Bucket(str(pts)))
        pb.total += 1
        if converted:
            pb.converted += 1

    report.buckets = list(buckets.values())

    if not won_set and not completed_set:
        report.notes.append(
            "Beide Label-Quellen leer — als Fetch-Fehler behandeln, nicht als 0-Conversion."
        )
    return report


def format_report(report: SignalReport) -> str:
    lines: list[str] = []
    bar = "=" * 72
    lines.append(bar)
    lines.append("POSTHOG-INTENT-SIGNAL KALIBRIERUNG — canonical label (Deal Won / completed)")
    lines.append(f"Stand: {datetime.now(timezone.utc).date().isoformat()} — Properties LIVE seit 2026-07-20 (!)")
    lines.append(bar)

    overall = report.contacts_converted / report.contacts_total if report.contacts_total else 0.0
    lines.append(
        f"\nKontakte mit PostHog-Signal: {report.contacts_total}  |  "
        f"konvertiert (canonical): {report.contacts_converted}  |  rate: {overall * 100:.2f}%"
    )
    lines.append(f"Referenz-Base-Rate (Vollbasis-Kalibrierung 18.07): {REFERENCE_BASE_RATE * 100:.1f}%")

    lines.append("\n" + "-" * 72)
    lines.append("SIGNAL-BUCKET → n / Deal-Won-Überlappung")
    lines.append("-" * 72)
    lines.append(f"  {'bucket':<34s} {'n':>6s} {'conv':>6s} {'rate':>8s}  {'belastbar?':s}")
    for b in report.buckets:
        ok = "ja" if b.converted >= MIN_BUCKET_CONVERTED else f"NEIN (<{MIN_BUCKET_CONVERTED} conv)"
        lines.append(
            f"  {b.label:<34s} {b.total:6d} {b.converted:6d} {b.rate * 100:7.2f}%  {ok}"
        )

    lines.append("\n" + "-" * 72)
    lines.append("SPEC-PUNKTE-SIMULATION (würde-Punkte nur aus den 3 neuen Signalen)")
    lines.append("-" * 72)
    for pts in sorted(report.spec_points):
        b = report.spec_points[pts]
        lines.append(f"  +{pts:>3d} Punkte: n={b.total:5d}  conv={b.converted:4d}  rate={b.rate * 100:6.2f}%")

    lines.append("\n" + "-" * 72)
    lines.append("INTENT_FUNNEL-SPLIT (nur Routing — wird NICHT gescort)")
    lines.append("-" * 72)
    for funnel, n in sorted(report.funnel_split.items(), key=lambda kv: -kv[1]):
        lines.append(f"  {funnel:<32s} {n:6d}")

    if report.notes:
        lines.append("\n" + "-" * 72)
        lines.append("NOTES")
        lines.append("-" * 72)
        for n in report.notes:
            lines.append(f"  !! {n}")
    lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# I/O — READ-ONLY HubSpot fetch (retry on 429/5xx, hard-fail on other non-200)
# ---------------------------------------------------------------------------


async def fetch_signal_contacts(*, timeout: float = 30.0) -> list[dict]:
    """
    All HubSpot contacts where ANY of the 3 score-relevant PostHog properties is
    set (3 OR'ed filterGroups). Paginated search, max 100/page.

    Repo standard (fix 6f6922e): 429/5xx → exponential backoff retry; any other
    non-200 raises (never silently truncate the base).
    """
    results: list[dict] = []
    after: str | None = None
    payload: dict = {
        "filterGroups": [
            {"filters": [{"propertyName": p, "operator": "HAS_PROPERTY"}]}
            for p in ("offer_dwell_minutes", "payment_page_visited", "vsl_watched_percent")
        ],
        "properties": ["email"] + SIGNAL_PROPERTIES,
        "limit": 100,
    }

    async with httpx.AsyncClient(timeout=timeout) as client:
        while True:
            if after:
                payload["after"] = after
            resp = None
            for attempt in range(6):
                resp = await client.post(
                    f"{HUBSPOT_BASE}/crm/v3/objects/contacts/search",
                    headers=_headers(),
                    json=payload,
                )
                if resp.status_code == 429 or resp.status_code >= 500:
                    wait = 2 ** attempt
                    logger.warning(
                        "signal search page %s: HTTP %s — retry %d/6 in %ds",
                        after or "first", resp.status_code, attempt + 1, wait,
                    )
                    await asyncio.sleep(wait)
                    continue
                break
            if resp.status_code != 200:
                raise RuntimeError(
                    f"HubSpot signal search failed: {resp.status_code} {resp.text[:300]}"
                )
            data = resp.json()
            results.extend(data.get("results", []))
            after = data.get("paging", {}).get("next", {}).get("after")
            if not after:
                break
            await asyncio.sleep(0.3)

    logger.info("fetch_signal_contacts: %d contacts with PostHog signals", len(results))
    return results


async def run() -> SignalReport:
    notes: list[str] = []

    try:
        won_set = await fetch_won_contacts()
    except Exception as exc:  # noqa: BLE001 — fail-soft on labels, loud in notes
        logger.error("posthog-calibrate: fetch_won_contacts failed: %s", exc)
        won_set = set()
        notes.append(f"HubSpot Won fetch failed ({exc}) — primary label missing.")

    try:
        completed_set = await fetch_completed_purchase_emails()
    except Exception as exc:  # noqa: BLE001
        logger.error("posthog-calibrate: completed-purchase fetch failed: %s", exc)
        completed_set = set()
        notes.append(f"Whyros completed-purchase fetch failed ({exc}) — secondary label missing.")

    contacts = await fetch_signal_contacts()

    report = build_report(contacts, won_set, completed_set)
    report.notes = notes + report.notes
    return report


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except (AttributeError, ValueError):
        pass
    report = asyncio.run(run())
    print(format_report(report))


if __name__ == "__main__":
    main()
