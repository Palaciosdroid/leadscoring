"""
Point-system calibration vs. the real Deal-Won label
(`python -m analytics.calibrate_points`).

Runs the transparent point-system (`scoring.points.compute_points`) over the
historical contact base, buckets every contact by its point-score, then reads
the CANONICAL conversion label (analytics.labels — HubSpot Deal Won OR Whyros
completed purchase) per bucket. Output:

  1. POINTS-BUCKET → CLOSE-RATE table — does a higher point-score actually mean
     a higher Deal-Won rate? (monotonicity check for the weights.)
  2. Hot / Warm / Cold THRESHOLD recommendation — the lowest bucket whose close-
     rate clears a target, so the live thresholds (scoring.points.TIERS) are set
     where the data justifies them rather than guessed.
  3. CLOSES-CONCENTRATION — what % of all real closes land in the recommended
     Hot+Warm band (vs. their share of the population). The whole point of the
     re-score: concentrate the closes Kevin should call.

Signals are assembled EXACTLY as the live scorer does — via the scorer's own
`_assemble_point_signals` (single source of truth; phone is never a signal) over
W1-mapped behavior (touchpoints + browser events), the Tally `lead_eig_*` props,
and the detected interest category. This guarantees the calibration scores match
production point-for-point.

All fetches are READ-ONLY. The report is FAIL-SOFT: if any source returns
partial/no data, the affected section reports what it has (and says so) instead
of crashing.
"""

import asyncio
import logging
import sys
from collections import defaultdict
from dataclasses import dataclass, field

from analytics.labels import (
    fetch_won_contacts,
    fetch_completed_purchase_emails,
    is_converted,
)
from batch.scorer import _assemble_point_signals
from scoring.interest import detect_interest_category
from scoring.points import compute_points, DISQUALIFIED_TIER
from scoring.touchpoint_mapper import (
    map_touchpoints_batch,
    map_browser_events_batch,
)
from integrations.supabase import get_supabase_client, _EVENT_FIELDS, _TOUCHPOINT_FIELDS

logger = logging.getLogger(__name__)

# Point buckets (lower bound, inclusive) used for the close-rate table. Wide
# enough to hold contacts per bucket, fine enough to locate a tier threshold.
# Disqualified contacts (interest=keines / unsubscribed) are reported in their
# own row — they are excluded from threshold/concentration math by definition.
BUCKET_EDGES: tuple[int, ...] = (0, 10, 25, 35, 50, 65, 80)

# A bucket needs at least this many contacts before its close-rate is trusted
# for a threshold recommendation (small buckets are noise).
MIN_BUCKET_N = 30

# Target close-rates the Hot / Warm thresholds must clear. Hot = the strong band
# (~budget-4000 measured 10.9%), Warm = clearly-above-base (~base 1.8%). The
# recommender returns the LOWEST bucket whose cumulative-from-top rate clears it.
HOT_TARGET_RATE = 0.08
WARM_TARGET_RATE = 0.04


# ---------------------------------------------------------------------------
# Pure helpers (unit-testable against fixtures, no I/O)
# ---------------------------------------------------------------------------


def bucket_for_points(points: int) -> int:
    """Return the lower edge of the BUCKET_EDGES bucket that `points` falls in."""
    chosen = BUCKET_EDGES[0]
    for edge in BUCKET_EDGES:
        if points >= edge:
            chosen = edge
        else:
            break
    return chosen


def assemble_signals(
    touchpoints: list[dict],
    browser_events: list[dict],
    props: dict,
    unsubscribed: bool,
) -> dict:
    """
    Build the `compute_points` signal dict for one contact, identically to the
    live scorer.

    Maps touchpoints + browser events to scored events (W1), detects the funnel
    (interest category), then delegates to the scorer's `_assemble_point_signals`
    so the calibration scores match production. Phone is never a signal.
    """
    scored_events = map_touchpoints_batch(touchpoints)
    scored_events.extend(map_browser_events_batch(browser_events))

    interest = detect_interest_category(scored_events)
    funnel = interest.get("category")

    return _assemble_point_signals(scored_events, props, funnel, unsubscribed)


# ---------------------------------------------------------------------------
# Report assembly (pure — takes already-fetched data, returns a structured dict)
# ---------------------------------------------------------------------------


@dataclass
class BucketStat:
    lower: int
    total: int = 0
    converted: int = 0

    @property
    def rate(self) -> float:
        return self.converted / self.total if self.total else 0.0


@dataclass
class ThresholdRec:
    hot: int | None = None
    warm: int | None = None
    note: str = ""


@dataclass
class CalibrationReport:
    buckets: list[BucketStat] = field(default_factory=list)
    disqualified_total: int = 0
    disqualified_converted: int = 0
    contacts_total: int = 0
    contacts_converted: int = 0
    threshold: ThresholdRec = field(default_factory=ThresholdRec)
    # Concentration of real closes in the recommended Hot+Warm band.
    hotwarm_closes: int = 0
    hotwarm_contacts: int = 0
    notes: list[str] = field(default_factory=list)

    @property
    def overall_rate(self) -> float:
        return self.contacts_converted / self.contacts_total if self.contacts_total else 0.0

    @property
    def closes_concentration(self) -> float:
        """% of ALL closes that land in the recommended Hot+Warm band."""
        return 100.0 * self.hotwarm_closes / self.contacts_converted if self.contacts_converted else 0.0

    @property
    def population_in_hotwarm(self) -> float:
        """% of the (qualified) population that lands in the Hot+Warm band."""
        return 100.0 * self.hotwarm_contacts / self.contacts_total if self.contacts_total else 0.0


def recommend_thresholds(buckets: list[BucketStat]) -> ThresholdRec:
    """
    Recommend Hot / Warm point thresholds from the bucketed close-rates.

    For each candidate threshold (a bucket lower-edge) compute the cumulative
    close-rate of everyone AT OR ABOVE it. Hot = the lowest threshold whose
    cumulative rate still clears HOT_TARGET_RATE; Warm = lowest clearing
    WARM_TARGET_RATE. Buckets below MIN_BUCKET_N are skipped as noise.

    Returns ThresholdRec(hot, warm, note). Either may be None if no bucket
    qualifies (e.g. no label data) — the note explains why.
    """
    rec = ThresholdRec()
    if not buckets:
        rec.note = "no buckets — cannot recommend thresholds"
        return rec

    # Walk edges from the TOP down. For each edge, the cumulative-from-top close-
    # rate is everyone at-or-above it. We extend the band downward as long as the
    # cumulative rate keeps clearing the target; the threshold is the lowest edge
    # in that contiguous clearing run (so the band above it genuinely clears —
    # robust to a non-monotone low bucket that would otherwise sneak in).
    ordered = sorted(buckets, key=lambda b: b.lower, reverse=True)
    hot_candidate: int | None = None
    warm_candidate: int | None = None
    hot_open = True
    warm_open = True
    for b in ordered:
        cum_total = sum(x.total for x in ordered if x.lower >= b.lower)
        cum_conv = sum(x.converted for x in ordered if x.lower >= b.lower)
        if cum_total < MIN_BUCKET_N:
            # Too thin to trust at this depth — stop extending either band lower.
            hot_open = warm_open = False
            continue
        rate = cum_conv / cum_total if cum_total else 0.0
        if hot_open and rate >= HOT_TARGET_RATE:
            hot_candidate = b.lower
        else:
            hot_open = False
        if warm_open and rate >= WARM_TARGET_RATE:
            warm_candidate = b.lower
        else:
            warm_open = False

    rec.hot = hot_candidate
    rec.warm = warm_candidate
    if hot_candidate is None and warm_candidate is None:
        rec.note = (
            "no bucket cleared the target close-rates "
            "(thin data or missing label) — keep current TIERS"
        )
    elif hot_candidate is not None and warm_candidate is not None and warm_candidate > hot_candidate:
        # Degenerate (warm above hot) — collapse to a single threshold.
        rec.warm = hot_candidate
        rec.note = "warm target only met inside the hot band — bands collapsed"
    return rec


def build_report(
    contacts: list[dict],
    touchpoints_by_contact: dict[str, list[dict]],
    events_by_visitor: dict[str, list[dict]],
    won_set: set[str],
    completed_set: set[str],
) -> CalibrationReport:
    """
    Assemble the calibration report from already-fetched data.

    contacts                — Whyros contact rows (id, email, visitor_id) merged
                              with HubSpot Tally props (lead_eig_*) under the same
                              dict (props live at top level, like the scorer).
    touchpoints_by_contact  — {contact_id: [touchpoint rows]}.
    events_by_visitor       — {visitor_id: [browser event rows]}.
    won_set / completed_set  — canonical label sets (analytics.labels).

    Pure: no network. Fail-soft — empty inputs yield an empty-but-valid report.
    """
    report = CalibrationReport()

    if not contacts:
        report.notes.append("No contacts available — bucket table empty.")
    if not won_set and not completed_set:
        report.notes.append(
            "No conversion label data (HubSpot + Whyros both empty) — close-rates "
            "will read 0%; treat as label-fetch failure, not zero conversion."
        )

    bucket_stats: dict[int, BucketStat] = {edge: BucketStat(lower=edge) for edge in BUCKET_EDGES}

    for c in contacts:
        cid = c.get("id")
        email = c.get("email")
        visitor_id = str(c.get("visitor_id") or "")

        touchpoints = touchpoints_by_contact.get(str(cid), []) if cid is not None else []
        browser_events = events_by_visitor.get(visitor_id, []) if visitor_id else []

        unsubscribed = any(
            (e.get("event_type") or "") == "email_unsubscribed" for e in browser_events
        )

        signals = assemble_signals(touchpoints, browser_events, c, unsubscribed)
        result = compute_points(signals)

        converted = is_converted(cid, email, won_set, completed_set)
        report.contacts_total += 1
        if converted:
            report.contacts_converted += 1

        # Disqualified leads (interest=keines / unsubscribed) are not part of the
        # tierable population — bucket them separately so they don't skew the table.
        if result.tier == DISQUALIFIED_TIER:
            report.disqualified_total += 1
            if converted:
                report.disqualified_converted += 1
            continue

        b = bucket_stats[bucket_for_points(result.points)]
        b.total += 1
        if converted:
            b.converted += 1

    report.buckets = [bucket_stats[edge] for edge in BUCKET_EDGES]

    # Threshold recommendation from the (non-disqualified) buckets.
    report.threshold = recommend_thresholds(report.buckets)

    # Closes-concentration: share of ALL real closes (incl. disqualified) that
    # land in the recommended Hot+Warm band. Band floor = warm threshold (or hot
    # if no warm). If neither is set, the band is empty.
    band_floor = report.threshold.warm
    if band_floor is None:
        band_floor = report.threshold.hot
    if band_floor is not None:
        for b in report.buckets:
            if b.lower >= band_floor:
                report.hotwarm_closes += b.converted
                report.hotwarm_contacts += b.total

    return report


def format_report(report: CalibrationReport) -> str:
    """Render a CalibrationReport as a plain-text block (CLI / Slack-friendly)."""
    lines: list[str] = []
    bar = "=" * 70
    lines.append(bar)
    lines.append("POINT-SYSTEM CALIBRATION — canonical label (Deal Won OR completed purchase)")
    lines.append(bar)

    tierable = report.contacts_total - report.disqualified_total
    lines.append(
        f"\nContacts: {report.contacts_total}  |  converted: {report.contacts_converted}  "
        f"|  overall: {report.overall_rate * 100:.2f}%"
    )
    lines.append(
        f"Disqualified (interest=keines / unsubscribed): {report.disqualified_total} "
        f"(converted {report.disqualified_converted})  |  tierable: {tierable}"
    )

    # --- bucket close-rate table ---
    lines.append("\n" + "-" * 70)
    lines.append("POINTS-BUCKET → DEAL-WON CLOSE-RATE")
    lines.append("-" * 70)
    lines.append(f"  {'bucket':>10s} {'n':>7s} {'conv':>6s} {'close-rate':>11s}")
    edges = list(BUCKET_EDGES)
    for i, b in enumerate(report.buckets):
        upper = edges[i + 1] if i + 1 < len(edges) else None
        label = f"{b.lower}+" if upper is None else f"{b.lower}-{upper - 1}"
        lines.append(
            f"  {label:>10s} {b.total:7d} {b.converted:6d} {b.rate * 100:10.2f}%"
        )

    # --- threshold recommendation ---
    lines.append("\n" + "-" * 70)
    lines.append("THRESHOLD RECOMMENDATION (cumulative close-rate from the top)")
    lines.append("-" * 70)
    t = report.threshold
    hot = "n/a" if t.hot is None else f"≥ {t.hot}"
    warm = "n/a" if t.warm is None else f"≥ {t.warm}"
    lines.append(f"  Hot  (target ≥ {HOT_TARGET_RATE * 100:.0f}% close-rate):  {hot}")
    lines.append(f"  Warm (target ≥ {WARM_TARGET_RATE * 100:.0f}% close-rate):  {warm}")
    lines.append("  Cold: below Warm")
    if t.note:
        lines.append(f"  note: {t.note}")

    # --- closes-concentration ---
    lines.append("\n" + "-" * 70)
    lines.append("CLOSES-CONCENTRATION (recommended Hot+Warm band)")
    lines.append("-" * 70)
    if report.hotwarm_contacts:
        lines.append(
            f"  {report.closes_concentration:.1f}% of all closes "
            f"({report.hotwarm_closes}/{report.contacts_converted}) "
            f"land in Hot+Warm, which is {report.population_in_hotwarm:.1f}% of the population "
            f"({report.hotwarm_contacts}/{report.contacts_total})."
        )
    else:
        lines.append("  (no Hot+Warm band — threshold not set / no label data)")

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
    """Page all contacts (id, email, visitor_id). READ-ONLY."""
    client = get_supabase_client()
    rows: list[dict] = []
    page = 0
    page_size = 1000
    while True:
        chunk = await client._get("contacts", {
            "select": "id,email,visitor_id",
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


async def _fetch_touchpoints_by_contact(contact_ids: list[str]) -> dict[str, list[dict]]:
    """Bulk-fetch touchpoints for the given contact_ids, grouped by contact_id."""
    client = get_supabase_client()
    out: dict[str, list[dict]] = defaultdict(list)
    chunk_size = 50  # UUID IN-clauses stay small (Supabase stmt-timeout guard)
    for i in range(0, len(contact_ids), chunk_size):
        chunk = contact_ids[i:i + chunk_size]
        ids_csv = ",".join(chunk)
        tps = await client._get("touchpoints", {
            "select": _TOUCHPOINT_FIELDS,
            "contact_id": f"in.({ids_csv})",
        })
        for tp in tps:
            cid = str(tp.get("contact_id") or "")
            if cid:
                out[cid].append(tp)
    return out


async def _fetch_events_by_visitor(visitor_ids: list[str]) -> dict[str, list[dict]]:
    """Bulk-fetch browser events for the given visitor_ids, grouped by visitor_id."""
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


def _merge_tally_props(
    contacts: list[dict], tally_by_email: dict[str, dict],
) -> None:
    """
    Merge mapped Tally Eignungscheck answers into each contact dict as the
    `lead_eig_*` props the scorer reads (by lowercased email). In-place.

    The live scorer reads these from HubSpot; for calibration we fold the Tally
    source straight in so the signal dict is identical without a HubSpot round-trip.
    """
    for c in contacts:
        email = (c.get("email") or "").strip().lower()
        mapped = tally_by_email.get(email)
        if not mapped:
            continue
        c["lead_eig_budget"] = mapped.get("budget")
        c["lead_eig_interest"] = mapped.get("interest")
        c["lead_eig_consult"] = mapped.get("consult")
        c["lead_eig_goal"] = mapped.get("goal")
        c["lead_eig_score"] = mapped.get("eig_score")


async def run() -> CalibrationReport:
    """Fetch (fail-soft) + build the calibration report. Returns it for re-use."""
    notes: list[str] = []

    # Conversion label sets (independent fail-soft fetches).
    try:
        won_set = await fetch_won_contacts()
    except Exception as exc:  # noqa: BLE001 — calibration must never crash on partial data
        logger.error("calibrate: fetch_won_contacts failed: %s", exc)
        won_set = set()
        notes.append(f"HubSpot Won fetch failed ({exc}) — primary label missing.")

    try:
        completed_set = await fetch_completed_purchase_emails()
    except Exception as exc:  # noqa: BLE001
        logger.error("calibrate: fetch_completed_purchase_emails failed: %s", exc)
        completed_set = set()
        notes.append(f"Whyros completed-purchase fetch failed ({exc}) — secondary label missing.")

    # Contacts (the scoring base).
    try:
        contacts = await _fetch_all_contacts()
    except Exception as exc:  # noqa: BLE001
        logger.error("calibrate: contact fetch failed: %s", exc)
        contacts = []
        notes.append(f"Contact fetch failed ({exc}) — bucket table empty.")

    # Tally Eignungscheck answers (strongest signal). Best-effort — missing Tally
    # just drops budget/interest/consult to None (those contacts score on behavior).
    tally_by_email: dict[str, dict] = {}
    try:
        from integrations.tally import fetch_submissions

        submissions = await fetch_submissions()
        for s in submissions:
            sub = s.get("submission", {})
            email = ""
            # Tally exposes the respondent email on the submission; fall back to
            # scanning answers for an email-typed response if absent.
            for key in ("email", "respondentEmail", "respondent_email"):
                if sub.get(key):
                    email = str(sub[key]).strip().lower()
                    break
            if email:
                tally_by_email[email] = s.get("mapped", {})
    except Exception as exc:  # noqa: BLE001
        logger.warning("calibrate: Tally fetch failed: %s", exc)
        notes.append(f"Tally fetch failed ({exc}) — Eignungscheck signals unavailable.")

    if tally_by_email:
        _merge_tally_props(contacts, tally_by_email)
    elif contacts:
        notes.append("No Tally answers merged — calibration runs on behavior signals only.")

    # Touchpoints + browser events (W1 behavior signals). Best-effort.
    contact_ids = list({str(c.get("id")) for c in contacts if c.get("id")})
    try:
        touchpoints_by_contact = (
            await _fetch_touchpoints_by_contact(contact_ids) if contact_ids else {}
        )
    except Exception as exc:  # noqa: BLE001
        logger.error("calibrate: touchpoint fetch failed: %s", exc)
        touchpoints_by_contact = {}
        notes.append(f"Touchpoint fetch failed ({exc}) — behavior signals reduced.")

    visitor_ids = list({str(c.get("visitor_id")) for c in contacts if c.get("visitor_id")})
    try:
        events_by_visitor = (
            await _fetch_events_by_visitor(visitor_ids) if visitor_ids else {}
        )
    except Exception as exc:  # noqa: BLE001
        logger.error("calibrate: event fetch failed: %s", exc)
        events_by_visitor = {}
        notes.append(f"Event fetch failed ({exc}) — behavior signals reduced.")

    report = build_report(
        contacts, touchpoints_by_contact, events_by_visitor, won_set, completed_set,
    )
    report.notes = notes + report.notes
    return report


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    # Windows console defaults to cp1252 — force UTF-8 so ≥/⚠️ render.
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except (AttributeError, ValueError):
        pass
    report = asyncio.run(run())
    print(format_report(report))


if __name__ == "__main__":
    main()
