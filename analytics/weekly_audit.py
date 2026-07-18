"""
Weekly health audit for the SBC lead-scoring system (READ-ONLY).

Closes two monitoring blindspots that the Slack batch-report alone does not:
  1. TIER-DISTRIBUTION BASELINE — snapshots hot/warm/cold counts every run and
     diffs vs the previous snapshot, so a scoring regression (e.g. a mode/threshold
     change silently gutting the warm band) is visible as a delta, not a guess.
  2. SILENT CRON FAILURE — checks how fresh the newest lead_score_updated_at is.
     The scheduled batches (08/12/16 CET) leave no persistent trace; if they stop,
     the only signal is stale scores. >8h stale = ALARM.

Also records launchcall-signal coverage and Kevin's live dialer-CSV health.

Each run APPENDS one JSON line to projekte/sbc-lead-scoring/audit_log.jsonl and
prints a human report with ✅ / ⚠️ / 🔴 flags. Schedule daily/weekly via Hetzner
cron or Windows Task Scheduler.

Creds: reads ~/.claude/secrets/SECRETS.json (same as the other ops scripts).
No timestamp is invented — uses the real wall clock at run time.
"""
import json, os, csv, io, time, datetime, collections, pathlib

import httpx

SECRETS = pathlib.Path.home() / ".claude" / "secrets" / "SECRETS.json"
AUDIT_LOG = pathlib.Path(r"C:/Users/sandr/Desktop/Claude/projekte/sbc-lead-scoring/audit_log.jsonl")

# Alarm thresholds
STALE_HOURS = 8            # newest score older than this -> batches likely stopped
CSV_MIN_ROWS = 200         # Kevin's callable list should never be near-empty
HOTWARM_DROP_PCT = 25      # hot+warm shrinking >this% vs last snapshot = regression

S = json.load(open(SECRETS, encoding="utf-8"))
HS = S["apiKeys"]["hubspot"]["access_token"]
hsH = {"Authorization": f"Bearer {HS}", "Content-Type": "application/json"}
HB = "https://api.hubapi.com"
cio = S["apiKeys"]["customerio"]
CIO_APP, CIO_KEY = cio["app_endpoint"].rstrip("/"), cio["app_api_key"]
R = S["railway_lead_scoring"]
DIALER_URL, DIALER_KEY = R["url"].rstrip("/"), R["debug_api_key"]
now = datetime.datetime.now(datetime.timezone.utc)


def req(m, u, **kw):
    for i in range(6):
        try:
            r = httpx.request(m, u, timeout=60, **kw)
            if r.status_code >= 500 or r.status_code == 429:
                time.sleep(2 * (i + 1)); continue
            return r
        except httpx.TransportError:
            time.sleep(2 * (i + 1))
    return r


def pdt(s):
    try:
        return datetime.datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except Exception:
        return None


def tier_distribution():
    """Total contacts per lead_tier via HubSpot search totals."""
    dist = {}
    for tier in ("1_hot", "2_warm", "3_cold", "4_disqualified"):
        body = {"filterGroups": [{"filters": [
            {"propertyName": "lead_tier", "operator": "EQ", "value": tier}]}], "limit": 1}
        r = req("POST", f"{HB}/crm/v3/objects/contacts/search", headers=hsH, json=body)
        dist[tier] = r.json().get("total", 0) if r.status_code == 200 else -1
    return dist


def newest_score_age_hours():
    """Hours since the most recently re-scored contact — batch liveness proxy."""
    body = {
        "filterGroups": [{"filters": [
            {"propertyName": "lead_score_updated_at", "operator": "HAS_PROPERTY"}]}],
        "sorts": [{"propertyName": "lead_score_updated_at", "direction": "DESCENDING"}],
        "properties": ["lead_score_updated_at"], "limit": 1,
    }
    r = req("POST", f"{HB}/crm/v3/objects/contacts/search", headers=hsH, json=body)
    if r.status_code != 200:
        return None
    results = r.json().get("results", [])
    if not results:
        return None
    ts = pdt(results[0]["properties"].get("lead_score_updated_at"))
    return round((now - ts).total_seconds() / 3600, 1) if ts else None


def launchcall_count():
    """Total launchcall-registered across the 5 funnel reminder segments (union)."""
    segs = {"AL": 395, "HC": 309, "GC": 362, "MC": 296, "BF": 324}
    ids = set()
    for sid in segs.values():
        start = None
        for _ in range(60):
            u = f"{CIO_APP}/segments/{sid}/membership?limit=1000" + (f"&start={start}" if start else "")
            r = req("GET", u, headers={"Authorization": f"Bearer {CIO_KEY}"})
            if r is None or r.status_code != 200:
                break
            j = r.json()
            for idf in j.get("identifiers", []):
                if idf.get("cio_id"):
                    ids.add(idf["cio_id"])
            start = j.get("next")
            if not start:
                break
    return len(ids)


def dialer_csv_health():
    r = req("GET", f"{DIALER_URL}/dialer/export.csv", params={"key": DIALER_KEY, "limit": 5000})
    if r.status_code != 200:
        return {"rows": -1, "status": r.status_code, "tiers": {}}
    rows = list(csv.DictReader(io.StringIO(r.text)))
    tk = next((c for c in (rows[0].keys() if rows else []) if "tier" in c.lower()), None)
    tiers = dict(collections.Counter(row.get(tk, "") for row in rows)) if tk else {}
    return {"rows": len(rows), "status": 200, "tiers": tiers}


def load_previous():
    if not AUDIT_LOG.exists():
        return None
    last = None
    for line in AUDIT_LOG.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            try:
                last = json.loads(line)
            except Exception:
                pass
    return last


def main():
    snap = {
        "ts": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "tier_distribution": tier_distribution(),
        "newest_score_age_h": newest_score_age_hours(),
        "launchcall_registered": launchcall_count(),
        "dialer_csv": dialer_csv_health(),
    }
    d = snap["tier_distribution"]
    hotwarm = (d.get("1_hot", 0) or 0) + (d.get("2_warm", 0) or 0)
    snap["hotwarm"] = hotwarm

    prev = load_previous()
    flags = []

    age = snap["newest_score_age_h"]
    if age is None:
        flags.append("🔴 Konnte Score-Frische nicht lesen (HubSpot-Fehler)")
    elif age > STALE_HOURS:
        flags.append(f"🔴 Batches gestoppt? Neuester Score ist {age}h alt (>{STALE_HOURS}h)")
    else:
        flags.append(f"✅ Batches laufen (neuester Score {age}h alt)")

    csv_rows = snap["dialer_csv"]["rows"]
    if csv_rows < 0:
        flags.append(f"🔴 Kevin-CSV nicht erreichbar (HTTP {snap['dialer_csv']['status']})")
    elif csv_rows < CSV_MIN_ROWS:
        flags.append(f"🔴 Kevin-CSV nur {csv_rows} Zeilen (<{CSV_MIN_ROWS}) — Liste fast leer!")
    else:
        flags.append(f"✅ Kevin-CSV: {csv_rows} anrufbare Leads")

    if prev:
        prev_hw = prev.get("hotwarm", 0) or 0
        if prev_hw and hotwarm < prev_hw * (1 - HOTWARM_DROP_PCT / 100):
            flags.append(
                f"🔴 Hot+Warm eingebrochen: {prev_hw} → {hotwarm} "
                f"(−{100*(prev_hw-hotwarm)/prev_hw:.0f}%) seit {prev.get('ts','?')[:10]}"
            )
        else:
            flags.append(f"✅ Hot+Warm stabil: {prev_hw} → {hotwarm}")
    else:
        flags.append(f"ℹ️ Erster Snapshot (Baseline gesetzt): Hot+Warm = {hotwarm}")

    AUDIT_LOG.parent.mkdir(parents=True, exist_ok=True)
    with AUDIT_LOG.open("a", encoding="utf-8") as f:
        f.write(json.dumps(snap, ensure_ascii=False) + "\n")

    print("=" * 60)
    print(f"SBC LEAD-SCORING — WEEKLY AUDIT  {snap['ts']}")
    print("=" * 60)
    for fl in flags:
        print(f"  {fl}")
    print(f"\n  Tier: {d}")
    print(f"  Launchcall registriert (CIO): {snap['launchcall_registered']}")
    print(f"  Kevin-CSV Tiers: {snap['dialer_csv']['tiers']}")
    print(f"\n  → geloggt nach {AUDIT_LOG}")


if __name__ == "__main__":
    main()
