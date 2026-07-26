"""Tests for the batch-report Slack alert on SILENT Aircall failure.

Regression guard for the 2026-06 incident: Kevin's dialer campaign 404'd, every
push failed, pushed=0 — and because the existing gap alert was gated on
`pushed > 0`, NOTHING alerted. It failed silently for days. These tests assert a
loud alert fires when the queue had leads but none were pushed.
"""
from integrations.slack import BatchRunStats, _build_batch_report_message


def _body(stats: BatchRunStats) -> str:
    return _build_batch_report_message(stats)["blocks"][1]["text"]["text"]


def _header(stats: BatchRunStats) -> str:
    return _build_batch_report_message(stats)["blocks"][0]["text"]["text"]


def test_silent_aircall_failure_alerts():
    # Queue had leads but nothing pushed -> catastrophic, must alert loudly.
    stats = BatchRunStats(leads_fetched=100, aircall_queued=50, aircall_pushed=0)
    assert "AIRCALL DOWN" in _body(stats)
    # And the run must NOT be reported as OK (no green check in header).
    assert "✅" not in _header(stats)


def test_down_alert_includes_error_sample():
    stats = BatchRunStats(
        aircall_queued=10, aircall_pushed=0,
        aircall_push_error_sample="404 dialer_campaign NOT_FOUND",
    )
    assert "404" in _body(stats)


# These fixtures carry leads_processed/hs_updates_ok on purpose: a run that
# fetches 100 leads but processes none is NOT a healthy run (see the
# scoring-dead tests below), so a green-header assertion needs a run that
# actually scored something.

def test_normal_push_no_down_alert():
    stats = BatchRunStats(
        leads_fetched=100, leads_processed=100, hs_updates_ok=100,
        aircall_queued=50, aircall_pushed=50, dialer_verified_count=50,
    )
    assert "AIRCALL DOWN" not in _body(stats)
    assert "✅" in _header(stats)


def test_empty_queue_no_false_alarm():
    # Legitimately nothing to push (e.g. outside call window) -> NO alarm.
    stats = BatchRunStats(
        leads_fetched=100, leads_processed=100, hs_updates_ok=100,
        aircall_queued=0, aircall_pushed=0,
    )
    assert "AIRCALL DOWN" not in _body(stats)
    assert "✅" in _header(stats)


def test_window_skipped_no_false_alarm():
    # 08:00 batch: all queued leads skipped by the 9-20 call window -> NO alarm.
    # Regression guard for the 08:00 false "AIRCALL DOWN" report (2026-06-30).
    stats = BatchRunStats(
        leads_fetched=100, leads_processed=100, hs_updates_ok=100,
        aircall_queued=50, aircall_pushed=0, aircall_window_skipped=50,
    )
    assert "AIRCALL DOWN" not in _body(stats)
    assert "✅" in _header(stats)
    assert "außerhalb Call-Window" in _body(stats)


def test_partial_window_skip_still_alarms():
    # Some leads WERE eligible (inside window) and none got pushed -> real failure.
    stats = BatchRunStats(
        leads_fetched=100, aircall_queued=50, aircall_pushed=0,
        aircall_window_skipped=30,
    )
    assert "AIRCALL DOWN" in _body(stats)
    assert "✅" not in _header(stats)


# ---------------------------------------------------------------------------
# Scoring-dead alert (outage 18.-26.07: 8 days, ~24 reports, nobody acted)
# ---------------------------------------------------------------------------

def test_zero_processed_is_fatal_not_quiet():
    # The exact shape of the outage: Step 1 died, so nothing was fetched or
    # processed. The old report read "0 leads, 0 hs_ok, 0 chunk_errors" and
    # looked like a calm run. Zero processed is NEVER legitimate (pool >10k).
    stats = BatchRunStats(leads_fetched=0, leads_processed=0, hs_updates_ok=0)
    assert "SCORING TOT" in _body(stats)
    assert "✅" not in _header(stats)
    assert "FATAL" in _header(stats)


def test_scoring_dead_pings_channel():
    # A silent card in a busy channel is not an alarm — force a push notification.
    stats = BatchRunStats(leads_processed=0, hs_updates_ok=0)
    assert "<!channel>" in _body(stats)


def test_healthy_run_does_not_ping_channel():
    stats = BatchRunStats(
        leads_fetched=10_678, leads_processed=10_678, hs_updates_ok=10_678,
        aircall_queued=5, aircall_pushed=5, dialer_verified_count=5,
    )
    assert "<!channel>" not in _body(stats)
    assert "SCORING TOT" not in _body(stats)
    assert "✅" in _header(stats)


def test_message_carries_top_level_text_for_push_notifications():
    # Block-Kit-only messages preview as blank in Slack push notifications —
    # part of why the outage stayed invisible on mobile.
    msg = _build_batch_report_message(BatchRunStats(leads_processed=0))
    assert msg.get("text"), "no top-level text -> blank push notification"
    assert "FATAL" in msg["text"]
