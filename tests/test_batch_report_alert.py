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


def test_normal_push_no_down_alert():
    stats = BatchRunStats(
        leads_fetched=100, aircall_queued=50, aircall_pushed=50, dialer_verified_count=50,
    )
    assert "AIRCALL DOWN" not in _body(stats)
    assert "✅" in _header(stats)


def test_empty_queue_no_false_alarm():
    # Legitimately nothing to push (e.g. outside call window) -> NO alarm.
    stats = BatchRunStats(leads_fetched=100, aircall_queued=0, aircall_pushed=0)
    assert "AIRCALL DOWN" not in _body(stats)
    assert "✅" in _header(stats)
