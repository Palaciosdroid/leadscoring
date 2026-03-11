"""Tests for engagement score calculation."""

from datetime import datetime, timezone, timedelta

from scoring.engagement import (
    calculate_engagement_score,
    recency_multiplier,
    inactivity_malus,
    BASE_POINTS,
)


class TestRecencyMultiplier:
    def test_within_7_days(self):
        assert recency_multiplier(0) == 1.0
        assert recency_multiplier(3) == 1.0
        assert recency_multiplier(7) == 1.0

    def test_8_to_14_days(self):
        assert recency_multiplier(8) == 0.7
        assert recency_multiplier(14) == 0.7

    def test_15_to_30_days(self):
        assert recency_multiplier(15) == 0.4
        assert recency_multiplier(30) == 0.4

    def test_over_30_days(self):
        assert recency_multiplier(31) == 0.1
        assert recency_multiplier(365) == 0.1


class TestInactivityMalus:
    def test_unsubscribed(self):
        assert inactivity_malus(5, unsubscribed=True) == -50

    def test_inactive_over_30_days(self):
        assert inactivity_malus(31, unsubscribed=False) == -30

    def test_inactive_15_to_30_days(self):
        assert inactivity_malus(15, unsubscribed=False) == -15

    def test_active_recently(self):
        assert inactivity_malus(5, unsubscribed=False) == 0

    def test_unsubscribed_trumps_inactivity(self):
        # unsubscribed should give -50 regardless of days
        assert inactivity_malus(60, unsubscribed=True) == -50


class TestCalculateEngagementScore:
    def _make_event(self, event_type: str, days_ago: float = 1) -> dict:
        ts = datetime.now(timezone.utc) - timedelta(days=days_ago)
        return {"event_type": event_type, "timestamp": ts.isoformat()}

    def test_empty_events(self):
        result = calculate_engagement_score([])
        # No events → days_since_last = 999 → inactivity malus -30
        assert result["score"] == -30
        assert result["raw_score"] == -30
        assert result["event_breakdown"] == []
        assert result["days_since_last_activity"] == 999.0

    def test_single_recent_event(self):
        events = [self._make_event("checkout_visited", days_ago=1)]
        result = calculate_engagement_score(events)
        # 40 base * 1.0 recency = 40
        assert result["score"] == 40
        assert len(result["event_breakdown"]) == 1

    def test_high_score_capped_at_100(self):
        # Stack enough events to exceed 100
        events = [
            self._make_event("checkout_visited", days_ago=1),
            self._make_event("application_submitted", days_ago=1),
            self._make_event("video_watched_100", days_ago=2),
        ]
        result = calculate_engagement_score(events)
        assert result["score"] <= 100

    def test_unknown_event_ignored(self):
        events = [{"event_type": "unknown_event", "timestamp": datetime.now(timezone.utc).isoformat()}]
        result = calculate_engagement_score(events)
        # Unknown events skipped → no last_activity → 999 days → -30 malus
        assert result["score"] == -30
        assert result["event_breakdown"] == []

    def test_recency_decay(self):
        recent = self._make_event("email_opened", days_ago=1)
        old = self._make_event("email_opened", days_ago=20)

        recent_result = calculate_engagement_score([recent])
        old_result = calculate_engagement_score([old])

        # Recent should score higher (5 * 1.0 vs 5 * 0.4)
        assert recent_result["score"] > old_result["score"]

    def test_unsubscribed_malus(self):
        events = [
            self._make_event("email_opened", days_ago=1),
            {"event_type": "email_unsubscribed", "timestamp": datetime.now(timezone.utc).isoformat()},
        ]
        result = calculate_engagement_score(events)
        # email_opened = 5 points, but unsubscribed = -50 malus
        assert result["score"] < 0
        assert result["unsubscribed"] is True

    def test_invalid_timestamp_skipped(self):
        events = [{"event_type": "email_opened", "timestamp": "not-a-date"}]
        result = calculate_engagement_score(events)
        # Invalid timestamp skipped → no last_activity → -30 malus
        assert result["score"] == -30

    def test_multiple_events_accumulate(self):
        events = [
            self._make_event("email_opened", days_ago=1),      # 5
            self._make_event("email_link_clicked", days_ago=2), # 10
            self._make_event("sales_page_visited", days_ago=3), # 20
        ]
        result = calculate_engagement_score(events)
        assert result["score"] == 35  # all within 7 days → full points
