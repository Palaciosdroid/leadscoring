"""
Tests for S1 Self-Learning: _analyze_call_calibration + _build_calibration_slack_message
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from analytics.buyer_journey import _analyze_call_calibration, _build_calibration_slack_message


def _make_contact(tier: str, outcome: str, score: float = 50.0, booked: bool = False) -> dict:
    return {"tier": tier, "outcome": outcome, "score": score, "booked": booked}


POSITIVE_OUTCOME = "Kontakt aufgenommen"
NEGATIVE_OUTCOME = "Nicht erreicht"


class TestAnalyzeCallCalibration:

    def test_empty_contacts_returns_empty_stats(self):
        result = _analyze_call_calibration([])
        assert result["total_contacts"] == 0
        assert result["tier_stats"] == []
        assert len(result["recommendations"]) == 1
        assert "grünen Bereich" in result["recommendations"][0]

    def test_single_tier_connection_rate(self):
        contacts = [
            _make_contact("1_hot", POSITIVE_OUTCOME),
            _make_contact("1_hot", NEGATIVE_OUTCOME),
        ]
        result = _analyze_call_calibration(contacts)
        hot = next(s for s in result["tier_stats"] if s["tier"] == "1_hot")
        assert hot["connection_rate"] == 50.0
        assert hot["total_calls"] == 2

    def test_booking_rate_calculated_correctly(self):
        contacts = [
            _make_contact("1_hot", POSITIVE_OUTCOME, booked=True),
            _make_contact("1_hot", POSITIVE_OUTCOME, booked=True),
            _make_contact("1_hot", NEGATIVE_OUTCOME, booked=False),
            _make_contact("1_hot", NEGATIVE_OUTCOME, booked=False),
        ]
        result = _analyze_call_calibration(contacts)
        hot = next(s for s in result["tier_stats"] if s["tier"] == "1_hot")
        assert hot["booking_rate"] == 50.0

    def test_avg_score_calculated_correctly(self):
        contacts = [
            _make_contact("2_warm", NEGATIVE_OUTCOME, score=40.0),
            _make_contact("2_warm", NEGATIVE_OUTCOME, score=60.0),
        ]
        result = _analyze_call_calibration(contacts)
        warm = next(s for s in result["tier_stats"] if s["tier"] == "2_warm")
        assert warm["avg_score"] == 50.0

    def test_multiple_tiers_all_present(self):
        contacts = [
            _make_contact("1_hot", POSITIVE_OUTCOME),
            _make_contact("2_warm", NEGATIVE_OUTCOME),
            _make_contact("3_cold", NEGATIVE_OUTCOME),
        ]
        result = _analyze_call_calibration(contacts)
        tiers = [s["tier"] for s in result["tier_stats"]]
        assert "1_hot" in tiers
        assert "2_warm" in tiers
        assert "3_cold" in tiers

    def test_tier_labels_mapped_correctly(self):
        contacts = [_make_contact("1_hot", POSITIVE_OUTCOME)]
        result = _analyze_call_calibration(contacts)
        hot = next(s for s in result["tier_stats"] if s["tier"] == "1_hot")
        assert hot["label"] == "Hot"

    def test_unknown_tier_falls_back_to_raw_key(self):
        contacts = [_make_contact("99_unknown", NEGATIVE_OUTCOME)]
        result = _analyze_call_calibration(contacts)
        unk = next(s for s in result["tier_stats"] if s["tier"] == "99_unknown")
        assert unk["label"] == "99_unknown"

    def test_recommendation_hot_threshold_too_low(self):
        """Hot with <40% connection rate → threshold too low warning"""
        contacts = [
            _make_contact("1_hot", POSITIVE_OUTCOME, score=30.0),  # 1 positive
            _make_contact("1_hot", NEGATIVE_OUTCOME, score=30.0),
            _make_contact("1_hot", NEGATIVE_OUTCOME, score=30.0),  # 33% = <40%
        ]
        result = _analyze_call_calibration(contacts)
        assert any("Hot-Schwelle zu niedrig" in r for r in result["recommendations"])

    def test_recommendation_warm_performs_like_hot(self):
        """Warm with >70% connection rate → warm threshold too high warning"""
        contacts = [_make_contact("2_warm", POSITIVE_OUTCOME, score=80.0) for _ in range(8)]
        contacts += [_make_contact("2_warm", NEGATIVE_OUTCOME, score=80.0) for _ in range(2)]
        result = _analyze_call_calibration(contacts)
        assert any("Warm-Leads performen wie Hot" in r for r in result["recommendations"])

    def test_recommendation_hot_strong_booking_rate(self):
        """Hot with >30% booking rate → positive feedback"""
        contacts = [_make_contact("1_hot", POSITIVE_OUTCOME, booked=True) for _ in range(4)]
        contacts += [_make_contact("1_hot", POSITIVE_OUTCOME, booked=False) for _ in range(6)]
        result = _analyze_call_calibration(contacts)
        assert any("konvertieren stark" in r for r in result["recommendations"])

    def test_recommendation_cold_high_connection_rate(self):
        """Cold with >30% connection rate → cold threshold too strict"""
        contacts = [_make_contact("3_cold", POSITIVE_OUTCOME, score=25.0) for _ in range(4)]
        contacts += [_make_contact("3_cold", NEGATIVE_OUTCOME, score=25.0) for _ in range(6)]
        result = _analyze_call_calibration(contacts)
        assert any("Cold-Leads mit" in r for r in result["recommendations"])

    def test_no_recommendations_when_all_normal(self):
        """Normal ranges → green zone message"""
        contacts = [
            _make_contact("1_hot", POSITIVE_OUTCOME),  # 50% connection, normal
        ]
        result = _analyze_call_calibration(contacts)
        # 50% connection on hot = no specific warning
        # booking = 0% = no positive feedback (not >30%)
        # just: "grünen Bereich" fallback
        assert any("grünen Bereich" in r or "keine Anpassungen" in r for r in result["recommendations"])

    def test_total_contacts_correct(self):
        contacts = [_make_contact("1_hot", POSITIVE_OUTCOME) for _ in range(7)]
        contacts += [_make_contact("2_warm", NEGATIVE_OUTCOME) for _ in range(3)]
        result = _analyze_call_calibration(contacts)
        assert result["total_contacts"] == 10


class TestBuildCalibrationSlackMessage:

    def _sample_calibration(self, connection_rate=50.0, booking_rate=20.0, total=100):
        return {
            "total_contacts": total,
            "tier_stats": [
                {
                    "tier": "1_hot",
                    "label": "Hot",
                    "total_calls": total,
                    "connection_rate": connection_rate,
                    "booking_rate": booking_rate,
                    "avg_score": 72.5,
                }
            ],
            "recommendations": ["Score-Kalibrierung im grünen Bereich — keine Anpassungen empfohlen."],
        }

    def test_returns_blocks_key(self):
        msg = _build_calibration_slack_message(self._sample_calibration())
        assert "blocks" in msg
        assert isinstance(msg["blocks"], list)

    def test_has_header_block(self):
        msg = _build_calibration_slack_message(self._sample_calibration())
        headers = [b for b in msg["blocks"] if b["type"] == "header"]
        assert len(headers) == 1
        assert "Kalibrierung" in headers[0]["text"]["text"]

    def test_has_context_with_total(self):
        msg = _build_calibration_slack_message(self._sample_calibration(total=42))
        context_blocks = [b for b in msg["blocks"] if b["type"] == "context"]
        assert any("42" in str(b) for b in context_blocks)

    def test_green_emoji_high_connection_rate(self):
        msg = _build_calibration_slack_message(self._sample_calibration(connection_rate=60.0))
        text = str(msg)
        assert "🟢" in text

    def test_yellow_emoji_medium_connection_rate(self):
        msg = _build_calibration_slack_message(self._sample_calibration(connection_rate=40.0))
        text = str(msg)
        assert "🟡" in text

    def test_red_emoji_low_connection_rate(self):
        msg = _build_calibration_slack_message(self._sample_calibration(connection_rate=10.0))
        text = str(msg)
        assert "🔴" in text

    def test_recommendations_in_output(self):
        calib = self._sample_calibration()
        calib["recommendations"] = ["Test-Empfehlung XYZ"]
        msg = _build_calibration_slack_message(calib)
        text = str(msg)
        assert "Test-Empfehlung XYZ" in text

    def test_empty_tier_stats_still_valid(self):
        calib = {
            "total_contacts": 0,
            "tier_stats": [],
            "recommendations": ["Keine Daten."],
        }
        msg = _build_calibration_slack_message(calib)
        assert "blocks" in msg

    def test_multiple_tiers_all_rendered(self):
        calib = {
            "total_contacts": 30,
            "tier_stats": [
                {"tier": "1_hot", "label": "Hot", "total_calls": 10, "connection_rate": 60.0, "booking_rate": 30.0, "avg_score": 80.0},
                {"tier": "2_warm", "label": "Warm", "total_calls": 10, "connection_rate": 40.0, "booking_rate": 15.0, "avg_score": 55.0},
                {"tier": "3_cold", "label": "Cold", "total_calls": 10, "connection_rate": 20.0, "booking_rate": 5.0, "avg_score": 30.0},
            ],
            "recommendations": ["Alles gut."],
        }
        msg = _build_calibration_slack_message(calib)
        text = str(msg)
        assert "Hot" in text
        assert "Warm" in text
        assert "Cold" in text
