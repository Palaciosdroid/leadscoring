"""Tests for interest category detection."""

from scoring.interest import detect_interest_category, _extract_category_from_url


class TestExtractCategoryFromUrl:
    def test_hypnose_url(self):
        assert _extract_category_from_url("https://sbc.de/hypnosecoach-ausbildung") == "hypnose"

    def test_lifecoach_url(self):
        assert _extract_category_from_url("https://sbc.de/lifecoach-programm") == "lifecoach"

    def test_meditation_url(self):
        assert _extract_category_from_url("https://sbc.de/meditationscoach") == "meditation"

    def test_no_match(self):
        assert _extract_category_from_url("https://sbc.de/impressum") is None

    def test_case_insensitive(self):
        assert _extract_category_from_url("https://sbc.de/HYPNOSE-kurs") == "hypnose"

    def test_breathwork_is_meditation(self):
        assert _extract_category_from_url("https://sbc.de/breathwork-seminar") == "meditation"


class TestDetectInterestCategory:
    def test_empty_events(self):
        result = detect_interest_category([])
        assert result["category"] is None
        assert result["confidence"] == 0.0

    def test_single_page_visit(self):
        events = [{"event_type": "page_visited", "url": "https://sbc.de/hypnose-ausbildung"}]
        result = detect_interest_category(events)
        assert result["category"] == "hypnose"
        assert result["confidence"] == 1.0

    def test_mixed_signals_strongest_wins(self):
        events = [
            {"event_type": "page_visited", "url": "https://sbc.de/hypnose"},       # 1 point
            {"event_type": "page_visited", "url": "https://sbc.de/hypnose-preis"}, # 1 point
            {"event_type": "checkout_visited", "url": "https://sbc.de/meditation-checkout"}, # 12 points
        ]
        result = detect_interest_category(events)
        assert result["category"] == "meditation"

    def test_metadata_detection(self):
        events = [{
            "event_type": "video_watched_100",
            "url": "",
            "metadata": {"video_title": "Hypnose Basics Modul 1"},
        }]
        result = detect_interest_category(events)
        assert result["category"] == "hypnose"

    def test_confidence_with_two_categories(self):
        events = [
            {"event_type": "sales_page_visited", "url": "https://sbc.de/hypnose"},     # 3
            {"event_type": "sales_page_visited", "url": "https://sbc.de/meditation"},   # 3
        ]
        result = detect_interest_category(events)
        # Equal scores → 50/50 confidence
        assert result["confidence"] == 0.5

    def test_no_url_no_metadata(self):
        events = [{"event_type": "email_opened"}]
        result = detect_interest_category(events)
        assert result["category"] is None

    def test_application_submitted_strong_signal(self):
        events = [
            {"event_type": "page_visited", "url": "https://sbc.de/meditation"},  # 1
            {"event_type": "application_submitted", "url": "https://sbc.de/lifecoach-bewerbung"},  # 15
        ]
        result = detect_interest_category(events)
        assert result["category"] == "lifecoach"
