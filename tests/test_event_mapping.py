"""Tests for Customer.io event mapping logic in main.py."""

from main import _map_cio_event, CIO_EVENT_MAP


class TestMapCioEvent:
    """Test the Customer.io → internal event type mapping."""

    # --- Page events resolved by URL ---

    def test_page_checkout(self):
        raw = {"event": "page", "data": {"page": {"url": "https://sbc.de/checkout/step1"}}}
        assert _map_cio_event(raw) == "checkout_visited"

    def test_page_sales(self):
        raw = {"event": "page", "data": {"page": {"url": "https://sbc.de/hypnose-ausbildung"}}}
        assert _map_cio_event(raw) == "sales_page_visited"

    def test_page_price(self):
        raw = {"event": "page", "data": {"page": {"url": "https://sbc.de/preise-und-kosten"}}}
        assert _map_cio_event(raw) == "price_info_viewed"

    def test_page_generic(self):
        raw = {"event": "page", "data": {"page": {"url": "https://sbc.de/blog/article"}}}
        assert _map_cio_event(raw) == "page_visited"

    # --- Video progress thresholds ---

    def test_video_progress_75(self):
        raw = {"event": "video_progress", "data": {"percent_complete": 80}}
        assert _map_cio_event(raw) == "video_watched_75"

    def test_video_progress_50(self):
        raw = {"event": "video_progress", "data": {"percent_complete": 55}}
        assert _map_cio_event(raw) == "video_watched_50"

    def test_video_progress_below_50_ignored(self):
        raw = {"event": "video_progress", "data": {"percent_complete": 30}}
        assert _map_cio_event(raw) is None

    def test_video_complete(self):
        raw = {"event": "video_complete", "data": {}}
        assert _map_cio_event(raw) == "video_watched_100"

    # --- Click events resolved by URL ---

    def test_click_checkout(self):
        raw = {"event": "click", "data": {"page": {"url": "https://sbc.de/buy-now"}}}
        assert _map_cio_event(raw) == "checkout_visited"

    def test_click_sales_cta(self):
        raw = {"event": "click", "data": {"page": {"url": "https://sbc.de/coaching-programm"}}}
        assert _map_cio_event(raw) == "cta_clicked"

    def test_click_generic_ignored(self):
        raw = {"event": "click", "data": {"page": {"url": "https://sbc.de/blog"}}}
        assert _map_cio_event(raw) is None

    # --- Direct mappings ---

    def test_email_opened(self):
        raw = {"event": "email_opened", "data": {}}
        assert _map_cio_event(raw) == "email_opened"

    def test_email_clicked_alias(self):
        raw = {"event": "email_clicked", "data": {}}
        assert _map_cio_event(raw) == "email_link_clicked"

    def test_form_submit_maps_to_application(self):
        raw = {"event": "form_submit", "data": {}}
        assert _map_cio_event(raw) == "application_submitted"

    def test_unsubscribed_alias(self):
        raw = {"event": "unsubscribed", "data": {}}
        assert _map_cio_event(raw) == "email_unsubscribed"

    def test_webinar_attended(self):
        raw = {"event": "webinar_attended", "data": {}}
        assert _map_cio_event(raw) == "webinar_attended"

    def test_resource_downloaded(self):
        raw = {"event": "resource_downloaded", "data": {}}
        assert _map_cio_event(raw) == "free_resource_downloaded"

    def test_unknown_event_returns_none(self):
        raw = {"event": "some_random_thing", "data": {}}
        assert _map_cio_event(raw) is None

    # --- Edge cases ---

    def test_missing_data_key(self):
        raw = {"event": "page"}
        assert _map_cio_event(raw) == "page_visited"

    def test_none_data(self):
        raw = {"event": "page", "data": None}
        assert _map_cio_event(raw) == "page_visited"
