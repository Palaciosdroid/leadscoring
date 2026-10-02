"""Briefing cases 1-11 for the morning call list (fixed 'today')."""
from datetime import datetime
from zoneinfo import ZoneInfo

from batch.morgen_anrufliste import build_sections, evaluate, render_message, unique_count

Z = ZoneInfo("Europe/Zurich")
CONNECTED = "f240bbac-87c9-4f6e-bf70-924b57d47db7"
WRONG = "17b47fee-58de-441e-a44c-c6300d46f273"
BUSY = "9d9162e7-6cf3-4944-bf63-4dff82258764"
NO_ANSWER = "73a0d17f-1163-4015-bdd5-ec830791da20"

TODAY = datetime(2026, 10, 8, 7, 0, tzinfo=Z)  # Thursday
JOINED = datetime(2026, 9, 28, 12, 0, tzinfo=Z)


def call(y, m, d, h, mi, disposition, direction="OUTBOUND"):
    ts = datetime(y, m, d, h, mi, tzinfo=Z).astimezone(ZoneInfo("UTC"))
    return {"hs_timestamp": ts.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "hs_call_direction": direction, "hs_call_disposition": disposition}


def test_01_no_call_since_join_is_new():
    r = evaluate({}, JOINED, [], TODAY)
    assert (r.status, r.attempts) == ("neu", 0)


def test_02_call_before_join_only_is_new():
    r = evaluate({}, JOINED, [call(2026, 9, 20, 10, 0, CONNECTED)], TODAY)
    assert (r.status, r.attempts) == ("neu", 0)


def test_03_last_call_connected_is_out():
    assert evaluate({}, JOINED, [call(2026, 10, 1, 10, 0, CONNECTED)], TODAY).status is None


def test_04_last_call_wrong_number_is_out():
    assert evaluate({}, JOINED, [call(2026, 10, 1, 10, 0, WRONG)], TODAY).status is None


def test_05_no_answer_two_days_ago_is_out():
    r = evaluate({}, JOINED, [call(2026, 10, 6, 18, 0, NO_ANSWER)], TODAY)
    assert (r.status, r.reason) == (None, "noch_nicht_faellig")


def test_06_no_answer_three_calendar_days_ago_is_due():
    # Monday 23:30 -> back on Thursday morning
    r = evaluate({}, JOINED, [call(2026, 10, 5, 23, 30, NO_ANSWER)], TODAY)
    assert (r.status, r.attempts) == ("wiedervorlage", 1)


def test_07_four_attempts_is_out():
    calls = [call(2026, 9, 29, 10, 0, NO_ANSWER), call(2026, 9, 30, 10, 0, BUSY),
             call(2026, 10, 1, 10, 0, NO_ANSWER), call(2026, 10, 3, 10, 0, NO_ANSWER)]
    r = evaluate({}, JOINED, calls, TODAY)
    assert (r.status, r.reason) == (None, "obergrenze")


def test_08_three_attempts_last_busy_three_days_ago_is_due():
    calls = [call(2026, 9, 29, 10, 0, NO_ANSWER), call(2026, 10, 1, 10, 0, NO_ANSWER),
             call(2026, 10, 5, 9, 0, BUSY)]
    r = evaluate({}, JOINED, calls, TODAY)
    assert (r.status, r.attempts) == ("wiedervorlage", 3)


def test_09_lead_call_booked_is_out():
    assert evaluate({"lead_call_booked": "true"}, JOINED, [], TODAY).reason == "termin_gebucht"


def test_10_meeting_after_join_out_before_join_ignored():
    after = evaluate({"engagements_last_meeting_booked": "2026-09-30T08:00:00Z"}, JOINED, [], TODAY)
    before = evaluate({"engagements_last_meeting_booked": "2026-09-01T08:00:00Z"}, JOINED, [], TODAY)
    assert after.status is None and before.status == "neu"


def test_11_inbound_calls_do_not_count():
    r = evaluate({}, JOINED, [call(2026, 10, 7, 10, 0, CONNECTED, "INBOUND")], TODAY)
    assert (r.status, r.attempts) == ("neu", 0)


def test_unknown_disposition_is_a_retry():
    assert evaluate({}, JOINED, [call(2026, 10, 2, 10, 0, "")], TODAY).status == "wiedervorlage"


def test_epoch_millis_timestamps():
    ms = str(int(datetime(2026, 10, 5, 9, 0, tzinfo=Z).timestamp() * 1000))
    calls = [{"hs_timestamp": ms, "hs_call_direction": "OUTBOUND", "hs_call_disposition": BUSY}]
    assert evaluate({}, JOINED, calls, TODAY).status == "wiedervorlage"


def _lists():
    return [
        {"id": "492", "name": "MC", "members": [
            {"contact_id": "b", "joined_at": datetime(2026, 10, 2, 10, tzinfo=Z)},
            {"contact_id": "a", "joined_at": datetime(2026, 10, 1, 10, tzinfo=Z)}]},
        {"id": "491", "name": "GC", "members": [{"contact_id": "a", "joined_at": datetime(2026, 10, 3, 10, tzinfo=Z)}]},
        {"id": "445", "name": "HC", "members": [{"contact_id": "c", "joined_at": datetime(2026, 10, 1, 10, tzinfo=Z)}]},
    ]


def test_sections_sorted_hints_and_gate():
    contacts = {"a": {"firstname": "Anna"}, "b": {}, "c": {}}
    sections = build_sections(_lists(), contacts, {}, TODAY, suppressed={"c"})
    assert [s["name"] for s in sections] == ["MC", "GC"]
    assert [r.contact_id for r in sections[0]["rows"]] == ["a", "b"]
    assert sections[0]["rows"][0].also_in == ["GC"]
    assert unique_count(sections) == 2


def test_render_message():
    contacts = {"a": {"firstname": "Anna", "lastname": "Muster", "phone": "+41 79 123 45 67"}, "b": {}, "c": {}}
    calls = {"b": [call(2026, 10, 5, 8, 15, BUSY)]}
    text = render_message(build_sections(_lists(), contacts, calls, TODAY), TODAY, "@Kevin")
    assert text.startswith("@Kevin 📞 Anrufliste 08.10.2026 (3 Kontakte)")
    assert "🆕 Neu · Anna Muster · +41 79 123 45 67" in text
    assert "letzter Anruf: 05.10. 08:15 · Besetzt" in text
    assert "⚠️ auch in: GC" in text
    assert "https://app.hubspot.com/contacts/27034546/record/0-1/a" in text


def test_render_empty():
    assert "Heute keine offenen Anrufe." in render_message([], TODAY)
