import asyncio

from batch.do_not_call import check_do_not_call


def test_phone_dnc_skips():
    result = asyncio.run(
        check_do_not_call(email="x@y.de", funnel="hypnose", phone_dnc=True)
    )
    assert result.should_skip is True
    assert result.reason == "phone_dnc"


def test_clean_lead_is_callable():
    result = asyncio.run(
        check_do_not_call(email="x@y.de", funnel="hypnose")
    )
    assert result.should_skip is False
    assert result.reason == ""
