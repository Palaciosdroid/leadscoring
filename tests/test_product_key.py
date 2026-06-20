"""product_key robustness — NULL/unknown keys must not crash and never be guessed.

Covers `classify_product_key` and `_extract_purchased_funnels` against the real
data hazard: ~35 Supabase purchases carry a NULL/empty `product_key`. Those must
surface as 'unclassified' (logged), never silently mapped to a funnel.
"""

import logging

from batch.scorer import (
    classify_product_key,
    _extract_purchased_funnels,
    UNCLASSIFIED_PRODUCT_KEY,
)


# (raw product_key, expected classification)
CLASSIFY_CASES = [
    ("hc", "hc"),
    ("MC", "mc"),
    (" gc ", "gc"),
    ("afk", "afk"),
    (None, UNCLASSIFIED_PRODUCT_KEY),      # NULL from Supabase
    ("", UNCLASSIFIED_PRODUCT_KEY),        # empty string
    ("   ", UNCLASSIFIED_PRODUCT_KEY),     # whitespace only
    ("xyz", UNCLASSIFIED_PRODUCT_KEY),     # unknown key — not guessed
]


def test_classify_product_key():
    for raw, expected in CLASSIFY_CASES:
        assert classify_product_key(raw) == expected, raw


def test_classify_logs_unclassified(caplog):
    with caplog.at_level(logging.DEBUG, logger="batch.scorer"):
        classify_product_key(None)
    assert any("unclassified" in r.message for r in caplog.records)


def test_extract_funnels_handles_null_product_key():
    """A NULL product_key must not crash and must not map to any funnel."""
    purchases = [
        {"product_key": None, "product_name": "Mystery Product"},
        {"product_key": "", "product_name": ""},
        {"product_key": "hc", "product_name": "Hypnosecoach Ausbildung"},
    ]
    funnels = _extract_purchased_funnels(purchases)
    assert funnels == ["hypnose"]  # only the known hc key produced a funnel


def test_extract_funnels_unknown_key_not_guessed():
    """An unknown product_key yields no funnel (no guessing)."""
    purchases = [{"product_key": "totally-new-sku", "product_name": "New Thing"}]
    assert _extract_purchased_funnels(purchases) == []


def test_extract_funnels_empty_list():
    assert _extract_purchased_funnels([]) == []
