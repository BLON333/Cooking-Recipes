import logging
from core.utils import lookup_fallback_odds


def test_lookup_fallback_odds_exact():
    odds = {
        "2025-07-07-TOR@CWS-T1941": {"val": 1},
    }
    row, key = lookup_fallback_odds("2025-07-07-TOR@CWS-T1941", odds)
    assert row == {"val": 1}
    assert key == "2025-07-07-TOR@CWS-T1941"


def test_lookup_fallback_odds_fuzzy_single():
    odds = {
        "2025-07-07-TOR@CWS-T1941": {"val": 1},
    }
    row, key = lookup_fallback_odds("2025-07-07-TOR@CWS-T1940", odds)
    assert row == {"val": 1}
    assert key == "2025-07-07-TOR@CWS-T1941"


def test_lookup_fallback_odds_choose_smallest_delta():
    odds = {
        "2025-07-07-TOR@CWS-T1939": {"k": "a"},
        "2025-07-07-TOR@CWS-T1941": {"k": "b"},
        "2025-07-07-TOR@CWS-T2000": {"k": "c"},
    }
    row, key = lookup_fallback_odds("2025-07-07-TOR@CWS-T1940", odds)
    assert row == {"k": "a"}
    assert key == "2025-07-07-TOR@CWS-T1939"


def test_lookup_fallback_odds_none():
    odds = {
        "2025-07-07-TOR@CWS-T2000": {"k": "c"},
    }
    row, key = lookup_fallback_odds("2025-07-08-TOR@CWS-T1940", odds)
    assert row is None and key is None


def test_lookup_fallback_odds_fuzzy_off_by_one():
    odds = {
        "2025-07-07-TOR@CWS-T1941": {"val": 1},
        "2025-07-07-TOR@CWS-T1943": {"val": 2},
    }
    result, key = lookup_fallback_odds("2025-07-07-TOR@CWS-T1940", odds)
    assert result == {"val": 1}
    assert key == "2025-07-07-TOR@CWS-T1941"


def test_lookup_fallback_odds_unique_exceeds_delta(caplog):
    odds = {
        "2025-07-07-TOR@CWS-T2200": {"val": 99},
    }
    with caplog.at_level(logging.WARNING):
        row, key = lookup_fallback_odds("2025-07-07-TOR@CWS-T2140", odds)
    assert row == {"val": 99}
    assert key == "2025-07-07-TOR@CWS-T2200"
    assert any("only match" in rec.message for rec in caplog.records)
