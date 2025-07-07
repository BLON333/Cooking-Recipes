import logging
from core.utils import lookup_fallback_odds


def test_lookup_fallback_odds_exact():
    odds = {
        "2025-07-07-TOR@CWS-T1941": {"val": 1},
    }
    assert lookup_fallback_odds("2025-07-07-TOR@CWS-T1941", odds) == {"val": 1}


def test_lookup_fallback_odds_fuzzy_single():
    odds = {
        "2025-07-07-TOR@CWS-T1941": {"val": 1},
    }
    assert lookup_fallback_odds("2025-07-07-TOR@CWS-T1940", odds) == {"val": 1}


def test_lookup_fallback_odds_choose_smallest_delta():
    odds = {
        "2025-07-07-TOR@CWS-T1939": {"k": "a"},
        "2025-07-07-TOR@CWS-T1941": {"k": "b"},
        "2025-07-07-TOR@CWS-T2000": {"k": "c"},
    }
    assert lookup_fallback_odds("2025-07-07-TOR@CWS-T1940", odds) == {"k": "a"}


def test_lookup_fallback_odds_none():
    odds = {
        "2025-07-07-TOR@CWS-T2000": {"k": "c"},
    }
    assert lookup_fallback_odds("2025-07-08-TOR@CWS-T1940", odds) is None
