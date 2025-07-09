import pytest

from core.should_log_bet import should_log_bet
from core.skip_reasons import SkipReason


def _base_bet(required_move):
    return {
        "game_id": "GAME1",
        "market": "totals",
        "side": "Over 8.5",
        "baseline_consensus_prob": 0.50,
        "market_prob": 0.518,
        "consensus_move": 0.018,
        "required_move": required_move,
        "ev_percent": 6.0,
        "raw_kelly": 1.0,
        "hours_to_game": 3,
        "market_odds": -110,
        "best_book": "fanduel",
        "_raw_sportsbook": {"fanduel": -110},
    }


def test_required_move_overrides_default():
    bet = _base_bet(0.02)
    result = should_log_bet(bet.copy(), {}, existing_csv_stakes={})
    assert result["skip"] is True
    assert result["reason"] == SkipReason.MARKET_NOT_MOVED.value


def test_log_when_move_exceeds_required():
    bet = _base_bet(0.015)
    result = should_log_bet(bet.copy(), {}, existing_csv_stakes={})
    assert result["log"] is True
    assert result["entry_type"] == "first"
