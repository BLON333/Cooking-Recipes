"""Utilities for confirming market signals."""

from __future__ import annotations
from typing import Optional

from core.config import DEBUG_MODE, VERBOSE_MODE
from core.market_pricer import kelly_fraction

__all__ = [
    "required_market_move",
    "print_threshold_table",
    "extract_book_count",
    "evaluate_late_confirmed_bet",
]

# Alignment-first betting strategy:
# - Only place a first bet when market movement confirms the model edge.
# - Top-ups now also require market confirmation using the same
#   dynamic movement threshold applied to first bets.

# Toggle for optional debug logging
VERBOSE = False

# Minimum additional stake required to generate a top-up bet
MIN_TOPUP_STAKE = 0.5


def extract_book_count(obj: dict) -> int:
    """Safely extract the number of books used from ``books_used`` field."""
    books_used = obj.get("books_used")
    if isinstance(books_used, str):
        return len([b.strip() for b in books_used.split(",") if b.strip()])
    if isinstance(books_used, list):
        return len(books_used)
    return 1


def required_market_move(
    hours_to_game: float,
    book_count: int = 1,
    market: str | None = None,
    ev_percent: float | None = None,
) -> float:
    """Return required consensus probability movement for confirmation.

    Parameters
    ----------
    hours_to_game : float
        Hours until the start of the game.
    book_count : int, optional
        Number of sportsbooks contributing to the consensus line. If ``None`` or
        invalid this defaults to ``1``.
    market : str, optional
        Market identifier used for applying volatility adjustments.
    ev_percent : float, optional
        Model-derived expected value percentage. Higher EV loosens the
        requirement while lower EV tightens it.

    Returns
    -------
    float
        Minimum consensus implied probability delta after all adjustments.
    """

    movement_unit = 0.0045

    # Base multipliers using hours to game and number of books available
    try:
        hours = float(hours_to_game)
    except Exception:
        hours = 0.0
    time_multiplier = 1.0 + max((hours - 6.0) / 24.0, 0.0)

    try:
        books = int(book_count)
    except Exception:
        books = 1
    book_multiplier = 1.0 + 0.3 * max(3 - books, 0)

    base_threshold = movement_unit * time_multiplier * book_multiplier

    # Loosen confirmation for full-game totals and spreads using a tiered rule
    full_game = (
        market
        and (
            market.startswith("totals")
            or market.startswith("spreads")
            or market.startswith("runline")
        )
        and "1st_" not in market
        and "1st" not in market
        and "team_totals" not in market
    )
    if full_game:
        try:
            ev_val = float(ev_percent)
        except Exception:
            ev_val = None
        if ev_val is not None and 10.0 <= ev_val <= 20.0:
            base_threshold *= 0.25
        else:
            base_threshold *= 0.50

    # Volatile segments like first inning or team totals require more movement
    if market and (
        "1st_3" in market or "1st_7" in market or "team_totals" in market
    ):
        base_threshold *= 1.5

    # EV adjustments – very high EV loosens, middling EV tightens slightly
    if ev_percent is not None:
        try:
            ev_val = float(ev_percent)
        except Exception:
            ev_val = None
        if ev_val is not None:
            if ev_val >= 12.0:
                base_threshold *= 0.8
            elif 5.0 <= ev_val <= 7.0:
                base_threshold *= 1.25

    return base_threshold


def print_threshold_table() -> None:
    """Print required market move thresholds at key hours.

    The table shows how much consensus line movement is needed for
    confirmation at selected hours leading up to a game.
    """

    key_hours = [24, 18, 12, 6, 3, 1, 0]
    print("[Hours to Game] | [Required Move (%)] | [Movement Units]")
    for hours in key_hours:
        threshold = required_market_move(hours, book_count=7)
        percent = threshold * 100.0
        units = threshold / 0.0045
        print(f"{hours:>3}h | {percent:>6.3f}% | {units:>5.2f}")


def evaluate_late_confirmed_bet(
    bet: dict,
    new_consensus_prob: float,
    existing_stake: float,
) -> Optional[dict]:
    """Return a bet update if confirmation rules are met."""

    try:
        hours = float(bet.get("hours_to_game"))
    except Exception:
        return None

    try:
        prev_prob = bet.get("baseline_consensus_prob")
        if prev_prob is None:
            prev_prob = bet.get("consensus_prob")
        prev_prob = float(prev_prob)
    except Exception:
        return None

    try:
        new_prob = float(new_consensus_prob)
    except Exception:
        return None

    # Determine how many books were used to form the consensus line
    count = extract_book_count(bet)

    movement = new_prob - prev_prob

    prob = (
        bet.get("blended_prob")
        or bet.get("sim_prob")
        or bet.get("consensus_prob")
        or new_prob
    )
    odds = bet.get("market_odds")
    if odds is None:
        return None

    try:
        prob_val = float(prob)
        odds_val = float(odds)
    except Exception:
        return None

    fraction = 0.125 if bet.get("market_class") == "alternate" else 0.25
    raw_kelly = bet.get("raw_kelly")
    if raw_kelly is None:
        raw_kelly = kelly_fraction(prob_val, odds_val, fraction=fraction)
    try:
        raw_kelly = float(raw_kelly)
    except Exception:
        raw_kelly = 0.0

    entry_type = str(bet.get("entry_type", "first")).lower()
    required_move = required_market_move(
        hours,
        book_count=count,
        market=bet.get("market"),
        ev_percent=bet.get("ev_percent"),
    )
    strength = movement / required_move if required_move > 0 else 0.0

    # Initial bet: require confirmation threshold before staking
    if entry_type == "first":
        if strength < 1.0:
            updated = bet.copy()
            updated.update(
                {
                    "stake": 0.0,
                    "entry_type": "first",
                    "skip_reason": "not_confirmed",
                    "consensus_prob": new_prob,
                    "market_prob": new_prob,
                }
            )
            return updated

        # 🔁 Using raw Kelly stake without confirmation scaling
        print(f"🔁 Using raw_kelly stake without confirmation scaling: {raw_kelly:.4f}")
        target_stake = round(raw_kelly, 4)
        try:
            max_full = float(bet.get("full_stake", target_stake))
        except Exception:
            max_full = target_stake
        target_stake = min(target_stake, max_full)
        updated = bet.copy()
        updated.update(
            {
                "stake": target_stake,
                "full_stake": target_stake,
                "entry_type": "first",
                "consensus_prob": new_prob,
                "market_prob": new_prob,
            }
        )
        return updated

    # Top-up: scale to raw Kelly ignoring confirmation
    target_stake = raw_kelly
    try:
        max_full = float(bet.get("full_stake", target_stake))
    except Exception:
        max_full = target_stake

    target_stake = min(target_stake, max_full)
    delta = round(target_stake - float(existing_stake), 2)

    if delta < MIN_TOPUP_STAKE:
        return None

    updated = bet.copy()
    updated.update(
        {
            "stake": delta,
            "full_stake": target_stake,
            "entry_type": "top-up",
            "consensus_prob": new_prob,
            "market_prob": new_prob,
        }
    )
    return updated