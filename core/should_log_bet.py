from core.config import DEBUG_MODE, VERBOSE_MODE

# Minimum stake thresholds used across the staking pipeline
MIN_FIRST_STAKE = 1.0
MIN_TOPUP_STAKE = 0.5

# Round stakes to this precision across the pipeline
ROUND_STAKE_TO = 0.01

# Odds outside this range are ignored for logging
MAX_POSITIVE_ODDS = 200
MIN_NEGATIVE_ODDS = -150

# Minimum EV requirements by market segment
MIN_EV_THRESHOLDS = {
    "1st_3": 0.08,         # Require 8% EV for 1st 3 innings markets
    "1st_7": 0.08,         # Require 8% EV for 1st 7 innings markets
    "1st": 0.10,           # Require 10% EV for 1st inning markets
    "1st_5": 0.05,         # Keep 5% EV for 1st 5 innings
    "team_totals": 0.08,   # Raise to 8% EV for team totals
    "spread": 0.05,        # Standard 5% EV for spreads
    "total": 0.05,         # Standard 5% EV for totals
    "h2h": 0.05,           # Standard 5% EV for moneyline bets
    "h2h_1st_5": 0.04,     # Allow more volume at 4% EV for h2h 1st 5
}

from core.market_pricer import decimal_odds
from core.confirmation_utils import required_market_move, book_agreement_score
from core.skip_reasons import SkipReason
from core.logger import get_logger
import csv
import os

from core.theme_key_utils import make_theme_key, theme_key_equals


from core.utils import (
    normalize_label_for_odds,
    classify_market_segment,
    TEAM_ABBR_TO_NAME,
    TEAM_NAME_TO_ABBR,
)


def round_stake(stake: float, precision: float = ROUND_STAKE_TO) -> float:
    """Return ``stake`` rounded to the nearest ``precision``."""
    return round(stake / precision) * precision


def _log_verbose(msg: str, verbose: bool = True) -> None:
    if verbose:
        print(msg)


def normalize_market_key(market: str) -> str:
    """Return a canonical key for a market name."""
    base = market.replace("alternate_", "").lower()
    if base.startswith("totals") or base.startswith("team_totals"):
        return "total"
    if base.startswith("spreads") or base.startswith("runline"):
        return "spread"
    if base in {"h2h", "moneyline"} or base.startswith("h2h") or base.startswith("moneyline"):
        return "h2h"
    return base


def get_theme(bet: dict) -> str:
    """Return the exposure theme for a bet."""
    side = bet["side"].strip()
    market = bet["market"].replace("alternate_", "")

    # 🆕 Handle team total bets like "ATL Over 4.5" or "Los Angeles Over 5.0"
    if "team_totals" in market:
        _, direction = parse_team_total_side(side)
        if direction:
            return direction

    if side.startswith("Over"):
        return "Over"
    if side.startswith("Under"):
        return "Under"

    if "h2h" in market or "spreads" in market or "runline" in market:
        tokens = side.split()
        if tokens:
            first = tokens[0]
            if first.upper() in TEAM_ABBR_TO_NAME:
                return first.upper()
            if first.title() in TEAM_NAME_TO_ABBR:
                return TEAM_NAME_TO_ABBR[first.title()]
        for name in TEAM_NAME_TO_ABBR:
            if side.startswith(name):
                return name
    return "Other"


def get_theme_key(market: str, theme: str) -> str:
    """Return a theme key combining theme name with a normalized market."""
    key = normalize_market_key(market)
    if key in {"total", "spread", "h2h"}:
        return f"{theme}_{key}"
    return f"{theme}_other"


def get_segment_group(market: str) -> str:
    base = market.replace("alternate_", "")
    seg = classify_market_segment(base)
    return "derivative" if seg != "full_game" else "full_game"


def normalize_segment(market: str) -> str:
    """Return a unified segment tag from a raw market name."""
    m = market.lower()
    if "1st_3" in m:
        return "1st_3"
    if "1st_5" in m:
        return "1st_5"
    if "1st_7" in m:
        return "1st_7"
    if "1st_1" in m or "1st_inning" in m:
        return "1st"
    return "full_game"


def parse_team_total_side(side: str) -> tuple[str, str]:
    """Return team abbreviation and direction from a team total label."""
    tokens = side.split()
    direction = "Over" if "Over" in tokens else "Under" if "Under" in tokens else ""

    team_abbr = None
    # common formats: 'ATL Over 4.5' or 'Over 4.5 ATL'
    for token in tokens:
        if token.upper() in TEAM_ABBR_TO_NAME:
            team_abbr = token.upper()
            break
        if token.title() in TEAM_NAME_TO_ABBR:
            team_abbr = TEAM_NAME_TO_ABBR[token.title()]
            break

    if not team_abbr:
        team_abbr = tokens[0].upper()

    return team_abbr, direction


def get_bet_group_key(bet: dict) -> str:
    """Classify a bet into a group key for staking logic."""
    market = bet["market"].lower()
    segment = classify_market_segment(market)

    if market in {"h2h", "spreads", "runline"}:
        return "mainline_spread_h2h"
    if market.startswith(("h2h_", "spreads_", "runline_")):
        return f"derivative_spread_h2h_{segment}"
    if market.startswith("totals") and not market.startswith("team_totals"):
        return f"totals_{segment}"
    if market.startswith("team_totals"):
        team, direction = parse_team_total_side(bet["side"])
        return f"team_total_{team}_{direction}"
    return f"{market}_{segment}"


def orientation_key(bet: dict) -> str:
    """Return a simplified orientation key used to detect opposing bets."""
    market = bet["market"].lower()
    side = bet["side"]

    if market.startswith("team_totals"):
        team, direction = parse_team_total_side(side)
        return f"{team}_{direction.lower()}"
    if market.startswith("totals"):
        return "over" if "over" in side.lower() else "under"
    # spreads/h2h/runline -> use team abbreviation
    tokens = side.split()
    team = tokens[0]
    if team.title() in TEAM_NAME_TO_ABBR:
        team = TEAM_NAME_TO_ABBR[team.title()]
    return team.upper()


def build_skipped_evaluation(
    reason: str, game_id: str | None = None, bet: dict | None = None
) -> dict:
    """Return a consistent structure for skipped evaluations."""
    result = {
        "game_id": game_id,
        "log": False,
        "full_stake": 0.0,
        "skip_reason": reason,
        "skip": True,
        "reason": reason,
    }
    if bet is not None:
        result.update(bet)
    return result


def _compute_csv_theme_total(
    game_id: str,
    theme_key: str,
    segment: str,
    csv_stakes: dict,
) -> float:
    """Return cumulative stake for a theme based on CSV stake mapping."""
    total = 0.0
    target = make_theme_key(game_id, theme_key, segment)
    for (gid, mkt, side), stake in csv_stakes.items():
        if gid != game_id:
            continue
        base = mkt.replace("alternate_", "")
        seg = normalize_segment(mkt)
        theme = get_theme({"side": side, "market": base})
        key = get_theme_key(base, theme)
        current = make_theme_key(gid, key, seg)
        if theme_key_equals(current, target):
            try:
                total += float(stake)
            except Exception:
                continue
    return total


def theme_already_logged_in_csv(
    csv_path: str, game_id: str, theme_key: str, segment: str
) -> bool:
    """Return ``True`` if a matching theme entry exists in ``csv_path``."""
    if not csv_path or not os.path.exists(csv_path):
        return False

    try:
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            target = make_theme_key(game_id, theme_key, segment)
            for row in reader:
                gid = row.get("game_id")
                market = row.get("market")
                side = row.get("side")
                if not gid or not market or not side:
                    continue
                base = market.replace("alternate_", "")
                seg = normalize_segment(market)
                theme = get_theme({"side": side, "market": base})
                key = get_theme_key(base, theme)
                current = make_theme_key(gid, key, seg)
                if theme_key_equals(current, target):
                    return True
    except Exception:
        pass
    return False


def should_log_bet(
    row: dict,
    exposure: dict,
    verbose: bool = True,
    min_ev: float = 0.05,
    eval_tracker: dict | None = None,
    reference_tracker: dict | None = None,
) -> dict:
    """Purely evaluate whether ``row`` should be logged.

    The function performs no side effects and does not mutate ``row`` or
    ``exposure``.  The returned dictionary includes:

    - ``log``: ``True`` when the bet should be logged.
    - ``stake``: stake amount to log (full stake or top-up delta).
    - ``entry_type``: ``"first"``, ``"top-up```, or ``"none``.
    - ``movement``: difference between current and baseline probabilities.
    - ``required_movement``: confirmation threshold applied.
    - ``skip_reason``: reason for skipping when ``log`` is ``False``.
    
    ``eval_tracker`` and ``reference_tracker`` provide baseline snapshot data
    used to calculate market movement.
    """

    new_bet = row.copy()

    game_id = new_bet["game_id"]
    market = new_bet["market"]
    side = normalize_label_for_odds(new_bet["side"], market)
    new_bet["side"] = side  # ensure consistent formatting

    raw_kelly = float(new_bet.get("raw_kelly", 0.0))
    stake = round_stake(raw_kelly * 0.25)

    print(f"🔍 DEBUG: raw_kelly from row = {raw_kelly}")
    print(f"🎯 Calculated stake from raw_kelly × 0.25 = {stake}")

    ev = float(new_bet.get("ev_percent", 0.0))

    segment = normalize_segment(market)
    try:
        hours_to_game = float(new_bet.get("hours_to_game"))
    except Exception:
        hours_to_game = None

    if DEBUG_MODE and ev >= 10.0 and stake >= 2.0:
        logger = get_logger(__name__)
        logger.debug(f"High EV bet passed thresholds: EV={ev:.2f}%, Stake={stake:.2f}u")

    odds_value = None
    try:
        odds_value = float(new_bet.get("market_odds"))
    except Exception:
        pass

    if odds_value is not None:
        if odds_value > MAX_POSITIVE_ODDS or odds_value < MIN_NEGATIVE_ODDS:
            _log_verbose(
                "⛔ should_log_bet: Rejected due to odds out of range",
                verbose,
            )
            return {
                **new_bet,
                "log": False,
                "skip": True,
                "stake": 0.0,
                "entry_type": "none",
                "movement": 0.0,
                "required_movement": 0.0,
                "skip_reason": "bad_odds",
                "reason": "bad_odds",
            }

    # Determine EV% threshold based on market type and segment
    base_market = market.replace("alternate_", "").lower()
    if base_market.startswith("team_totals"):
        category = "team_totals"
    else:
        category = normalize_market_key(base_market)
    combo_key = f"{category}_{segment}"

    # Determine the threshold to use
    threshold_frac = min_ev  # default fallback
    if combo_key in MIN_EV_THRESHOLDS:
        threshold_frac = MIN_EV_THRESHOLDS[combo_key]
    elif segment in MIN_EV_THRESHOLDS:
        threshold_frac = MIN_EV_THRESHOLDS[segment]
    elif category in MIN_EV_THRESHOLDS:
        threshold_frac = MIN_EV_THRESHOLDS[category]

    if ev < threshold_frac * 100:
        if verbose:
            print(
                f"⛔ should_log_bet: Rejected due to EV threshold → EV: {ev:.2f}%, Required: {threshold_frac * 100:.2f}%"
            )
        return {
            **new_bet,
            "log": False,
            "skip": True,
            "stake": 0.0,
            "entry_type": "none",
            "movement": 0.0,
            "required_movement": threshold_frac,
            "skip_reason": "low_ev",
            "reason": "low_ev",
        }

    base_market = market.replace("alternate_", "")
    theme = get_theme({"side": side, "market": base_market})
    theme_key = get_theme_key(base_market, theme)
    exposure_key = make_theme_key(game_id, theme_key, segment)
    theme_total = exposure.get(exposure_key, 0.0)
    delta_base = theme_total
    is_alt_line = (
        market.startswith("alternate_") or new_bet.get("market_class") == "alternate"
    )

    prior_entry = None
    t_key = f"{game_id}:{market}:{side}"

    if reference_tracker is not None:
        tracker_entry = reference_tracker.get(t_key)
        if isinstance(tracker_entry, dict):
            prior_entry = tracker_entry

    if prior_entry is None and eval_tracker is not None:
        tracker_entry = eval_tracker.get(t_key)
        if isinstance(tracker_entry, dict):
            prior_entry = tracker_entry

    # 🆕 Track early bets for potential confirmation-based top-ups
    # ``segment`` and ``hours_to_game`` defined earlier

    prev_prob = None
    if prior_entry is not None:
        prev_prob = prior_entry.get("consensus_prob")
        if prev_prob is None:
            prev_prob = prior_entry.get("market_prob")
    curr_prob = new_bet.get("consensus_prob")
    if curr_prob is None:
        curr_prob = new_bet.get("market_prob")
    movement = 0.0
    try:
        if prev_prob is not None and curr_prob is not None:
            movement = float(curr_prob) - float(prev_prob)
    except Exception:
        movement = 0.0

    books = new_bet.get("per_book")
    book_count = len(books) if isinstance(books, dict) and books else 1
    agreement = book_agreement_score(new_bet.get("per_book", {}))
    threshold = required_market_move(
        hours_to_game or 8,
        book_count=book_count,
        market=new_bet.get("market"),
        ev_percent=new_bet.get("ev_percent"),
        agreement=agreement,
    )
    if prev_prob is not None and movement < threshold and theme_total == 0:
        if verbose:
            print(
                "⏸️ Market move did not meet confirmation threshold. Skipping log."
            )
        return {
            **new_bet,
            "log": False,
            "skip": True,
            "stake": 0.0,
            "entry_type": "none",
            "movement": movement,
            "required_movement": threshold,
            "skip_reason": "not_confirmed",
            "reason": "not_confirmed",
        }


    tracker_key = f"{game_id}:{market}:{side}"

    # Restrict early bets for low-liquidity segments (1st_3, 1st_7, team_totals)
    if segment in {"1st_3", "1st_7", "team_totals"} and hours_to_game is not None and hours_to_game > 12:
        if verbose:
            print(f"⏳ should_log_bet: Queued {segment} bet — too early (>12h to game)")
        return {
            **new_bet,
            "log": False,
            "skip": True,
            "stake": 0.0,
            "entry_type": "none",
            "movement": movement,
            "required_movement": threshold,
            "skip_reason": "time_blocked",
            "reason": "time_blocked",
        }

    if theme_total == 0:
        stake_to_log = round_stake(stake)
        if stake_to_log < MIN_FIRST_STAKE:
            _log_verbose(
                f"⛔ Skipping bet — scaled stake {stake_to_log}u is below {MIN_FIRST_STAKE:.1f}u minimum",
                verbose,
            )
            return {
                **new_bet,
                "log": False,
                "skip": True,
                "stake": 0.0,
                "entry_type": "none",
                "movement": movement,
                "required_movement": threshold,
                "skip_reason": SkipReason.LOW_INITIAL.value,
                "reason": SkipReason.LOW_INITIAL.value,
            }

        _log_verbose(
            f"✅ should_log_bet: First bet → {side} | {theme_key} [{segment}] | Stake: {stake_to_log:.2f}u | EV: {ev:.2f}%",
            verbose,
        )
        return {
            **new_bet,
            "log": True,
            "skip": False,
            "stake": stake_to_log,
            "entry_type": "first",
            "movement": movement,
            "required_movement": threshold,
            "skip_reason": None,
        }

    # Round the delta once to avoid floating point drift across the pipeline
    delta_raw = stake - delta_base
    delta = round_stake(delta_raw)
    if delta >= MIN_TOPUP_STAKE:
        _log_verbose(
            f"🔼 should_log_bet: Top-up accepted → {side} | {theme_key} [{segment}] | Δ {delta:.2f}u",
            verbose,
        )
        return {
            **new_bet,
            "log": True,
            "skip": False,
            "stake": delta,
            "entry_type": "top-up",
            "movement": movement,
            "required_movement": threshold,
            "skip_reason": None,
        }

    if delta > 0:
        _log_verbose(
            f"🔄 Delta stake {delta:.2f}u below minimum top-up threshold",
            verbose,
        )
        return {
            **new_bet,
            "log": False,
            "skip": True,
            "stake": 0.0,
            "entry_type": "none",
            "movement": movement,
            "required_movement": threshold,
            "skip_reason": SkipReason.LOW_TOPUP.value,
            "reason": SkipReason.LOW_TOPUP.value,
        }

    _log_verbose(
        f"⛔ Delta stake {delta:.2f}u < {MIN_TOPUP_STAKE:.1f}u minimum",
        verbose,
    )
    return {
        **new_bet,
        "log": False,
        "skip": True,
        "stake": 0.0,
        "entry_type": "none",
        "movement": movement,
        "required_movement": threshold,
        "skip_reason": SkipReason.ALREADY_LOGGED.value,
        "reason": SkipReason.ALREADY_LOGGED.value,
    }