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
from core.skip_reasons import SkipReason
from core.logger import get_logger
import csv
import os

from core.theme_key_utils import make_theme_key, theme_key_equals
from core.theme_exposure_tracker import build_theme_key
from core.theme_utils import (
    normalize_market_key,
    parse_team_total_side,
    get_theme,
    get_theme_key,
    normalize_segment,
)


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


def get_segment_group(market: str) -> str:
    base = market.replace("alternate_", "")
    seg = classify_market_segment(base)
    return "derivative" if seg != "full_game" else "full_game"


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
    new_bet: dict,
    existing_theme_stakes: dict,
    csv_exposure: dict | None = None,
    verbose: bool = True,
    min_ev: float = 0.05,
    min_stake: float = MIN_FIRST_STAKE,
    eval_tracker: dict | None = None,
    reference_tracker: dict | None = None,
    existing_csv_stakes: dict | None = None,
    csv_path: str | None = None,
) -> dict:
    """Evaluate whether a bet should be logged and return a structured result.

    Parameters
    ----------
    existing_csv_stakes : dict | None, optional
        Mapping of previously logged stakes keyed by ``(game_id, market, side)``.
        When provided, the function treats missing entries as no prior stake for
        that specific market even if theme exposure exists.
    csv_exposure : dict | None, optional
        Theme exposure loaded from ``market_evals.csv`` keyed by
        ``game_id:theme_key:segment``.  When provided, tracker exposure is
        ignored and this mapping is used exclusively.

    The optional ``eval_tracker`` should contain previous market evaluations
    keyed by ``game_id:market:side:book`` so line movement can be enforced for
    first-time entries.

    reference_tracker : dict | None, optional
        Optional frozen snapshot used for movement comparison when
        ``eval_tracker`` is updated externally.

    csv_path : str | None, optional
        Path to ``market_evals.csv`` for verifying previously logged exposure.
        When provided, the CSV is scanned to confirm any prior theme-level
        entry before classifying a bet as a top-up.
    """

    game_id = new_bet["game_id"]
    market = new_bet["market"]
    side = normalize_label_for_odds(new_bet["side"], market)
    new_bet["side"] = side  # ensure consistent formatting
    # ``full_stake`` may be absent in legacy entries; fall back to ``stake``
    # or 0.0 to avoid KeyError.
    stake = round_stake(
        float(
            new_bet.get(
                "raw_kelly",
                new_bet.get("full_stake", new_bet.get("stake", 0.0)),
            )
        )
    )
    ev = new_bet["ev_percent"]

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
            new_bet["entry_type"] = "none"
            new_bet["skip_reason"] = "bad_odds"
            return build_skipped_evaluation("bad_odds", game_id, new_bet)

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
        new_bet["entry_type"] = "none"
        new_bet["skip_reason"] = "low_ev"
        return build_skipped_evaluation("low_ev", game_id, new_bet)

    base_market = market.replace("alternate_", "")
    theme = get_theme({"side": side, "market": base_market})
    theme_key = get_theme_key(base_market, theme)
    exposure_key = make_theme_key(game_id, theme_key, segment)
    theme_total = 0.0
    if csv_exposure is not None:
        theme_total = csv_exposure.get(exposure_key, 0.0)

    tracker_theme_key = build_theme_key(new_bet)
    prior_stake = 0.0
    if csv_exposure is not None:
        prior_stake = csv_exposure.get(tracker_theme_key, 0.0)

    entry_type = "top-up" if prior_stake > 0 else "first"
    delta = max(0.0, stake - prior_stake)

    print(f"[DEBUG] Evaluating exposure for {theme_key}")
    print(f"  CSV Exposure: {prior_stake}")
    print(f"  Target Stake: {stake}")
    print(f"  Stake Delta: {stake - prior_stake}")
    print(f"  Entry Type: {'top-up' if prior_stake > 0 else 'first'}")

    if entry_type == "top-up" and delta < MIN_TOPUP_STAKE:
        entry_type = "none"
        skip_reason = SkipReason.LOW_TOPUP.value
    new_bet["entry_type"] = entry_type
    csv_stake = 0.0
    if existing_csv_stakes is not None:
        csv_stake = existing_csv_stakes.get((game_id, market, side), 0.0)

    # When the row indicates it has not been logged and we do not see a matching
    # entry in ``existing_csv_stakes``, treat it as a fresh bet regardless of any
    # theme-level exposure that may exist.
    if new_bet.get("logged") is False and csv_stake == 0:
        theme_total = 0.0

    if theme_total >= stake and existing_csv_stakes is not None:
        csv_theme_total = _compute_csv_theme_total(
            game_id, theme_key, segment, existing_csv_stakes
        )
        if csv_theme_total == 0:
            theme_total = 0.0

    # ``delta_base`` represents the portion of stake already logged for this
    # particular bet.  Theme totals are still tracked for informational purposes
    # but we base delta calculations on the market-level CSV record when
    # available.
    delta_base = csv_stake if csv_stake > 0 else theme_total
    if theme_total >= stake and csv_stake == 0:
        print(
            f"⚠️ Theme stake exists ({theme_total}) but no CSV stake for {side}. Tracker may be stale."
        )
        delta_base = 0.0
    if new_bet.get("entry_type") in {"first", "top-up"} and new_bet.get("consensus_move", 0.0) < new_bet.get("required_move", 0.0):
        new_bet["skip_reason"] = "not_confirmed"
        new_bet["stake"] = 0.0
        return build_skipped_evaluation("not_confirmed", game_id, new_bet)

    tracker_key = f"{game_id}:{market}:{side}"

    if theme_total > 0 and csv_path is not None:
        if not theme_already_logged_in_csv(csv_path, game_id, theme_key, segment):
            _log_verbose(
                "⚠️ Tracker showed exposure but no log found — treating as first log.",
                verbose,
            )
            theme_total = 0.0
            delta_base = 0.0

    # Restrict early bets for low-liquidity segments (1st_3, 1st_7, team_totals)
    if segment in {"1st_3", "1st_7", "team_totals"} and hours_to_game is not None and hours_to_game > 12:
        try:
            from core.pending_bets import queue_pending_bet

            baseline = new_bet.get("consensus_prob") or new_bet.get("market_prob")
            queue_pending_bet({**new_bet, "baseline_consensus_prob": baseline})
        except Exception:
            pass
        if verbose:
            print(f"⏳ should_log_bet: Queued {segment} bet — too early (>12h to game)")
        new_bet["entry_type"] = "none"
        new_bet["skip_reason"] = "time_blocked"
        return build_skipped_evaluation("time_blocked", game_id, new_bet)

    rounded_delta = round_stake(delta)

    if entry_type == "first":
        new_bet["stake"] = round_stake(stake)
        if new_bet["stake"] < MIN_FIRST_STAKE:
            _log_verbose(
                f"⛔ Skipping bet — scaled stake {new_bet['stake']}u is below {MIN_FIRST_STAKE:.1f}u minimum",
                verbose,
            )
            new_bet["entry_type"] = "none"
            new_bet["skip_reason"] = SkipReason.LOW_INITIAL.value
            return build_skipped_evaluation(
                SkipReason.LOW_INITIAL.value, game_id, new_bet
            )
        _log_verbose(
            f"✅ should_log_bet: First bet → {side} | {theme_key} [{segment}] | Stake: {stake:.2f}u | EV: {ev:.2f}%",
            verbose,
        )
        for stale_key in [
            "skip",
            "log",
            "entry_type",
            "full_stake",
            "stake",
            "skip_reason",
        ]:
            new_bet.pop(stale_key, None)
        return {
            "skip": False,
            "log": True,
            "entry_type": "first",
            "stake": stake,
            "full_stake": stake,
            "ev": ev,
            "game_id": game_id,
            "side": new_bet["side"],
            **new_bet,
        }

    if entry_type == "top-up":
        new_bet["stake"] = rounded_delta
        _log_verbose(
            f"🔼 should_log_bet: Top-up accepted → {side} | {theme_key} [{segment}] | Δ {rounded_delta:.2f}u",
            verbose,
        )
        for stale_key in [
            "skip",
            "log",
            "entry_type",
            "full_stake",
            "stake",
            "skip_reason",
        ]:
            new_bet.pop(stale_key, None)
        return {
            "skip": False,
            "log": True,
            "entry_type": "top-up",
            "stake": rounded_delta,
            "full_stake": stake,
            "partial_stake": rounded_delta,
            "ev": ev,
            "game_id": game_id,
            "side": new_bet["side"],
            **new_bet,
        }

    # entry_type == "none"
    msg = f"⛔ Delta stake {rounded_delta:.2f}u < {MIN_TOPUP_STAKE:.1f}u minimum"
    new_bet["entry_type"] = "none"
    new_bet["skip_reason"] = skip_reason
    _log_verbose(msg, verbose)
    return build_skipped_evaluation(skip_reason, game_id, new_bet)
