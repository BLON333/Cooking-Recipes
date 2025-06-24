import os
from collections import defaultdict
from datetime import datetime, timedelta

from core.bootstrap import *  # noqa

from cli.log_betting_evals import (
    load_quiet_log_queue,
    save_quiet_log_queue,
    deduplicate_pending_quiet_logs,
    write_to_csv,
    load_existing_stakes,
    record_successful_log,
    send_discord_notification,
)
from core.theme_exposure_tracker import load_tracker as load_theme_stakes, save_tracker as save_theme_stakes
from core.market_eval_tracker import load_tracker as load_eval_tracker
from core.dispatch_clv_snapshot import (
    parse_start_time,
    lookup_consensus_prob,
    latest_odds_file,
    load_odds,
)
from core.time_utils import compute_hours_to_game
from core.market_pricer import kelly_fraction, to_american_odds
from core.odds_fetcher import prob_to_american
from core.skip_reasons import SkipReason
from core.should_log_bet import MAX_POSITIVE_ODDS, MIN_NEGATIVE_ODDS


def get_latest_odds() -> dict:
    """Load the most recent market odds JSON if available."""
    path = latest_odds_file()
    if path and os.path.exists(path):
        return load_odds(path)
    return {}


def reprocess_pending_quiet_logs(
    odds_data: dict | None = None,
    min_ev: float = 0.05,
    max_ev: float = 0.20,
    kelly_threshold: float = 1.0,
) -> int:
    """Revalidate and log bets queued during quiet hours."""
    deduplicate_pending_quiet_logs()
    queue = load_quiet_log_queue()
    if not queue:
        return 0

    odds = odds_data or get_latest_odds()
    if not odds:
        return 0

    existing = load_existing_stakes("logs/market_evals.csv")
    session_exposure = defaultdict(set)
    theme_stakes = load_theme_stakes()
    load_eval_tracker()  # ensure tracker file exists

    logged = 0
    for bet in queue:
        gid = bet.get("game_id")
        if not gid:
            continue

        odds_game = odds.get(gid) or odds.get(gid.split("-T")[0])
        start_dt = parse_start_time(gid, odds_game)
        if not start_dt or compute_hours_to_game(start_dt) <= 0:
            bet["skip_reason"] = SkipReason.EXPIRED_POST_QUIET_HOURS.value
            continue
        if not odds_game:
            bet["skip_reason"] = SkipReason.EXPIRED_POST_QUIET_HOURS.value
            continue

        market_prob = lookup_consensus_prob(odds_game, bet["market"], bet["side"])
        if market_prob is None:
            bet["skip_reason"] = SkipReason.EXPIRED_POST_QUIET_HOURS.value
            continue

        market_odds = prob_to_american(market_prob)
        try:
            market_odds_val = float(market_odds)
        except Exception:
            bet["skip_reason"] = SkipReason.EXPIRED_POST_QUIET_HOURS.value
            continue

        try:
            sim_prob = float(bet.get("blended_prob") or bet.get("sim_prob") or 0.0)
        except Exception:
            sim_prob = 0.0

        fair_odds = to_american_odds(sim_prob)
        ev_percent = round((sim_prob - market_prob) * 100, 2)
        fraction = 0.125 if str(bet.get("market_class", "")).startswith("alt") else 0.25
        raw_kelly = kelly_fraction(sim_prob, market_odds_val, fraction=fraction)

        if not (min_ev * 100 <= ev_percent <= max_ev * 100):
            bet["skip_reason"] = SkipReason.EXPIRED_POST_QUIET_HOURS.value
            continue
        if raw_kelly < kelly_threshold:
            bet["skip_reason"] = SkipReason.EXPIRED_POST_QUIET_HOURS.value
            continue
        if market_odds_val < MIN_NEGATIVE_ODDS or market_odds_val > MAX_POSITIVE_ODDS:
            bet["skip_reason"] = SkipReason.EXPIRED_POST_QUIET_HOURS.value
            continue

        updated = bet.copy()
        updated.update(
            {
                "market_odds": market_odds_val,
                "market_prob": market_prob,
                "consensus_prob": market_prob,
                "fair_odds": fair_odds,
                "ev_percent": ev_percent,
                "raw_kelly": raw_kelly,
                "stake": round(raw_kelly, 2),
                "hours_to_game": compute_hours_to_game(start_dt),
            }
        )

        result = write_to_csv(
            updated,
            "logs/market_evals.csv",
            existing,
            session_exposure,
            theme_stakes,
            force_log=True,
        )
        if result and not result.get("skip_reason"):
            record_successful_log(result, existing, theme_stakes)
            send_discord_notification(result)
            logged += 1
        else:
            bet["skip_reason"] = SkipReason.EXPIRED_POST_QUIET_HOURS.value

    save_quiet_log_queue([])
    if logged:
        save_theme_stakes(theme_stakes)
    return logged


if __name__ == "__main__":
    count = reprocess_pending_quiet_logs()
    print(f"Processed {count} pending quiet logs")