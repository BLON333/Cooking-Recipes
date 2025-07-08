import os
import json
from typing import Optional

from core.utils import safe_load_json, parse_game_id
from core.snapshot_tracker_loader import (
    find_latest_market_snapshot_path,
    find_latest_snapshot_tracker_path,
)
from core.snapshot_core import load_snapshot_tracker as load_tracker_from_core
from core.confirmation_utils import required_market_move, extract_book_count
from core.should_log_bet import should_log_bet

PENDING_BETS_PATH = os.path.join('logs', 'pending_bets.json')
BACKTEST_DIR = 'backtest'
TRACKER_DIR = os.path.join('data', 'trackers')

TARGET_GAME_ID = '2025-07-05-STL@CHC-T1420'
TARGET_MARKET = 'totals_1st_5_innings'
TARGET_SIDE = 'Under 4.5'


def load_pending_bet(game_id: str, market: str, side: str) -> Optional[dict]:
    data = safe_load_json(PENDING_BETS_PATH) or {}
    key = f"{game_id}:{market}:{side}"
    return data.get(key)


def load_snapshot_row(path: str, game_id: str, market: str, side: str) -> Optional[dict]:
    rows = safe_load_json(path) or []
    for row in rows:
        if (
            str(row.get('game_id')) == game_id
            and str(row.get('market')) == market
            and str(row.get('side')) == side
        ):
            return row
    return None


def load_snapshot_tracker(game_date: str) -> dict:
    path = find_latest_snapshot_tracker_path(game_date)
    if path and os.path.exists(path):
        tracker = safe_load_json(path)
        if isinstance(tracker, dict):
            return tracker
    try:
        return load_tracker_from_core()
    except Exception:
        return {}


def main() -> None:
    pending = load_pending_bet(TARGET_GAME_ID, TARGET_MARKET, TARGET_SIDE)
    if not pending:
        print('❌ Pending bet not found.')
        return

    snapshot_path = find_latest_market_snapshot_path(BACKTEST_DIR)
    if not snapshot_path:
        print('❌ No market snapshot file found.')
        return

    row = load_snapshot_row(snapshot_path, TARGET_GAME_ID, TARGET_MARKET, TARGET_SIDE)
    if not row:
        print('❌ Snapshot row not found for target bet.')
        return

    game_date = parse_game_id(TARGET_GAME_ID).get('date')
    tracker = load_snapshot_tracker(game_date)

    result = should_log_bet(row.copy(), {}, eval_tracker=tracker, reference_tracker=tracker)

    market_prob = row.get('market_prob') or row.get('consensus_prob')
    baseline = pending.get('baseline_consensus_prob') or row.get('baseline_consensus_prob')
    move = None
    if baseline is not None and market_prob is not None:
        move = market_prob - baseline

    hours = float(row.get('hours_to_game', 0) or 0)
    book_count = extract_book_count(row)
    required_move = required_market_move(hours, book_count=book_count, market=row.get('market'), ev_percent=row.get('ev_percent'))

    print('--- Debug should_log_bet ---')
    print(f"EV%: {row.get('ev_percent')}")
    print(f"Stake: {row.get('stake')}")
    if move is not None:
        print(f"Baseline vs market_prob: {baseline} -> {market_prob} (Δ {move:+.5f})")
    else:
        print('Baseline or market_prob missing.')
    print(f"Confirmation threshold delta: {(move - required_move):+.5f}" if move is not None else f"Required move: {required_move:.5f}")
    print(f"Movement direction: {row.get('mkt_movement')}")
    verdict = 'LOG' if result.get('log') else 'SKIP'
    reason = result.get('reason') or result.get('skip_reason')
    print(f"Logging verdict: {verdict} ({reason})")


if __name__ == '__main__':
    main()
