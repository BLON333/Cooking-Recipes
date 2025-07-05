import os
import json
from datetime import datetime
from core.snapshot_tracker_loader import (
    find_latest_market_snapshot_path,
    find_latest_snapshot_tracker_path,
)
from core.market_eval_tracker import build_tracker_key, load_tracker
from core.utils import safe_load_json, parse_snapshot_timestamp

BACKTEST_DIR = "backtest"
PENDING_BETS_PATH = os.path.join("logs", "pending_bets.json")


def main() -> None:
    # Find latest snapshot
    snapshot_path = find_latest_market_snapshot_path(BACKTEST_DIR)
    if not snapshot_path or not os.path.exists(snapshot_path):
        print("❌ No market snapshot found.")
        return
    print(f"📄 Using snapshot: {snapshot_path}")

    snapshot_data = safe_load_json(snapshot_path) or []
    if not isinstance(snapshot_data, list):
        print("❌ Snapshot file is not a list")
        return

    # Determine snapshot date from file name
    name = os.path.basename(snapshot_path)
    token = name.replace("market_snapshot_", "").split(".")[0]
    dt = parse_snapshot_timestamp(token)
    snapshot_date = dt.date() if isinstance(dt, datetime) else None

    tracker_path = find_latest_snapshot_tracker_path(snapshot_date)
    print(f"📄 Using tracker snapshot: {tracker_path}")
    tracker_data = safe_load_json(tracker_path) or {}
    if not isinstance(tracker_data, dict):
        tracker_data = load_tracker()

    pending_bets = safe_load_json(PENDING_BETS_PATH) or {}
    if not isinstance(pending_bets, dict):
        pending_bets = {}

    print("\nKey\tPending Baseline\tTracker Baseline\tCurrent Prob\tΔ vs Pending\tΔ vs Tracker")
    print("-" * 90)

    seen = set()

    for row in snapshot_data:
        game_id = row.get("game_id")
        market = row.get("market")
        side = row.get("side")
        market_prob = row.get("market_prob")
        if game_id is None or market is None or side is None:
            continue
        key = build_tracker_key(game_id, market, side)
        if key in seen:
            continue
        seen.add(key)

        pending_row = pending_bets.get(key) or {}
        pending_base = pending_row.get("baseline_consensus_prob")
        tracker_base = tracker_data.get(key, {}).get("market_prob")

        delta_pending = None
        if pending_base is not None and market_prob is not None:
            delta_pending = market_prob - pending_base

        delta_tracker = None
        if tracker_base is not None and market_prob is not None:
            delta_tracker = market_prob - tracker_base

        flag = ""
        if pending_base is not None and tracker_base is not None and abs(pending_base - tracker_base) > 1e-6:
            flag = "❌"

        print(
            f"{key}\t"
            f"{pending_base if pending_base is not None else 'N/A'}\t"
            f"{tracker_base if tracker_base is not None else 'N/A'}\t"
            f"{market_prob if market_prob is not None else 'N/A'}\t"
            f"{delta_pending if delta_pending is not None else 'N/A'}\t"
            f"{delta_tracker if delta_tracker is not None else 'N/A'} {flag}"
        )


if __name__ == "__main__":
    main()
