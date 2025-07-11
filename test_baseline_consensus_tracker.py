import os
import json
from core.snapshot_tracker_loader import find_latest_market_snapshot_path
from core.utils import safe_load_json, safe_load_dict, build_snapshot_key

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

    pending_bets = safe_load_dict(PENDING_BETS_PATH)
    if not isinstance(pending_bets, dict):
        pending_bets = {}

    print("\nKey\tPending Baseline\tSnapshot Baseline\tCurrent Prob\tΔ vs Pending")
    print("-" * 90)

    seen = set()

    for row in snapshot_data:
        game_id = row.get("game_id")
        market = row.get("market")
        side = row.get("side")
        market_prob = row.get("market_prob")
        if game_id is None or market is None or side is None:
            continue
        key = build_snapshot_key(game_id, market, side)
        if key in seen:
            continue
        seen.add(key)

        pending_row = pending_bets.get(key) or {}
        pending_base = pending_row.get("baseline_consensus_prob")
        snapshot_base = row.get("baseline_consensus_prob")

        delta_pending = None
        if pending_base is not None and snapshot_base is not None:
            delta_pending = snapshot_base - pending_base

        flag = ""
        if pending_base is not None and snapshot_base is not None and abs(pending_base - snapshot_base) > 1e-6:
            flag = "❌"

        print(
            f"{key}\t"
            f"{pending_base if pending_base is not None else 'N/A'}\t"
            f"{snapshot_base if snapshot_base is not None else 'N/A'}\t"
            f"{market_prob if market_prob is not None else 'N/A'}\t"
            f"{delta_pending if delta_pending is not None else 'N/A'} {flag}"
        )


if __name__ == "__main__":
    main()
