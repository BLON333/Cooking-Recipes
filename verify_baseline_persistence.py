import os
import json
from typing import Any, Dict

from core.market_eval_tracker import build_tracker_key
from core.utils import safe_load_json
from core.snapshot_tracker_loader import find_latest_market_snapshot_path

BACKTEST_DIR = "backtest"
TRACKER_FILE = os.path.join("data", "trackers", "market_eval_tracker.json")

def load_json(path: str) -> Any:
    return safe_load_json(path)

def main() -> None:
    snapshot_path = find_latest_market_snapshot_path(BACKTEST_DIR)
    if not snapshot_path or not os.path.exists(snapshot_path):
        print("\u274c No market snapshot found.")
        return

    tracker: Dict[str, Any] = load_json(TRACKER_FILE) or {}
    snapshot = load_json(snapshot_path) or []

    if not isinstance(snapshot, list):
        print("\u274c Snapshot file is not a list")
        return
    if not isinstance(tracker, dict):
        print("\u274c Tracker file is not a dict")
        return

    header = [
        "game_id",
        "market",
        "side",
        "snapshot_baseline",
        "tracker_baseline",
        "market_prob",
        "delta",
        "status",
    ]
    print("\t".join(header))

    passed = 0
    failed = 0

    for row in snapshot:
        if not isinstance(row, dict):
            continue
        base = row.get("baseline_consensus_prob")
        if base is None:
            continue

        gid = row.get("game_id")
        market = row.get("market")
        side = row.get("side")
        if gid is None or market is None or side is None:
            continue

        key = build_tracker_key(str(gid), str(market), str(side))
        tracker_base = tracker.get(key, {}).get("baseline_consensus_prob")

        mkt_prob = row.get("market_prob")
        delta = mkt_prob - base if mkt_prob is not None else None

        status = "✅" if tracker_base is not None and abs(tracker_base - base) < 1e-6 else "❌"
        if status == "✅":
            passed += 1
        else:
            failed += 1

        print(
            f"{gid}\t{market}\t{side}\t{base}\t"
            f"{tracker_base if tracker_base is not None else 'N/A'}\t"
            f"{mkt_prob if mkt_prob is not None else 'N/A'}\t"
            f"{delta if delta is not None else 'N/A'}\t"
            f"{status}"
        )

    total = passed + failed
    print(f"\nRows checked: {total}")
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")

if __name__ == "__main__":
    main()
