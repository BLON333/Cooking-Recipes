#!/usr/bin/env python3
"""Delta Movement Verifier

Check ``baseline_consensus_prob`` against ``market_prob`` in a snapshot file
and compare to the latest snapshot tracker.
"""

import argparse
import json
import os
from typing import Any, Dict, Optional

from core.snapshot_core import build_key
from core.market_snapshot_tracker import load_latest_snapshot_tracker

TOLERANCE = 1e-4


def load_json(path: str) -> Optional[Any]:
    """Return parsed JSON from ``path`` or ``None``."""
    if not os.path.exists(path):
        print(f"❌ File not found: {path}")
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"❌ Failed to read {path}: {e}")
        return None


def verify(snapshot_file: str, *, threshold: Optional[float] = None) -> None:
    snapshot = load_json(snapshot_file)
    tracker, tracker_path = load_latest_snapshot_tracker()
    if not isinstance(snapshot, list):
        print("❌ Snapshot must be a list of dictionaries")
        return
    if not tracker:
        print("❌ No snapshot tracker available")
        return

    header = [
        "game_id",
        "market",
        "side",
        "baseline_snapshot",
        "baseline_tracker",
        "market_prob_snapshot",
        "market_prob_tracker",
        "delta_snapshot",
        "delta_tracker",
        "status",
    ]
    print("\t".join(header))

    for row in snapshot:
        if not isinstance(row, dict):
            continue
        base = row.get("baseline_consensus_prob")
        curr = row.get("market_prob")
        if base is None or curr is None:
            continue
        gid = row.get("game_id")
        market = row.get("market")
        side = row.get("side")
        if not gid or not market or not side:
            continue
        delta = curr - base
        if threshold is not None and abs(delta) < threshold:
            continue
        key = build_key(str(gid), str(market), str(side))
        entry: Dict[str, Any] = tracker.get(key, {}) if isinstance(tracker, dict) else {}
        t_base = entry.get("baseline_consensus_prob")
        t_prob = entry.get("market_prob")
        t_delta = t_prob - t_base if t_base is not None and t_prob is not None else None

        status = "✅"
        if t_base is None or abs((t_base or 0) - base) > TOLERANCE:
            status = "❌"
        if t_prob is None or abs((t_prob or 0) - curr) > TOLERANCE:
            status = "❌"
        if t_delta is None or abs(t_delta - delta) > TOLERANCE:
            status = "❌"

        print(
            f"{gid}\t{market}\t{side}\t{base}\t{t_base}\t{curr}\t{t_prob}\t{delta:.6f}\t"
            f"{t_delta if t_delta is not None else 'N/A'}\t{status}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify baseline consensus movement against tracker")
    parser.add_argument("snapshot_file", help="Path to market_snapshot JSON file")
    parser.add_argument(
        "--threshold",
        type=float,
        default=None,
        help="Filter rows with |delta| less than this value",
    )

    args = parser.parse_args()
    verify(args.snapshot_file, threshold=args.threshold)


if __name__ == "__main__":
    main()
