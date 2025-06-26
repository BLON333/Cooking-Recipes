#!/usr/bin/env python
"""Move quiet-hour queued bets into pending_bets.json."""

import os
import sys
import argparse

from core.bootstrap import *  # noqa
from core.utils import safe_load_json
from core.pending_bets import queue_pending_bet

DEFAULT_QUIET_JSON = os.path.join("logs", "pending_quiet_logs.json")
DEFAULT_PENDING_JSON = os.path.join("logs", "pending_bets.json")


def move_bets(quiet_path: str = DEFAULT_QUIET_JSON, pending_path: str = DEFAULT_PENDING_JSON) -> int:
    """Move all bets from ``quiet_path`` into ``pending_path``."""
    if not os.path.exists(quiet_path):
        print(f"❌ No quiet-hour file found at: {quiet_path}")
        return 0

    data = safe_load_json(quiet_path)
    if not data:
        print(f"❌ No bets loaded from {quiet_path}")
        return 0

    if isinstance(data, dict):
        bets = list(data.values())
    elif isinstance(data, list):
        bets = data
    else:
        print(f"❌ Unexpected format in {quiet_path} (expected dict or list)")
        return 0

    moved = 0
    for bet in bets:
        queue_pending_bet(bet, path=pending_path)
        moved += 1

    print(f"✅ Moved {moved} bets to {pending_path}")
    return moved


def main() -> None:
    parser = argparse.ArgumentParser(description="Move quiet-hour queued bets back into pending_bets.json")
    parser.add_argument("--quiet-json", default=DEFAULT_QUIET_JSON, help="Path to pending_quiet_logs.json")
    parser.add_argument("--pending-json", default=DEFAULT_PENDING_JSON, help="Path to pending_bets.json")
    args = parser.parse_args()

    moved = move_bets(args.quiet_json, args.pending_json)
    if not moved:
        sys.exit(1)


if __name__ == "__main__":
    main()

