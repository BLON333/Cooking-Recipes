import os
import sys
import logging
import json
from collections import defaultdict
from argparse import ArgumentParser

# Quiet logging from other modules
os.environ.setdefault("LOG_LEVEL", "WARNING")
logging.getLogger().setLevel("WARNING")

# Ensure repo root on path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from core.snapshot_tracker_loader import find_latest_market_snapshot_path
from core.utils import safe_load_json


def validate_snapshot_rows(rows):
    counts = {
        "missing_baseline": 0,
        "missing_roles": 0,
        "missing_logged_ts": 0,
        "missing_stake": 0,
        "bad_movement": 0,
    }
    offenders = []

    for row in rows:
        has_error = False
        if row.get("baseline_consensus_prob") is None:
            counts["missing_baseline"] += 1
            has_error = True
        if not isinstance(row.get("snapshot_roles"), list) or not row.get("snapshot_roles"):
            counts["missing_roles"] += 1
            has_error = True
        if row.get("logged") and not row.get("logged_ts"):
            counts["missing_logged_ts"] += 1
            has_error = True
        if (row.get("stake") is None) or (row.get("snapshot_stake") is None) or (row.get("raw_kelly") is None):
            counts["missing_stake"] += 1
            has_error = True
        movement = row.get("mkt_movement")
        if movement not in {"up", "down", "same"}:
            counts["bad_movement"] += 1
            has_error = True
        if not row.get("game_id") or not row.get("market") or not row.get("side"):
            has_error = True
        if (row.get("market_prob") is None) and (row.get("consensus_prob") is None):
            has_error = True
            counts.setdefault("missing_prob", 0)
            counts["missing_prob"] += 1
        if has_error:
            offenders.append(row)
    return counts, offenders


def summarize_roles(rows):
    summary = defaultdict(int)
    for row in rows:
        roles = row.get("snapshot_roles")
        if isinstance(roles, list):
            for r in roles:
                summary[r] += 1
    return summary


def run_snapshot_integrity_test(limit=5, show_summary=False):
    path = find_latest_market_snapshot_path()
    if not path:
        print("\u274c No snapshot files found.")
        return 1
    rows = safe_load_json(path)
    if not isinstance(rows, list):
        print(f"\u274c Snapshot file not a list: {path}")
        return 1

    counts, offenders = validate_snapshot_rows(rows)

    total_failures = sum(counts.values())
    print(f"\nSnapshot Integrity Report for {os.path.basename(path)}")
    print(f"Total rows checked: {len(rows)}")
    for k, v in counts.items():
        print(f"{k.replace('_', ' ').title()}: {v}")

    if show_summary:
        summary = summarize_roles(rows)
        if summary:
            print("\nSnapshot Role Summary:")
            for role, cnt in sorted(summary.items(), key=lambda x: (-x[1], x[0])):
                print(f"  {role}: {cnt}")

    if total_failures and offenders:
        print(f"\nFirst {min(limit, len(offenders))} offending rows:")
        for r in offenders[:limit]:
            short = {k: r.get(k) for k in ["game_id", "market", "side", "mkt_movement", "logged"]}
            print(json.dumps(short, indent=2))

    return 0 if total_failures == 0 else 1


if __name__ == "__main__":
    parser = ArgumentParser(description="Validate latest market snapshot file")
    parser.add_argument("--summary", action="store_true", help="show role summary")
    parser.add_argument("--limit", type=int, default=5, help="number of failing rows to display")
    args = parser.parse_args()
    exit_code = run_snapshot_integrity_test(limit=args.limit, show_summary=args.summary)
    sys.exit(exit_code)
