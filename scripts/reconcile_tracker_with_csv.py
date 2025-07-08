#!/usr/bin/env python3
"""Report phantom entries in the latest snapshot tracker based on market_evals.csv."""

import csv
import json
import os
from datetime import datetime

from core.market_snapshot_tracker import load_latest_snapshot_tracker

CSV_PATH = os.path.join("logs", "market_evals.csv")


def parse_tracker_key(key: str):
    """Return ``(game_id, market, side)`` from a tracker key."""
    parts = key.split(":")
    if len(parts) >= 3:
        return parts[0], parts[1], parts[2]
    return None, None, None


def load_csv_keys(csv_path: str) -> set[tuple[str, str, str]]:
    """Load ``market_evals.csv`` and return a set of triples."""
    entries = set()
    if not os.path.exists(csv_path):
        print(f"❌ CSV not found: {csv_path}")
        return entries

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            gid = row.get("game_id")
            market = row.get("market")
            side = row.get("side")
            if gid and market and side:
                entries.add((gid.strip(), market.strip(), side.strip()))
    return entries




def reconcile(csv_path: str = CSV_PATH) -> None:
    csv_keys = load_csv_keys(csv_path)
    tracker, _ = load_latest_snapshot_tracker()
    if not tracker:
        print("❌ No snapshot tracker found")
        return

    original_count = len(tracker)
    removed_keys: list[str] = []
    for key in list(tracker.keys()):
        parsed = parse_tracker_key(key)
        if parsed[0] is None:
            continue
        if parsed not in csv_keys:
            removed_keys.append(key)
            del tracker[key]

    removed_count = len(removed_keys)
    cleaned_tracker = tracker
    
    print("✅ Reconciliation Complete")
    print(f"🔢 Tracker entries removed: {removed_count}")
    print(f"📊 Final tracker size: {len(cleaned_tracker)}")

    print(f"Total entries in market_eval_tracker before: {original_count}")
    print(f"Total entries after: {len(cleaned_tracker)}")
    print(f"Total logged bets in market_evals.csv: {len(csv_keys)}")
    print(f"Number of phantom tracker entries removed: {removed_count}")
    if removed_keys:
        print("Top 10 removed keys:")
        for key in removed_keys[:10]:
            print(f"  - {key}")

    # Confirm remaining keys exist in CSV
    missing = [k for k in cleaned_tracker if parse_tracker_key(k) not in csv_keys]
    if missing:
        print(f"⚠️ {len(missing)} remaining keys not found in CSV")
    else:
        print("✅ All remaining keys verified against CSV")


if __name__ == "__main__":
    reconcile()