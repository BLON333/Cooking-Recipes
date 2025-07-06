#!/usr/bin/env python
"""Generate pending_bets.json from latest snapshot file."""

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import glob

from core.market_eval_tracker import build_tracker_key
from core.utils import safe_load_json
from core.snapshot_core import _assign_snapshot_role, ensure_baseline_consensus_prob
from core.market_normalizer import normalize_market_key
from core.pending_bets import (
    infer_market_class,
    load_pending_bets,
    save_pending_bets,
)
from cli.log_betting_evals import load_market_conf_tracker

SNAPSHOT_DIR = os.path.join("backtest")
PENDING_JSON = os.path.join("logs", "pending_bets.json")

# Load current pending bets to merge with snapshot rows
existing = load_pending_bets(PENDING_JSON)


def load_latest_snapshot(directory: str = SNAPSHOT_DIR) -> tuple[list, str | None]:
    """Return rows from the most recent ``snapshot_*.json`` in ``directory``."""
    pattern = os.path.join(directory, "market_snapshot_*.json")
    files = sorted(glob.glob(pattern))
    if not files:
        return [], None
    latest = max(files, key=os.path.getmtime)
    data = safe_load_json(latest)
    if isinstance(data, list):
        rows = data
    elif isinstance(data, dict):
        rows = list(data.values())
    else:
        rows = []
    return rows, latest


def filter_rows(rows: list) -> list:
    """Return rows meeting the EV/Kelly/logged criteria."""
    filtered = []
    for row in rows:
        try:
            logged = bool(row.get("logged"))
            ev = float(row.get("ev_percent", 0) or 0)
            rk = float(row.get("raw_kelly", 0) or 0)
            stake = float(row.get("stake", row.get("full_stake", 0)) or 0)
        except Exception:
            continue

        try:
            hours = float(row.get("hours_to_game", 0) or 0)
            if logged and hours < 0:
                continue  # drop logged bets for past games
        except Exception:
            pass

        # Skip only if all metrics fail their thresholds
        if ev < 5.0 and rk < 1.0 and stake < 1.0:
            continue  # Filter weak edges only

        filtered.append(row)

    return filtered


def build_pending(rows: list, tracker: dict) -> dict:
    """Convert ``rows`` into pending bet entries keyed by tracker key."""
    pending: dict = {}
    for row in rows:
        key = build_tracker_key(row.get("game_id"), row.get("market"), row.get("side"))

        baseline = None
        if key in existing and isinstance(existing[key], dict):
            baseline = existing[key].get("baseline_consensus_prob")

        if baseline is None and "baseline_consensus_prob" in row:
            baseline = row["baseline_consensus_prob"]

        if baseline is None and key in tracker and isinstance(tracker[key], dict):
            baseline = tracker[key].get("consensus_prob")

        if baseline is None:
            baseline = row.get("consensus_prob") or row.get("market_prob")

        if baseline is not None:
            row["baseline_consensus_prob"] = baseline

        entry = {
            "game_id": row.get("game_id"),
            "market": row.get("market"),
            "side": row.get("side"),
            "ev_percent": row.get("ev_percent"),
            "raw_kelly": row.get("raw_kelly"),
            "market_odds": row.get("market_odds"),
            "sim_prob": row.get("sim_prob"),
            "market_prob": row.get("market_prob"),
            "blended_fv": row.get("blended_fv"),
            "book": row.get("book") or row.get("best_book"),
            "date_simulated": row.get("date_simulated"),
            "logged": row.get("logged", False),
            "last_skip_reason": row.get("skip_reason"),
        }
        entry["market_group"] = infer_market_class(entry.get("market"))
        if "market_class" not in entry:
            meta = normalize_market_key(entry.get("market", ""))
            entry["market_class"] = meta.get("market_class", "main")
        role = _assign_snapshot_role(entry)
        entry["snapshot_role"] = role
        roles = []
        if isinstance(row.get("snapshot_roles"), list):
            roles.extend(row["snapshot_roles"])
        if role not in roles:
            roles.append(role)
        if "best_book" not in roles:
            roles.append("best_book")
        entry["snapshot_roles"] = roles
        if "baseline_consensus_prob" in row and row["baseline_consensus_prob"] is not None:
            entry["baseline_consensus_prob"] = row["baseline_consensus_prob"]
        pending[key] = entry
    return pending


def main() -> None:
    rows, snap = load_latest_snapshot()
    if snap is None or not rows:
        print("❌ No snapshot data found")
        return

    tracker = load_market_conf_tracker()
    filtered = filter_rows(rows)
    ensure_baseline_consensus_prob(filtered)
    new_rows = build_pending(filtered, tracker)

    for key, row in new_rows.items():
        existing_row = existing.get(key, {})
        preserve_fields = ["queued_ts", "logged", "entry_type"]
        for field in preserve_fields:
            if field in existing_row and field not in row:
                row[field] = existing_row[field]

        row.setdefault(
            "movement_confirmed", existing_row.get("movement_confirmed", False)
        )
        row.setdefault(
            "visible_in_snapshot", existing_row.get("visible_in_snapshot", True)
        )
        if "last_skip_reason" not in row or row["last_skip_reason"] is None:
            if "last_skip_reason" in existing_row:
                row["last_skip_reason"] = existing_row["last_skip_reason"]
        existing[key] = row

    save_pending_bets(existing, PENDING_JSON)

    missing_baseline = [k for k, v in existing.items() if v.get("baseline_consensus_prob") is None]
    if missing_baseline:
        print(f"⚠️ {len(missing_baseline)} bets missing baseline_consensus_prob")
        for k in missing_baseline[:5]:
            print(f"  - {k}")
    else:
        print("✅ All pending bets include baseline_consensus_prob.")

    print(f"✅ Saved {len(existing)} entries from {os.path.basename(snap)}")


if __name__ == "__main__":
    main()
