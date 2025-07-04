#!/usr/bin/env python
"""Generate pending_bets.json from latest snapshot file."""

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import json
import glob

from core.market_eval_tracker import build_tracker_key
from core.utils import safe_load_json
from core.snapshot_core import _assign_snapshot_role
from core.market_normalizer import normalize_market_key
from cli.log_betting_evals import load_market_conf_tracker

SNAPSHOT_DIR = os.path.join("backtest")
PENDING_JSON = os.path.join("logs", "pending_bets.json")


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
            ev = float(row.get("ev_percent", 0))
            rk = float(row.get("raw_kelly", 0))
        except Exception:
            continue
        try:
            hours = float(row.get("hours_to_game", 0))
            if logged and hours < 0:
                continue  # drop logged bets for past games
        except Exception:
            pass
        # Allow logged bets so they remain in pending and stay refreshed
        if ev < 5.0 or rk < 1.0:
            continue  # Filter weak edges only
        filtered.append(row)
    return filtered


def build_pending(rows: list, tracker: dict) -> dict:
    """Convert ``rows`` into pending bet entries keyed by tracker key."""
    pending: dict = {}
    for row in rows:
        key = build_tracker_key(row.get("game_id"), row.get("market"), row.get("side"))
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
            "skip_reason": row.get("skip_reason"),
            "logged": row.get("logged", False),
        }
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
        if key in tracker and isinstance(tracker[key], dict):
            cp = tracker[key].get("consensus_prob")
            if cp is not None:
                entry["baseline_consensus_prob"] = cp
        pending[key] = entry
    return pending


def save_pending_bets(pending: dict, path: str = PENDING_JSON) -> None:
    """Atomically write ``pending`` to ``path``."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w") as f:
        json.dump(pending, f, indent=2)
    os.replace(tmp, path)


def main() -> None:
    rows, snap = load_latest_snapshot()
    if snap is None or not rows:
        print("❌ No snapshot data found")
        return

    tracker = load_market_conf_tracker()
    filtered = filter_rows(rows)
    pending = build_pending(filtered, tracker)
    save_pending_bets(pending, PENDING_JSON)

    print(f"✅ Saved {len(pending)} entries from {os.path.basename(snap)}")


if __name__ == "__main__":
    main()