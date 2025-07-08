import os
import json
from datetime import datetime
from typing import Dict, Tuple

from core.utils import safe_load_json
from core.lock_utils import with_locked_file
from core.market_eval_tracker import build_tracker_key
from core.snapshot_tracker_loader import find_latest_market_snapshot_path

DEFAULT_DIR = "backtest"


def load_latest_snapshot_tracker(directory: str = DEFAULT_DIR) -> Tuple[Dict[str, dict], str | None]:
    """Return tracker dict built from the most recent snapshot file."""
    path = find_latest_market_snapshot_path(directory)
    if not path or not os.path.exists(path):
        return {}, None

    data = safe_load_json(path) or []
    tracker: Dict[str, dict] = {}
    rows = data if isinstance(data, list) else data.values() if isinstance(data, dict) else []
    for row in rows:
        key = build_tracker_key(row.get("game_id"), row.get("market"), row.get("side"))
        tracker[key] = row
    return tracker, path


def write_market_snapshot(tracker: Dict[str, dict], directory: str = DEFAULT_DIR) -> str:
    """Persist ``tracker`` as a new ``market_snapshot_*.json`` file."""
    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    path = os.path.join(directory, f"market_snapshot_{timestamp}.json")
    tmp = path + ".tmp"
    lock = path + ".lock"
    os.makedirs(directory, exist_ok=True)
    rows = list(tracker.values())
    with with_locked_file(lock):
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(rows, f, indent=2)
        os.replace(tmp, path)
    return path
