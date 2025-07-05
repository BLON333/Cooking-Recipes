import os
import json
from datetime import datetime, timedelta
from typing import Dict

from core.utils import safe_load_json
from core.lock_utils import with_locked_file
from core.logger import get_logger

TRACKER_PATH = os.path.join("data", "trackers", "market_conf_tracker.json")

logger = get_logger(__name__)


def load_tracker(path: str = TRACKER_PATH) -> Dict[str, dict]:
    """Load the market confirmation tracker from ``path``."""
    data = safe_load_json(path)
    if isinstance(data, dict):
        return data
    if os.path.exists(path):
        print(f"⚠️ Could not load market confirmation tracker at {path}, starting fresh.")
    return {}


def save_tracker(tracker: Dict[str, dict], path: str = TRACKER_PATH) -> None:
    """Persist ``tracker`` to ``path`` atomically using a lock."""
    lock = f"{path}.lock"
    tmp = f"{path}.tmp"
    os.makedirs(os.path.dirname(path), exist_ok=True)
    try:
        with with_locked_file(lock):
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(tracker, f, indent=2)
            os.replace(tmp, path)
    except Exception as e:  # pragma: no cover - unexpected save failure
        logger.warning("❌ Failed to save market confirmation tracker: %s", e)


def clean_stale_tracker_entries(path: str = TRACKER_PATH, max_age_days: int | None = None) -> int:
    """Remove stale entries from the market confirmation tracker.

    Parameters
    ----------
    path : str
        Location of the tracker file.
    max_age_days : int | None, optional
        Entries with a ``timestamp`` older than ``max_age_days`` are removed if
        this parameter is provided.

    Returns
    -------
    int
        Total number of entries removed.
    """
    tracker = load_tracker(path)
    if not tracker:
        return 0

    cutoff = None
    if isinstance(max_age_days, (int, float)) and max_age_days > 0:
        cutoff = datetime.utcnow() - timedelta(days=max_age_days)

    removed_missing = 0
    removed_old = 0
    keys_to_delete: list[str] = []

    for key, entry in list(tracker.items()):
        if not isinstance(entry, dict):
            keys_to_delete.append(key)
            removed_missing += 1
            continue

        if entry.get("consensus_prob") is None:
            keys_to_delete.append(key)
            removed_missing += 1
            continue

        if cutoff is not None:
            ts = entry.get("timestamp")
            if ts:
                try:
                    dt = datetime.fromisoformat(str(ts))
                except Exception:
                    dt = None
                if dt and dt < cutoff:
                    keys_to_delete.append(key)
                    removed_old += 1

    for k in keys_to_delete:
        tracker.pop(k, None)

    if keys_to_delete:
        save_tracker(tracker, path)

    if removed_missing:
        print(f"🧹 Removed {removed_missing} stale tracker entries (missing consensus_prob)")
    if removed_old:
        print(f"🧹 Removed {removed_old} stale tracker entries (old timestamp)")

    return removed_missing + removed_old
