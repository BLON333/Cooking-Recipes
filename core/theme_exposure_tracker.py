# DEPRECATED: theme_exposure.json is no longer used in the exposure evaluation pipeline.
# All exposure tracking is now based on market_evals.csv + session memory.

import os
import json
import ast
from typing import Dict

from core.theme_key_utils import make_theme_key, parse_theme_key
from core.theme_utils import get_theme, get_theme_key, normalize_segment

from core.file_utils import with_locked_file

# Default location for persistent theme exposure tracking
# Align with other trackers under ``data/trackers``
TRACKER_PATH = os.path.join("data", "trackers", "theme_exposure.json")


def load_tracker(path: str = TRACKER_PATH) -> Dict[str, float]:
    """Load theme exposure tracker from ``path``.

    Handles both legacy tuple-string keys and the new "``game::theme::segment``"
    format.
    """
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return {}
        stakes: Dict[str, float] = {}
        for k, v in data.items():
            if isinstance(k, str):
                if "::" in k:
                    gid, theme, seg = parse_theme_key(k)
                    norm_key = make_theme_key(gid, theme, seg)
                    stakes[norm_key] = float(v)
                    continue
                try:
                    key = ast.literal_eval(k)
                except Exception:
                    key = None
                if isinstance(key, (list, tuple)) and len(key) == 3:
                    stakes[make_theme_key(str(key[0]), str(key[1]), str(key[2]))] = float(
                        v
                    )
        return stakes
    except Exception:
        return {}


def save_tracker(stakes: Dict[str, float], path: str = TRACKER_PATH) -> None:
    """Atomically persist theme exposure tracker to ``path``."""
    serializable = {k: v for k, v in stakes.items()}
    lock = f"{path}.lock"
    tmp = f"{path}.tmp"
    os.makedirs(os.path.dirname(path), exist_ok=True)
    try:
        with with_locked_file(lock):
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(serializable, f, indent=2)
            os.replace(tmp, path)
    except Exception as e:
        print(f"⚠️ Failed to save theme exposure tracker: {e}")


def build_theme_key(row: dict) -> str:
    """Return ``game::theme::segment`` key for ``row``."""
    game_id = str(row.get("game_id", ""))
    theme_key = row.get("theme_key")
    market = row.get("market", "")
    side = row.get("side", "")
    segment = row.get("segment")

    if not theme_key:
        theme = get_theme({"side": side, "market": market})
        theme_key = get_theme_key(market, theme)
    if not segment:
        segment = normalize_segment(market)

    return make_theme_key(game_id, str(theme_key), str(segment))
