import os, glob
from datetime import date, datetime
from core.market_eval_tracker import TRACKER_PATH

SNAPSHOT_DIR = os.path.join('data', 'trackers')


def find_latest_snapshot_tracker_path(game_date: date | str, directory: str = SNAPSHOT_DIR) -> str:
    """Return path to the snapshot tracker closest to ``game_date``.

    Files are expected to follow a ``market_eval_tracker_*YYYY*`` pattern
    inside ``directory``.  If no dated snapshot is found, fall back to the
    default tracker path.
    """
    if isinstance(game_date, str):
        try:
            game_date = datetime.strptime(game_date, "%Y-%m-%d").date()
        except Exception:
            pass
    date_str = getattr(game_date, "strftime", lambda f: str(game_date))("%Y-%m-%d")

    patterns = [
        os.path.join(directory, f"market_eval_tracker_{date_str}*.json"),
        os.path.join(directory, f"market_eval_tracker_snapshot_{date_str}*.json"),
    ]

    for pat in patterns:
        files = glob.glob(pat)
        if files:
            return max(files, key=os.path.getmtime)

    # Generic fallback to any dated snapshot
    generic = glob.glob(os.path.join(directory, "market_eval_tracker_*.json"))
    if generic:
        return max(generic, key=os.path.getmtime)

    # Final fallback to the live tracker
    return TRACKER_PATH
