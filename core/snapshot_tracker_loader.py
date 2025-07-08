import os, glob
from datetime import date, datetime

# Default location for historical market tracker snapshots
# (used only as a fallback when no dated snapshot exists)
DEFAULT_TRACKER_PATH = os.path.join("data", "trackers", "market_snapshot_tracker.json")

SNAPSHOT_DIR = os.path.join('data', 'trackers')


def find_latest_snapshot_tracker_path(game_date: date | str, directory: str = SNAPSHOT_DIR) -> str:
    """Return path to the snapshot tracker closest to ``game_date``.

    Files are expected to follow a ``market_snapshot_tracker_*YYYY*`` pattern
    inside ``directory``. If no dated snapshot is found, fall back to the
    default tracker path.
    """
    if isinstance(game_date, str):
        try:
            game_date = datetime.strptime(game_date, "%Y-%m-%d").date()
        except Exception:
            pass
    date_str = getattr(game_date, "strftime", lambda f: str(game_date))("%Y-%m-%d")

    patterns = [
        os.path.join(directory, f"market_snapshot_tracker_{date_str}*.json"),
        os.path.join(directory, f"market_snapshot_{date_str}*.json"),
    ]

    for pat in patterns:
        files = glob.glob(pat)
        if files:
            return max(files, key=os.path.getmtime)

    # Generic fallback to any dated snapshot
    generic = glob.glob(os.path.join(directory, "market_snapshot_tracker_*.json"))
    if generic:
        return max(generic, key=os.path.getmtime)

    # Final fallback to the live tracker path
    return DEFAULT_TRACKER_PATH


def find_latest_market_snapshot_path(backtest_dir: str = "backtest") -> str | None:
    """Return the most recently modified market snapshot file.

    Parameters
    ----------
    backtest_dir : str, optional
        Directory containing ``market_snapshot_*.json`` files.

    Returns
    -------
    str | None
        Path to the latest snapshot file or ``None`` if none found.
    """

    pattern = os.path.join(backtest_dir, "market_snapshot_*.json")
    matches = glob.glob(pattern)
    if not matches:
        return None
    return max(matches, key=os.path.getmtime)

