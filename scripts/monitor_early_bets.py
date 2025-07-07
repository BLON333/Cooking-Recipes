import os
import json
import time
from collections import defaultdict
from datetime import datetime

from core.bootstrap import *  # noqa
from core.logger import get_logger
from core.snapshot_tracker_loader import find_latest_market_snapshot_path
from core.utils import safe_load_json
from core.lock_utils import with_locked_file
from cli.log_betting_evals import (
    load_existing_stakes,
    build_theme_exposure_tracker,
)
from core.shared_logging_logic import evaluate_snapshot_row_for_logging
from core.market_eval_tracker import load_tracker as load_eval_tracker

logger = get_logger(__name__)

CHECK_INTERVAL = 30 * 60  # 30 minutes
DEFAULT_BACKTEST_DIR = os.path.join(os.path.dirname(__file__), "..", "backtest")


def _load_snapshot(backtest_dir: str = DEFAULT_BACKTEST_DIR) -> tuple[list, str | None]:
    path = find_latest_market_snapshot_path(backtest_dir)
    rows = safe_load_json(path) if path else []
    return rows or [], path


def _save_snapshot(rows: list, path: str) -> None:
    tmp = path + ".tmp"
    lock = path + ".lock"
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with with_locked_file(lock):
        with open(tmp, "w") as f:
            json.dump(rows, f, indent=2)
        try:
            with open(tmp, "r") as f:
                json.load(f)
        except Exception as e:  # pragma: no cover - unexpected parse failure
            logger.error("❌ Failed to parse snapshot temp file %s: %s", tmp, e)
            return
        os.replace(tmp, path)


def recheck_pending_bets(backtest_dir: str = DEFAULT_BACKTEST_DIR) -> None:
    rows, path = _load_snapshot(backtest_dir)
    if not path or not rows:
        logger.warning("\u26a0\ufe0f No snapshot rows found")
        return

    existing = load_existing_stakes("logs/market_evals.csv")
    session_exposure = defaultdict(set)
    theme_stakes = build_theme_exposure_tracker("logs/market_evals.csv")
    eval_tracker = load_eval_tracker()

    updated_rows = []
    changed = False
    now_ts = datetime.now().isoformat()

    for row in rows:
        if not isinstance(row, dict):
            updated_rows.append(row)
            continue
        if row.get("logged"):
            updated_rows.append(row)
            continue

        result = evaluate_snapshot_row_for_logging(
            row,
            theme_stakes,
            eval_tracker,
            existing,
        )

        if result and not result.get("skip_reason") and result.get("log"):
            row.update(result)
            row["logged"] = True
            row["logged_ts"] = now_ts
            row["queued"] = False
            changed = True
        else:
            reason = (result or {}).get("skip_reason")
            if reason:
                row["skip_reason"] = reason
            row["queued"] = True
            changed = True
        updated_rows.append(row)

    if changed:
        _save_snapshot(updated_rows, path)
        logger.info("\u2705 Snapshot updated: %s", path)


def update_pending_from_snapshot(rows: list) -> None:  # Backwards compatibility
    return


def main() -> None:
    while True:
        recheck_pending_bets()
        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    main()
