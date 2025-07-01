import os
import json
import time
from datetime import datetime

from core.utils import (
    safe_load_json,
    parse_game_id,
    EASTERN_TZ,
)
from core.logger import get_logger
from core.time_utils import compute_hours_to_game
from core.lock_utils import with_locked_file
from core.snapshot_core import _assign_snapshot_role

logger = get_logger(__name__)


def _start_time_from_gid(game_id: str) -> datetime | None:
    parts = parse_game_id(game_id)
    date = parts.get("date")
    time_part = parts.get("time", "")
    if not date:
        return None
    if time_part.startswith("T"):
        raw = time_part.split("-")[0][1:]
        digits = "".join(c for c in raw if c.isdigit())[:4]
        try:
            dt = datetime.strptime(f"{date} {digits}", "%Y-%m-%d %H%M")
            return dt.replace(tzinfo=EASTERN_TZ)
        except Exception:
            return None
    try:
        return datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=EASTERN_TZ)
    except Exception:
        return None

PENDING_BETS_PATH = os.path.join('logs', 'pending_bets.json')


def load_pending_bets(path: str = PENDING_BETS_PATH) -> dict:
    """Return dictionary of pending bets keyed by tracker key."""
    data = safe_load_json(path)
    if isinstance(data, dict):
        return data
    return {}


def save_pending_bets(pending: dict, path: str = PENDING_BETS_PATH) -> None:
    """Persist ``pending`` to ``path`` atomically using a lock."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    lock = f"{path}.lock"
    tmp = f"{path}.tmp"
    try:
        with with_locked_file(lock):
            with open(tmp, 'w') as f:
                json.dump(pending, f, indent=2)

            # Skip replace if contents are unchanged
            skip_replace = False
            if os.path.exists(path):
                try:
                    with open(path, 'r') as cur, open(tmp, 'r') as new:
                        if cur.read() == new.read():
                            skip_replace = True
                except Exception:
                    pass

            if not skip_replace:
                # Retry replace a few times in case another process still has the
                # file open. This mirrors the behavior used in other trackers.
                for _ in range(5):
                    try:
                        os.replace(tmp, path)
                        break
                    except PermissionError as e:
                        last_err = e
                        time.sleep(0.1)
                else:
                    print(f"⚠️ Failed to save pending bets: {last_err}")
            else:
                os.remove(tmp)
    except Exception as e:
        print(f"⚠️ Failed to save pending bets: {e}")


def queue_pending_bet(bet: dict, path: str = PENDING_BETS_PATH) -> None:
    """Append or update ``bet`` in ``pending_bets.json``."""
    pending = load_pending_bets(path)
    key = f"{bet['game_id']}:{bet['market']}:{bet['side']}"
    bet_copy = {
        k: v
        for k, v in bet.items()
        if not k.startswith("_") and k != "adjusted_kelly"
    }

    # Ensure required snapshot metadata is present
    if "market_class" not in bet_copy:
        bet_copy["market_class"] = "main"
    role = _assign_snapshot_role(bet_copy)
    bet_copy["snapshot_role"] = role
    roles = set(bet_copy.get("snapshot_roles") or [])
    roles.add("best_book")
    roles.add(role)
    bet_copy["snapshot_roles"] = sorted(roles)
    existing = pending.get(key, {})
    bet_copy["queued_ts"] = existing.get("queued_ts", datetime.now().isoformat())
    bet_copy["logged"] = bool(existing.get("logged", False))
    if "logged_ts" in existing:
        bet_copy["logged_ts"] = existing["logged_ts"]

    if "baseline_consensus_prob" not in bet_copy:
        baseline = bet_copy.get("market_prob") or bet_copy.get("consensus_prob")
        if baseline is not None:
            bet_copy["baseline_consensus_prob"] = baseline

    if "hours_to_game" not in bet_copy:
        start_dt = _start_time_from_gid(bet_copy["game_id"])
        if start_dt:
            bet_copy["hours_to_game"] = round(compute_hours_to_game(start_dt), 2)

    # Merge snapshot role information with any existing entry
    role = bet_copy.get("snapshot_role") or _assign_snapshot_role(bet_copy)
    bet_copy["snapshot_role"] = role
    existing_roles = []
    if isinstance(existing.get("snapshot_roles"), list):
        existing_roles.extend(existing["snapshot_roles"])
    if isinstance(bet_copy.get("snapshot_roles"), list):
        for r in bet_copy["snapshot_roles"]:
            if r not in existing_roles:
                existing_roles.append(r)
    for r in [role, "best_book"]:
        if r not in existing_roles:
            existing_roles.append(r)
    bet_copy["snapshot_roles"] = existing_roles

    pending[key] = bet_copy
    save_pending_bets(pending, path)
    validate_pending_bets(pending)


def validate_pending_bets(pending: dict) -> None:
    """Log a warning if any pending row is missing required fields."""
    missing_roles = 0
    missing_class = 0
    for row in pending.values():
        if not row.get("snapshot_roles"):
            missing_roles += 1
        if "market_class" not in row:
            missing_class += 1

    if missing_roles or missing_class:
        parts = []
        if missing_roles:
            parts.append(f"{missing_roles} rows missing snapshot_roles")
        if missing_class:
            parts.append(f"{missing_class} rows missing market_class")
        logger.warning("⚠️ pending_bets.json has %s", ", ".join(parts))
