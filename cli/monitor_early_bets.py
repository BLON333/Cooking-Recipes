import os
import glob
import json
from core.bootstrap import *  # noqa
import time
from collections import defaultdict
from datetime import datetime

from core.utils import (
    parse_game_id,
    EASTERN_TZ,
)
from core.time_utils import compute_hours_to_game
from core.logger import get_logger
from core.confirmation_utils import required_market_move
from core.pending_bets import (
    load_pending_bets,
    save_pending_bets,
    PENDING_BETS_PATH,
    validate_pending_bets,
    infer_market_class,
)
from core.market_normalizer import normalize_market_key
from core.snapshot_core import _assign_snapshot_role
from core.market_eval_tracker import (
    load_tracker as load_eval_tracker,
    build_tracker_key,
)
from cli.log_betting_evals import (
    write_to_csv,
    load_existing_stakes,
    record_successful_log,
    load_market_conf_tracker,
    build_theme_exposure_tracker,
)
from core.should_log_bet import should_log_bet
import pytz

logger = get_logger(__name__)

CHECK_INTERVAL = 30 * 60  # 30 minutes

# Directory containing generated snapshot JSON files
DEFAULT_SNAPSHOT_DIR = os.path.join(os.path.dirname(__file__), "..", "backtest")


def load_latest_snapshot(snapshot_dir: str = DEFAULT_SNAPSHOT_DIR) -> list:
    """Load the most recent ``market_snapshot_*.json`` from ``snapshot_dir``."""
    pattern = os.path.join(snapshot_dir, "market_snapshot_*.json")
    files = glob.glob(pattern)
    if not files:
        logger.warning("⚠️ No snapshot files found in %s", snapshot_dir)
        return []

    latest = max(files, key=os.path.getmtime)
    try:
        with open(latest, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception as e:
        logger.warning("⚠️ Failed to load snapshot %s — %s", latest, e)
        return []


def _start_time_from_gid(game_id: str) -> datetime | None:
    parts = parse_game_id(game_id)
    date = parts.get("date")
    time_part = parts.get("time", "")
    if not date:
        return None
    if time_part.startswith("T"):
        # Handle tokens like "T1845" or "T1845-DH1" by isolating the time digits
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


def _clean_snapshot_row(row: dict) -> dict:
    """Return a sanitized pending bet dict from ``row``."""
    allowed: dict = {}
    for k, v in row.items():
        if k.startswith("_") or (
            k.startswith("snapshot_") and k not in {"snapshot_role", "snapshot_roles"}
        ) or k.endswith("_display"):
            continue
        allowed[k] = v
    return allowed


def merge_snapshot_pending(pending: dict, rows: list) -> dict:
    """Merge queued bets from ``rows`` into ``pending``."""
    if not isinstance(pending, dict):
        pending = {}

    merged = dict(pending)
    for r in rows:
        if not isinstance(r, dict):
            continue
        if not r.get("queued_ts"):
            continue
        gid = r.get("game_id")
        market = r.get("market")
        side = r.get("side")
        if not gid or not market:
            continue
        key = f"{gid}:{market}:{side}"
        base = merged.get(key, {})
        bet = _clean_snapshot_row(r)
        if "market_class" not in bet:
            meta = normalize_market_key(market or "")
            bet["market_class"] = meta.get("market_class", "main")
        bet["market_group"] = infer_market_class(market)
        # Assign snapshot role if missing
        if "snapshot_role" not in bet:
            bet["snapshot_role"] = _assign_snapshot_role(bet)

        # Assign snapshot_roles list
        roles = set(bet.get("snapshot_roles", []))
        roles.add(bet["snapshot_role"])
        if "best_book" not in roles:
            roles.add("best_book")
        bet["snapshot_roles"] = sorted(roles)

        game_parts = parse_game_id(bet.get("game_id", ""))
        date = game_parts.get("date")
        time_str = game_parts.get("time", "").replace("T", "")
        if date and time_str:
            try:
                dt = datetime.strptime(f"{date} {time_str}", "%Y-%m-%d %H%M")
                dt = dt.replace(tzinfo=pytz.timezone("US/Eastern"))
                bet["hours_to_game"] = round(compute_hours_to_game(dt), 2)
            except Exception:
                pass

        baseline = base.get("baseline_consensus_prob")
        if baseline is None:
            baseline = bet.get("baseline_consensus_prob")
        if baseline is None:
            fallback = bet.get("market_prob")
            if fallback is not None:
                baseline = fallback
        if baseline is not None:
            bet["baseline_consensus_prob"] = baseline

        bet["queued_ts"] = base.get("queued_ts", r.get("queued_ts"))
        merged[key] = bet
    return merged


def update_pending_from_snapshot(rows: list, path: str = PENDING_BETS_PATH) -> None:
    """Overwrite ``pending_bets.json`` with entries built from ``rows``."""
    tracker = load_market_conf_tracker()
    existing = load_pending_bets(path)
    pending: dict = {}
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
                continue  # drop logged bets for games in the past
        except Exception:
            pass
        # Refresh logged bets each loop instead of discarding them
        if ev < 5.0 or rk < 1.0:
            continue  # Still filter weak edges
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
        if key in tracker and isinstance(tracker[key], dict):
            cp = tracker[key].get("consensus_prob")
            if cp is not None:
                entry["baseline_consensus_prob"] = cp

        existing_row = existing.get(key)
        if existing_row:
            baseline = existing_row.get("baseline_consensus_prob")
            if baseline is not None:
                entry["baseline_consensus_prob"] = baseline

        # Fallback to market_prob or consensus_prob when no baseline is present
        baseline = entry.get("baseline_consensus_prob")
        if baseline is None:
            baseline = row.get("market_prob") or row.get("consensus_prob")
            if baseline is not None:
                entry["baseline_consensus_prob"] = baseline

        pending[key] = entry

    if pending:
        save_pending_bets(pending, path)
        validate_pending_bets(pending)



def enrich_pending_row(row: dict) -> dict:
    from core.market_normalizer import normalize_market_key, normalize_side
    from core.time_utils import compute_hours_to_game  # noqa: F401
    from core.utils import parse_game_id
    from datetime import datetime
    import pytz

    enriched = row.copy()

    game_parts = parse_game_id(enriched["game_id"])
    date = game_parts.get("date")
    time_str = game_parts.get("time", "").replace("T", "")
    if date and time_str:
        try:
            dt = datetime.strptime(f"{date} {time_str}", "%Y-%m-%d %H%M")
            enriched["Start Time (ISO)"] = dt.replace(tzinfo=pytz.timezone("US/Eastern")).isoformat()
        except Exception:
            pass

    market_meta = normalize_market_key(enriched.get("market", ""))
    enriched["segment"] = market_meta.get("segment", "full_game")
    enriched["market_class"] = market_meta.get("market_class", "main")
    enriched["segment_label"] = market_meta.get("label", "mainline")
    enriched["lookup_side"] = normalize_side(enriched.get("side", ""))

    enriched["best_book"] = enriched.get("book", "")
    enriched["blended_prob"] = (
        enriched.get("blended_prob") or enriched.get("sim_prob") or enriched.get("market_prob")
    )
    enriched["fair_odds"] = enriched.get("fair_odds") or enriched.get("blended_fv")
    enriched["market_fv"] = enriched.get("blended_fv")
    enriched["pricing_method"] = "snapshot_recheck"
    enriched["logger_config"] = "default"

    return enriched


def recheck_pending_bets(
    path: str = PENDING_BETS_PATH, snapshot_dir: str = DEFAULT_SNAPSHOT_DIR
) -> None:
    pending = load_pending_bets(path)
    snapshot_rows = load_latest_snapshot(snapshot_dir)
    pending = merge_snapshot_pending(pending, snapshot_rows)
    if not pending:
        return

    existing = load_existing_stakes("logs/market_evals.csv")
    session_exposure = defaultdict(set)
    theme_stakes = build_theme_exposure_tracker("logs/market_evals.csv")
    eval_tracker = load_eval_tracker()
    snapshot_index = {
        (
            r.get("game_id"),
            r.get("market"),
            str(r.get("side", "")).lower(),
        ): r
        for r in snapshot_rows
        if isinstance(r, dict)
    }

    updated = {}
    for key, bet in pending.items():
        bet.pop("adjusted_kelly", None)
        start_dt = _start_time_from_gid(bet["game_id"])
        if not start_dt:
            continue
        hours_to_game = compute_hours_to_game(start_dt)
        if hours_to_game <= 0:
            # Game started; drop entry
            continue
        row = snapshot_index.get(
            (bet.get("game_id"), bet.get("market"), str(bet.get("side", "")).lower())
        )
        if not row:
            start_dt = _start_time_from_gid(bet["game_id"])
            if not start_dt:
                continue
            hours_to_game = compute_hours_to_game(start_dt)

            if hours_to_game <= 0:
                # Game has started — remove from pending
                continue
            else:
                bet["hours_to_game"] = round(hours_to_game, 2)
                updated[key] = bet
                continue
        # Preserve baseline
        baseline = bet.get("baseline_consensus_prob")

        # ✅ Copy snapshot fields into pending bet entry
        for field in [
            "sim_prob",
            "blended_prob",
            "blended_fv",
            "ev_percent",
            "raw_kelly",
            "market_odds",
            "market_prob",
            "market_class",
            "hours_to_game",
        ]:
            if field in row:
                bet[field] = row[field]

        # ✅ Copy per_book from snapshot (optional, useful for CLV tracking)
        if "_raw_sportsbook" in row:
            bet["per_book"] = row["_raw_sportsbook"]

        # ✅ Normalize books_used
        consensus_books = row.get("consensus_books", {})
        if isinstance(consensus_books, dict):
            bet["books_used"] = list(consensus_books.keys())

        # 4. Compute movement vs baseline
        try:
            current_prob = float(row.get("market_prob"))
            base_prob = float(baseline)
            bet["consensus_move"] = round(current_prob - base_prob, 5)
        except Exception:
            bet["consensus_move"] = 0.0

        # 5. Compute required movement threshold
        book_count = len(bet.get("books_used", [])) or 1
        hours = bet.get("hours_to_game", 0)
        bet["required_move"] = round(
            required_market_move(
                hours_to_game=hours,
                book_count=book_count,
                market=bet.get("market"),
                ev_percent=bet.get("ev_percent"),
            ),
            5,
        )
        if bet.get("consensus_move", 0.0) < bet.get("required_move", 0.0):
            updated[key] = bet
            continue
        row = bet.copy()
        row.pop("adjusted_kelly", None)
        new_prob = bet.get("market_prob")
        row["consensus_prob"] = new_prob
        row["market_prob"] = new_prob
        row["hours_to_game"] = bet.get("hours_to_game", hours_to_game)
        if row.get("entry_type") == "first":
            raw_kelly = float(row.get("raw_kelly", 0))
            row["stake"] = round(raw_kelly, 4)
            row["full_stake"] = row["stake"]

        row = enrich_pending_row(row)

        evaluated = should_log_bet(
            row,
            theme_stakes,
            verbose=False,
            eval_tracker=eval_tracker,
            existing_csv_stakes=existing,
        )
        if evaluated:
            evaluated.pop("adjusted_kelly", None)
            result = write_to_csv(
                evaluated,
                "logs/market_evals.csv",
                existing,
                session_exposure,
                theme_stakes,
            )
            if result is None:
                reason = bet.get("skip_reason", "unknown")
                logger.info(
                    f"⏩ Skipped: {bet['game_id']} | {bet['market']} | {bet['side']} → reason: {reason}"
                )
            elif result and not result.get("skip_reason") and result.get("side"):
                record_successful_log(result, existing, theme_stakes)
                bet.update(result)
                bet["logged"] = True
                bet["logged_ts"] = datetime.now().isoformat()
            else:
                # logger.warning(
                #     "❌ Skipping tracker update: result was skipped or malformed → %s",
                #     result,
                # )
                pass
        updated[key] = bet

    # Ensure every pending entry has snapshot role metadata
    for k, v in updated.items():
        if "snapshot_role" not in v:
            v["snapshot_role"] = _assign_snapshot_role(v)
        if "snapshot_roles" not in v:
            v["snapshot_roles"] = [v["snapshot_role"], "best_book"]
        elif v["snapshot_role"] not in v["snapshot_roles"]:
            v["snapshot_roles"].append(v["snapshot_role"])
        if "best_book" not in v.get("snapshot_roles", []):
            v["snapshot_roles"].append("best_book")
        v["snapshot_roles"] = sorted(set(v["snapshot_roles"]))

    if updated != pending:
        # Format fields before saving
        for key, bet in updated.items():
            # Sort keys for readability
            bet_sorted = {k: bet[k] for k in sorted(bet.keys())}

            # Optional: Round float fields for cleaner storage
            for float_key in [
                "sim_prob",
                "blended_prob",
                "blended_fv",
                "ev_percent",
                "raw_kelly",
                "market_prob",
                "consensus_move",
                "required_move",
                "market_odds",
            ]:
                if float_key in bet_sorted and isinstance(bet_sorted[float_key], float):
                    bet_sorted[float_key] = round(bet_sorted[float_key], 4)

            # Keep books_used as a sorted list
            if isinstance(bet_sorted.get("books_used"), list):
                bet_sorted["books_used"] = sorted(bet_sorted["books_used"])

            updated[key] = bet_sorted

        save_pending_bets(updated, path)


def main() -> None:
    while True:
        recheck_pending_bets()
        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    main()
