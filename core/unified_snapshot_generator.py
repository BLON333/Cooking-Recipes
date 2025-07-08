#!/usr/bin/env python
"""Unified snapshot generator.

This script combines the logic of the various snapshot generators into
one builder that outputs a timestamped JSON file. Each row is annotated
with ``snapshot_roles`` describing which downstream snapshot categories
it qualifies for.

Snapshot data flow after the snapshot-first refactor:

- Simulation rows from ``load_simulations()``
- Market odds from ``fetch_market_odds_from_api()`` or cached fallback data
- Row enrichment via ``_enrich_snapshot_row()``
- Persistent fields merged from the prior snapshot using
  ``_merge_persistent_fields()``
"""

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from core.config import DEBUG_MODE, VERBOSE_MODE
import json
import argparse
import shutil
import csv
from datetime import timedelta
from core.bootstrap import *  # noqa


from core.utils import now_eastern, safe_load_json, lookup_fallback_odds, parse_game_id
from core.logger import get_logger
from core.odds_fetcher import fetch_market_odds_from_api
from core.book_whitelist import ALLOWED_BOOKS
from core.snapshot_core import (
    load_simulations,
    build_snapshot_rows as _core_build_snapshot_rows,
    MARKET_EVAL_TRACKER,
    MARKET_EVAL_TRACKER_BEFORE_UPDATE,
    load_snapshot_tracker,
    expand_snapshot_rows_with_kelly,
    _assign_snapshot_role,
    ensure_baseline_consensus_prob,
)
from core.snapshot_tracker_loader import find_latest_market_snapshot_path
from core.book_helpers import ensure_consensus_books
from core.market_pricer import kelly_fraction
from core.confirmation_utils import required_market_move, extract_book_count
from core.consensus_pricer import calculate_consensus_prob

logger = get_logger(__name__)

# Debug/verbose toggles
VERBOSE = False
DEBUG = False
DEBUG_MOVEMENT = False
_movement_debug_count = 0
MOVEMENT_DEBUG_LIMIT = 5

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def latest_odds_file(folder="data/market_odds") -> str | None:
    files = sorted(
        [
            f
            for f in os.listdir(folder)
            if f.startswith("market_odds_") and f.endswith(".json")
        ],
        reverse=True,
    )
    return os.path.join(folder, files[0]) if files else None


# ---------------------------------------------------------------------------
# Snapshot role helpers
# ---------------------------------------------------------------------------
# Book list aligned with ODDS_FETCHER Issue 1 updates
POPULAR_BOOKS = list(ALLOWED_BOOKS)


def is_best_book_row(row: dict) -> bool:
    """Return True if row uses a popular sportsbook."""
    return row.get("book") in POPULAR_BOOKS


def is_live_snapshot_row(row: dict) -> bool:
    """Return True if row qualifies for the live snapshot."""
    return row.get("ev_percent", 0) >= 3.0


def is_personal_book_row(row: dict) -> bool:
    """Return True if row is from a personal sportsbook."""
    return row.get("book") in POPULAR_BOOKS


# ---------------------------------------------------------------------------
# Snapshot generation
# ---------------------------------------------------------------------------


def build_snapshot_rows(sim_data: dict, odds_json: dict, min_ev: float = 0.01):
    """Wrapper around snapshot_core.build_snapshot_rows with debug logging."""
    if VERBOSE or DEBUG:
        for game_id in sim_data.keys():
            print(f"\U0001F50D Evaluating {game_id}")
            if lookup_fallback_odds(game_id, odds_json)[0]:
                print(f"\u2705 Matched odds for {game_id}")
            else:
                print(f"\u274C No odds found for {game_id}")
    return _core_build_snapshot_rows(sim_data, odds_json, min_ev=min_ev)




def _enrich_snapshot_row(row: dict, *, debug_movement: bool = False) -> None:
    """Populate enrichment fields on a snapshot row."""
    baseline = row.get("baseline_consensus_prob")

    curr = row.get("market_prob") or row.get("consensus_prob")

    # 🧩 Enrich: movement
    movement = "same"
    if baseline is not None and curr is not None:
        try:
            diff = float(curr) - float(baseline)
            if abs(diff) < 1e-6:
                movement = "same"
            elif diff > 0:
                movement = "up"
            else:
                movement = "down"
        except Exception:
            movement = "same"
    row["mkt_movement"] = movement

    if baseline is not None and curr is not None and movement != "same":
        row["mkt_prob_display"] = f"{baseline * 100:.1f}% → {curr * 100:.1f}%"
    elif curr is not None:
        row["mkt_prob_display"] = f"{curr * 100:.1f}%"
    else:
        row["mkt_prob_display"] = "-"

    # 🧩 Enrich: stake
    if row.get("stake") is None:
        prob = row.get("blended_prob") or row.get("sim_prob")
        odds = row.get("market_odds")
        if prob is not None and odds is not None:
            fraction = 0.125 if row.get("market_class") == "alternate" else 0.25
            stake_val = kelly_fraction(prob, float(odds), fraction=fraction)
            row["stake"] = stake_val
            row["raw_kelly"] = stake_val

    row["snapshot_stake"] = round(float(row.get("stake", 0)), 2)
    row["is_prospective"] = row.get("stake", 0) == 0 and row.get("raw_kelly", 0) > 0

    # 🧩 Enrich: FV tier
    fv = row.get("blended_fv")
    if fv is None:
        p = row.get("blended_prob") or row.get("sim_prob")
        if p:
            fv = 1 / p
            row["blended_fv"] = fv
    if fv is not None:
        if abs(fv) >= 150:
            tier = "A"
        elif abs(fv) >= 120:
            tier = "B"
        else:
            tier = "C"
        row["fv_tier"] = tier

    # 🧩 Enrich: roles
    roles = list(row.get("snapshot_roles") or [])
    role = _assign_snapshot_role(row)
    if role not in roles:
        roles.append(role)
    if is_live_snapshot_row(row) and "live" not in roles:
        roles.append("live")
    if is_personal_book_row(row) and "personal" not in roles:
        roles.append("personal")
    row["snapshot_role"] = role
    row["snapshot_roles"] = list(dict.fromkeys(roles))

    # 🧩 Enrich: stake visibility
    visible = False
    if row.get("logged") and row.get("hours_to_game", 0) > 0:
        visible = True
    elif roles:
        visible = True
    row["visible_in_snapshot"] = visible

    # 🧩 Enrich: confirmation metrics
    try:
        curr_prob = float(row.get("market_prob"))
        base_prob = float(row.get("baseline_consensus_prob"))
        row["consensus_move"] = round(curr_prob - base_prob, 5)
    except Exception:
        row["consensus_move"] = 0.0

    try:
        hours = float(row.get("hours_to_game", 0))
    except Exception:
        hours = 0.0
    book_count = extract_book_count(row)
    row["required_move"] = round(
        required_market_move(
            hours_to_game=hours,
            book_count=book_count,
            market=row.get("market"),
            ev_percent=row.get("ev_percent"),
        ),
        5,
    )

    if row.get("consensus_move", 0.0) >= row.get("required_move", 0.0):
        row["movement_confirmed"] = True
    else:
        row.setdefault("movement_confirmed", False)

    if debug_movement:
        global _movement_debug_count
        if _movement_debug_count < MOVEMENT_DEBUG_LIMIT:
            delta = (row.get("market_prob") or 0) - (row.get("baseline_consensus_prob") or 0)
            print(
                f"Movement Debug → game_id: {row.get('game_id')} | Baseline: {row.get('baseline_consensus_prob')}"
                f" | Market: {row.get('market_prob')} | Δ = {delta*100:+.1f}% | confirmed: {row.get('movement_confirmed')}"
            )
            _movement_debug_count += 1
        elif _movement_debug_count == MOVEMENT_DEBUG_LIMIT:
            print("Movement Debug output truncated...")
            _movement_debug_count += 1

    # 🧩 Enrich: early/low-EV gating
    ev = row.get("ev_percent", 0.0) or 0.0
    rk = row.get("raw_kelly", 0.0) or 0.0
    stake = row.get("stake", row.get("full_stake", 0.0)) or 0.0
    if ev < 5.0 and rk < 1.0 and stake < 1.0:
        row.setdefault("skip_reason", "low_ev")

    segment = str(row.get("segment", ""))
    if segment in {"1st_3", "1st_7", "team_totals"} and hours > 12:
        row.setdefault("skip_reason", "time_blocked")
        row["entry_type"] = "none"


def _load_prior_snapshot_map(directory: str = "backtest") -> dict:
    """Return map of snapshot key to prior row."""
    path = find_latest_market_snapshot_path(directory)
    if not path or not os.path.exists(path):
        return {}
    data = safe_load_json(path)
    if not isinstance(data, list):
        return {}
    mapping = {}
    for r in data:
        key = (r.get("game_id"), r.get("market"), r.get("side"))
        mapping[key] = r
    return mapping


def _merge_persistent_fields(rows: list, prior_map: dict) -> None:
    """Merge persistent state fields from ``prior_map`` into ``rows``."""
    fields = [
        "logged",
        "logged_ts",
        "queued",
        "queued_ts",
        "skip_reason",
        "baseline_consensus_prob",
        "snapshot_roles",
        "movement_confirmed",
        "last_seen_loop_ts",
    ]

    now_ts = now_eastern().isoformat()
    for row in rows:
        key = (row.get("game_id"), row.get("market"), row.get("side"))
        prior = prior_map.get(key)
        if not prior:
            row["last_seen_loop_ts"] = now_ts
            continue
        for field in fields:
            if field == "baseline_consensus_prob":
                if row.get(field) is None and prior.get(field) is not None:
                    row[field] = prior[field]
            elif field == "snapshot_roles":
                prior_roles = prior.get(field)
                if prior_roles:
                    roles = set(prior_roles)
                    roles.update(row.get(field, []))
                    row[field] = sorted(roles)
            else:
                if not row.get(field) and prior.get(field) is not None:
                    row[field] = prior[field]
        row["last_seen_loop_ts"] = now_ts


_SANITIZE_WARNED_TYPES: set[type] = set()


def sanitize_json_row(row: dict) -> dict:
    """Return a copy of ``row`` with JSON-serializable values."""
    sanitized: dict = {}
    for k, v in row.items():
        new_v = v
        warn_type: type | None = None
        if isinstance(v, set):
            new_v = list(v)
            warn_type = set
        else:
            try:
                import numpy as np
                if isinstance(v, np.bool_):
                    new_v = bool(v)
                    warn_type = np.bool_
                elif isinstance(v, np.generic):
                    new_v = v.item()
                    warn_type = type(v)
            except Exception:
                pass
        sanitized[k] = new_v
        if warn_type and warn_type not in _SANITIZE_WARNED_TYPES:
            logger.warning("Converting %s for JSON serialization", warn_type)
            _SANITIZE_WARNED_TYPES.add(warn_type)
    return sanitized


def build_snapshot_for_date(
    date_str: str,
    odds_data: dict | None,
    ev_range: tuple[float, float] = (5.0, 20.0),
    prior_map: dict | None = None,
) -> list:
    """Return expanded snapshot rows for a single date."""
    if prior_map is None:
        prior_map = {}
    sim_dir = os.path.join("backtest", "sims", date_str)
    sims = load_simulations(sim_dir)
    if not sims:
        logger.warning("❌ No simulation files found for %s", date_str)
        return []

    # Fetch or slice market odds
    if odds_data is None:
        odds = fetch_market_odds_from_api(list(sims.keys()))
    else:
        odds = {gid: lookup_fallback_odds(gid, odds_data)[0] for gid in sims.keys()}

    for gid in sims.keys():
        if gid not in odds or odds.get(gid) is None:
            logger.warning(
                "\u26A0\uFE0F No odds found for %s \u2014 check if sim or odds file used wrong ID format",
                gid,
            )

    # Build base rows and expand per-book variants
    raw_rows = build_snapshot_rows(sims, odds, min_ev=0.01)
    logger.info("\U0001F9EA Raw bets from build_snapshot_rows(): %d", len(raw_rows))
    expanded_rows = expand_snapshot_rows_with_kelly(raw_rows, POPULAR_BOOKS)
    logger.info("\U0001F9E0 Expanded per-book rows: %d", len(expanded_rows))

    rows = expanded_rows

    # 🎯 Retain all rows (EV% filter removed)
    min_ev, max_ev = ev_range  # kept for compatibility
    logger.info(
        "📊 Snapshot generation: %d rows evaluated (no EV%% filtering applied)",
        len(rows),
    )

    # 📦 Assign snapshot roles and enrich rows
    snapshot_rows = []
    best_book_tracker: dict[tuple[str, str, str], dict] = {}

    for row in rows:
        row_market = row.get("market")
        row_label = row.get("side")
        if row_market and row_label:
            consensus_data, method = calculate_consensus_prob(
                row["game_id"],
                odds_data,
                row_market,
                row_label,
                debug=False,
            )
            if consensus_data and consensus_data.get("consensus_prob") is not None:
                row["consensus_prob"] = consensus_data["consensus_prob"]

        snap_key = (row.get("game_id"), row.get("market"), row.get("side"))
        prior_baseline = prior_map.get(snap_key, {}).get("baseline_consensus_prob") if prior_map else None

        if prior_baseline is not None:
            row["baseline_consensus_prob"] = prior_baseline
        else:
            try:
                odds_baseline = (
                    odds_data[row["game_id"]]
                    [row["market"]]
                    [row["side"]]
                    .get("consensus_prob")
                )
                if odds_baseline is not None:
                    row["baseline_consensus_prob"] = odds_baseline
            except (KeyError, TypeError):
                pass

        _enrich_snapshot_row(row, debug_movement=DEBUG_MOVEMENT)

        if is_best_book_row(row):
            key = (row.get("game_id"), row.get("market"), row.get("side"))
            best_row = best_book_tracker.get(key)
            if not best_row:
                best_book_tracker[key] = row
            else:
                ev = row.get("ev_percent", 0)
                best_ev = best_row.get("ev_percent", 0)
                if ev > best_ev or (
                    ev == best_ev and row.get("stake", 0) > best_row.get("stake", 0)
                ):
                    best_book_tracker[key] = row

        snapshot_rows.append(row)

    for best_row in best_book_tracker.values():
        best_row.setdefault("snapshot_roles", []).append("best_book")

    final_rows = snapshot_rows

    logger.info("\u2705 Final snapshot rows to write: %d", len(final_rows))

    num_with_roles = sum(1 for r in final_rows if r.get("snapshot_roles"))
    num_stake_half = sum(1 for r in final_rows if r.get("stake", 0) >= 0.5)
    num_stake_one = sum(1 for r in final_rows if r.get("stake", 0) >= 1.0)
    logger.info(
        "\U0001F4CA Of those: %d rows have roles, %d have stake \u2265 0.5u, %d have stake \u2265 1.0u",
        num_with_roles,
        num_stake_half,
        num_stake_one,
    )

    return final_rows


def main() -> None:
    try:
        parser = argparse.ArgumentParser(description="Generate unified market snapshot")
        parser.add_argument("--date", default=None)
        parser.add_argument("--odds-path", default=None, help="Path to cached odds JSON")
        parser.add_argument(
            "--ev-range",
            default="5.0,20.0",
            help="EV%% range to include as 'min,max'",
        )
        parser.add_argument(
            "--debug-movement",
            action="store_true",
            help="Print market movement confirmation debug logs",
        )
        args = parser.parse_args()

        global DEBUG_MOVEMENT
        DEBUG_MOVEMENT = args.debug_movement

        if args.date:
            date_list = [d.strip() for d in str(args.date).split(",") if d.strip()]
        else:
            today = now_eastern().strftime("%Y-%m-%d")
            tomorrow = (now_eastern() + timedelta(days=1)).strftime("%Y-%m-%d")
            date_list = [today, tomorrow]
    
        try:
            min_ev, max_ev = map(float, args.ev_range.split(","))
        except Exception:
            logger.error("❌ Invalid --ev-range format, expected 'min,max'")
            return
    
        odds_cache = None
        if args.odds_path:
            if not os.path.exists(args.odds_path):
                logger.error(
                    "❌ Failed to generate snapshot – odds file not found: %s",
                    args.odds_path,
                )
                sys.exit(1)
            odds_cache = safe_load_json(args.odds_path)
            if odds_cache:
                logger.info("📥 Loaded odds from %s", args.odds_path)
            else:
                logger.error(
                    "❌ Failed to generate snapshot – no valid odds data loaded"
                    " from %s",
                    args.odds_path,
                )
                sys.exit(1)
        else:
            auto_path = latest_odds_file()
            if auto_path:
                odds_cache = safe_load_json(auto_path)
                if odds_cache:
                    logger.info("📥 Auto-loaded latest odds: %s", auto_path)
                else:
                    logger.error(
                        "❌ Failed to generate snapshot – no valid odds data"
                        " loaded from %s",
                        auto_path,
                    )
                    sys.exit(1)
            if odds_cache is None:
                logger.error(
                    "❌ Failed to generate snapshot – no market_odds_*.json files"
                    " found."
                )
                sys.exit(1)
    
        # Refresh tracker baseline before snapshot generation
        MARKET_EVAL_TRACKER.clear()
        MARKET_EVAL_TRACKER.update(load_snapshot_tracker())
        MARKET_EVAL_TRACKER_BEFORE_UPDATE.clear()
        MARKET_EVAL_TRACKER_BEFORE_UPDATE.update(MARKET_EVAL_TRACKER)

        all_rows: list = []
        prior_map = _load_prior_snapshot_map("backtest")
        for date_str in date_list:
            rows_for_date = build_snapshot_for_date(
                date_str,
                odds_cache,
                (min_ev, max_ev),
                prior_map=prior_map,
            )
            for row in rows_for_date:
                row["snapshot_for_date"] = date_str
            all_rows.extend(rows_for_date)

        if len(all_rows) == 0:
            logger.error(
                "❌ Failed to generate snapshot – no qualifying bets found."
            )
            sys.exit(1)

        # Snapshot tracker state is not persisted separately
    
        timestamp = now_eastern().strftime("%Y%m%dT%H%M")
        out_dir = "backtest"
        final_path = os.path.join(out_dir, f"market_snapshot_{timestamp}.json")
        tmp_path = os.path.join(out_dir, f"market_snapshot_{timestamp}.tmp")

        # 🔁 Merge persistent fields from prior snapshot
        _merge_persistent_fields(all_rows, prior_map)

        # 🧩 Enrich: baseline
        ensure_baseline_consensus_prob(all_rows, MARKET_EVAL_TRACKER_BEFORE_UPDATE)

        all_rows = [sanitize_json_row(r) for r in all_rows]

        os.makedirs(out_dir, exist_ok=True)
        with open(tmp_path, "w") as f:
            json.dump(all_rows, f, indent=2)

        # Validate written JSON before renaming
        try:
            with open(tmp_path) as f:
                json.load(f)
        except Exception:
            logger.exception("❌ Snapshot JSON validation failed for %s", tmp_path)
            bad_path = final_path + ".bad.json"
            try:
                shutil.move(tmp_path, bad_path)
                logger.error("🚨 Corrupted snapshot moved to %s", bad_path)
            except Exception as mv_err:
                logger.error("❌ Failed to move corrupt snapshot: %s", mv_err)
            return

        try:
            if os.path.exists(final_path):
                os.remove(final_path)  # 🔐 Ensure overwrite is possible
            os.rename(tmp_path, final_path)
        except Exception:
            logger.exception(
                "❌ Failed to finalize snapshot rename from %s to %s",
                tmp_path,
                final_path,
            )
            return

        logger.info("✅ Snapshot written: %s with %d rows", final_path, len(all_rows))

        # -------------------------------------------------------------------
        # Write summary CSV for log-ready bets
        # -------------------------------------------------------------------
        summary_path = os.path.join(out_dir, f"snapshot_summary_{timestamp}.csv")
        headers = [
            "game_id",
            "market",
            "side",
            "ev_percent",
            "raw_kelly",
            "stake",
            "baseline_consensus_prob",
            "market_prob",
            "best_book",
            "market_odds",
            "consensus_move",
            "required_move",
            "movement_confirmed",
            "should_be_logged",
        ]

        with open(summary_path, "w", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()

            for row in all_rows:
                try:
                    ev = float(row.get("ev_percent", 0))
                    stake = float(row.get("stake", row.get("full_stake", 0) or 0))
                    if ev < 5.0 or stake < 1.0:
                        continue

                    baseline = row.get("baseline_consensus_prob")
                    market_prob = row.get("market_prob")
                    best_book = row.get("best_book")
                    market_odds = row.get("market_odds")
                    required_move = row.get("required_move")
                    movement_confirmed = bool(row.get("movement_confirmed"))

                    if None in (baseline, market_prob, best_book, market_odds, required_move):
                        continue

                    consensus_move = float(baseline) - float(market_prob)
                    should_be_logged = (
                        "Yes" if movement_confirmed and ev >= 5.0 and stake >= 1.0 else "No"
                    )

                    writer.writerow(
                        {
                            "game_id": row.get("game_id"),
                            "market": row.get("market"),
                            "side": row.get("side"),
                            "ev_percent": f"{ev:.4f}",
                            "raw_kelly": f"{float(row.get('raw_kelly', 0) or 0):.4f}",
                            "stake": f"{stake:.4f}",
                            "baseline_consensus_prob": f"{float(baseline):.4f}",
                            "market_prob": f"{float(market_prob):.4f}",
                            "best_book": best_book,
                            "market_odds": market_odds,
                            "consensus_move": f"{consensus_move:.4f}",
                            "required_move": f"{float(required_move):.4f}",
                            "movement_confirmed": movement_confirmed,
                            "should_be_logged": should_be_logged,
                        }
                    )
                except Exception:
                    # Skip rows with invalid or missing data
                    continue
    except Exception:
        logger.exception("Snapshot generation failed:")
        sys.exit(1)


if __name__ == "__main__":
    main()
