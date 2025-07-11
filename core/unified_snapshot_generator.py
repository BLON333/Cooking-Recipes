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
from core.odds_normalizer import canonical_game_id
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
from core.confirmation_utils import required_market_move
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
        for game_id in sim_data:
            canonical_id = canonical_game_id(game_id)
            print(f"\U0001F50D Sim game_id: {game_id} \u2192 Canonical: {canonical_id}")

            if canonical_id not in odds_json:
                print(f"\u274C No odds found for canonical_id: {canonical_id}")
                continue

            game_odds = odds_json[canonical_id]
            print(f"\u2705 Found odds for: {canonical_id} \u2192 Markets: {list(game_odds.keys())}")

            for market in ["totals", "h2h", "spreads"]:
                if market not in game_odds:
                    print(f"\u26A0\uFE0F Market '{market}' not found for {canonical_id}")
                    continue

                for side_key, side_data in game_odds[market].items():
                    cp = side_data.get("consensus_prob")
                    if cp is None:
                        print(f"\u26A0\uFE0F No consensus_prob for {market} \u2192 {side_key}")
                    else:
                        print(f"\u2705 {market} \u2192 {side_key}: consensus_prob = {cp}")

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
    try:
        base_prob = float(row.get("baseline_consensus_prob"))
        market_prob = float(row.get("market_prob"))
        ev = float(row.get("ev_percent", 0))
        stake = float(row.get("stake", 0))
        if (
            (market_prob - base_prob) > 0
            and ev >= 5.0
            and stake >= 1.0
            and "fv_drop" not in roles
        ):
            roles.append("fv_drop")
    except Exception:
        pass
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
    book_count = len(row.get("books_used", [])) or 1
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
            if VERBOSE or DEBUG:
                print(
                    f"Movement Debug → game_id: {row.get('game_id')} | Baseline: {row.get('baseline_consensus_prob')}"
                    f" | Market: {row.get('market_prob')} | Δ = {delta*100:+.1f}% | confirmed: {row.get('movement_confirmed')}"
                )
            _movement_debug_count += 1
        elif _movement_debug_count == MOVEMENT_DEBUG_LIMIT:
            if VERBOSE or DEBUG:
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
        canon_gid = canonical_game_id(r.get("game_id", ""))
        key = (
            canon_gid,
            r.get("market"),
            r.get("side"),
            r.get("book") or r.get("best_book"),
        )
        mapping[key] = r
    return mapping


def _merge_persistent_fields(rows: list, prior_map: dict) -> None:
    """Merge persistent state fields from ``prior_map`` into ``rows``."""

    now_ts = now_eastern().isoformat()
    filtered: list[dict] = []
    for row in rows:
        canon_gid = canonical_game_id(row.get("game_id", ""))
        book = row.get("book") or row.get("best_book")
        key = (
            canon_gid,
            row.get("market"),
            row.get("side"),
            book,
        )
        prior = prior_map.get(key)

        # Always update the heartbeat timestamp
        row["last_seen_loop_ts"] = now_ts

        if prior:
            for field in [
                "queued",
                "queued_ts",
                "logged",
                "logged_ts",
                "skip_reason",
                "baseline_consensus_prob",
                "movement_confirmed",
                "snapshot_roles",
                "theme_key",
            ]:
                val = row.get(field)
                if field == "snapshot_roles":
                    continue  # Don't merge roles — they are reassigned fresh
                if field in prior and (val is None or val is False or val == []):
                    row[field] = prior[field]


        skip = (
            row.get("skip_reason") in {"low_ev", "time_blocked"}
            and not row.get("queued")
            and not row.get("logged")
        )
        if not skip:
            filtered.append(row)

    rows[:] = filtered

def sanitize_json_row(row: dict) -> dict:
    """Return a sanitized copy of ``row`` ready for JSON serialization."""
    TRIM_FIELDS = {
        "_tracker_entry",
        "_prior_snapshot",
        "_raw_sportsbook",
        "consensus_books",
    }

    sanitized: dict = {}
    for k, v in row.items():
        if k in TRIM_FIELDS:
            continue
        try:
            json.dumps(v)
            sanitized[k] = v
        except (TypeError, OverflowError):
            sanitized[k] = str(v)
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

    if VERBOSE or DEBUG:
        print("🎯 Sim GIDs:")
        for gid in sims.keys():
            print(" →", gid)

    # Fetch or slice market odds
    if odds_data is None:
        odds = fetch_market_odds_from_api(list(sims.keys()))
    else:
        odds = {}
        if VERBOSE or DEBUG:
            print("🔍 Odds Matching Debug:")
        for gid in sims.keys():
            canon = canonical_game_id(gid)
            matched, matched_key = lookup_fallback_odds(canon, odds_data)
            if VERBOSE or DEBUG:
                print(f"  {gid} → {canon} → Match: {matched_key or '❌ No match'}")
            if matched:
                odds[canon] = matched

    for gid in sims.keys():
        canon_gid = canonical_game_id(gid)
        if canon_gid not in odds or odds.get(canon_gid) is None:
            logger.warning(
                "\u26A0\uFE0F No odds found for %s \u2014 check if sim or odds file used wrong ID format",
                gid,
            )

    # Build base rows and expand per-book variants
    raw_rows = build_snapshot_rows(sims, odds, min_ev=0.01)
    logger.info("\U0001F9EA Raw bets from build_snapshot_rows(): %d", len(raw_rows))

    for r in raw_rows:
        mkt = r.get("market")
        label = r.get("side")
        if not mkt or not label:
            continue
        canon_gid = canonical_game_id(r.get("game_id", ""))
        game_odds, _ = lookup_fallback_odds(canon_gid, odds)
        try:
            cp = game_odds[mkt][label].get("consensus_prob") if game_odds else None
        except Exception:
            cp = None
        if cp is not None:
            r["consensus_prob"] = cp

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
            canonical_gid = canonical_game_id(row["game_id"])
            game_odds, _ = lookup_fallback_odds(canonical_gid, odds)

            try:
                cp = game_odds[row_market][row_label].get("consensus_prob") if game_odds else None
            except Exception:
                cp = None

            if cp is not None:
                row["consensus_prob"] = cp
                if VERBOSE or DEBUG:
                    print(f"[Consensus] Using inherited value: {row['consensus_prob']}")

        canon_gid = canonical_game_id(row.get("game_id", ""))
        snap_key = (
            canon_gid,
            row_market,
            row_label,
            row.get("book") or row.get("best_book"),
        )
        prior_baseline = (
            prior_map.get(snap_key, {}).get("baseline_consensus_prob") if prior_map else None
        )

        canon_gid = canonical_game_id(row.get("game_id", ""))
        side_key = f"{canon_gid}:{row_market}:{row_label}"

        if prior_baseline is not None:
            row["baseline_consensus_prob"] = prior_baseline
        else:
            fallback = MARKET_EVAL_TRACKER_BEFORE_UPDATE.get(side_key, {}).get(
                "baseline_consensus_prob"
            )
            if fallback is not None:
                row["baseline_consensus_prob"] = fallback
            elif row.get("baseline_consensus_prob") is None:
                row["baseline_consensus_prob"] = row.get("consensus_prob")

        # Always recompute snapshot roles fresh for this build
        row.pop("snapshot_roles", None)
        _enrich_snapshot_row(row, debug_movement=DEBUG_MOVEMENT)

        if is_best_book_row(row):
            canon_gid = canonical_game_id(row.get("game_id", ""))
            key = (canon_gid, row.get("market"), row.get("side"))
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
        parser.add_argument("--debug", action="store_true", help="Enable debug logging")
        parser.add_argument("--verbose", action="store_true", help="Enable verbose mode")
        args = parser.parse_args()

        global DEBUG_MOVEMENT
        DEBUG_MOVEMENT = args.debug_movement
        global DEBUG, VERBOSE
        DEBUG = args.debug or DEBUG_MODE
        VERBOSE = args.verbose or VERBOSE_MODE
        if VERBOSE or DEBUG:
            print("🧪 DEBUG_MODE ENABLED — Verbose output activated")

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
        odds_file_path = None
        if args.odds_path:
            if not os.path.exists(args.odds_path):
                logger.error(
                    "❌ Failed to generate snapshot – odds file not found: %s",
                    args.odds_path,
                )
                sys.exit(1)
            odds_file_path = args.odds_path
        else:
            auto_path = latest_odds_file()
            if auto_path:
                odds_file_path = auto_path
            if odds_file_path is None:
                logger.error(
                    "❌ Failed to generate snapshot – no market_odds_*.json files"
                    " found."
                )
                sys.exit(1)

        if odds_file_path:
            try:
                with open(odds_file_path, "r", encoding="utf-8") as f:
                    odds_cache = json.load(f)
                if isinstance(odds_cache, dict) and odds_cache:
                    logger.info("📥 Loaded odds from %s", odds_file_path)
                    if VERBOSE or DEBUG:
                        print("📂 Odds file loaded:", odds_file_path)
                        print("📦 Odds file keys:", list(odds_cache.keys())[:5])
                else:
                    logger.error(
                        "❌ Odds file loaded but is empty or invalid structure: %s",
                        odds_file_path,
                    )
                    sys.exit(1)
            except Exception as e:
                logger.exception("❌ Failed to load odds from %s", odds_file_path)
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

        # 🗒️ Final deduplication pass
        before_dedup = len(all_rows)
        seen_keys: set[tuple] = set()
        deduped_rows: list = []
        for r in all_rows:
            key = (
                r.get("game_id"),
                r.get("market"),
                r.get("side"),
                r.get("book"),
            )
            if key in seen_keys:
                continue
            seen_keys.add(key)
            deduped_rows.append(r)
        dropped = before_dedup - len(deduped_rows)
        if dropped:
            logger.debug("🗒️ Deduplicated %d rows from final snapshot", dropped)
        all_rows = deduped_rows

        all_rows = [sanitize_json_row(r) for r in all_rows]

        os.makedirs(out_dir, exist_ok=True)
        with open(tmp_path, "w", encoding="utf-8") as f:
            f.write("[\n")
            first = True
            for row in all_rows:
                if not first:
                    f.write(",\n")
                json.dump(row, f)
                first = False
            f.write("\n]")

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
        # Write summary CSV for log-ready bets if verbose mode enabled
        # -------------------------------------------------------------------
        if VERBOSE:
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

                        consensus_move = float(market_prob) - float(baseline)
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
        else:
            logger.info("📝 Skipping snapshot_summary CSV (not in verbose mode)")

        if VERBOSE:
            # -------------------------------------------------------------------
            # Write debug CSV summarizing all snapshot rows
            # -------------------------------------------------------------------
            summary_debug_path = os.path.join(out_dir, f"snapshot_debug_{timestamp}.csv")
            debug_headers = [
                "game_id",
                "market",
                "side",
                "ev_percent",
                "stake",
                "raw_kelly",
                "baseline_consensus_prob",
                "market_prob",
                "consensus_move",
                "required_move",
                "movement_confirmed",
                "logged",
                "should_be_logged",
            ]

            with open(summary_debug_path, "w", newline="") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=debug_headers)
                writer.writeheader()

                for row in all_rows:
                    try:
                        baseline = row.get("baseline_consensus_prob")
                        market_prob = row.get("market_prob")
                        if baseline is None or market_prob is None:
                            continue

                        ev = float(row.get("ev_percent", 0))
                        stake = float(row.get("stake", row.get("full_stake", 0) or 0))
                        raw_kelly = float(row.get("raw_kelly", 0) or 0)
                        required_move = row.get("required_move")
                        movement_confirmed = bool(row.get("movement_confirmed"))
                        logged = bool(row.get("logged"))
                        consensus_move = float(market_prob) - float(baseline)

                        should_be_logged = (
                            "Yes" if (stake >= 1.0 and ev >= 5.0 and movement_confirmed) else "No"
                        )

                        writer.writerow(
                            {
                                "game_id": row.get("game_id"),
                                "market": row.get("market"),
                                "side": row.get("side"),
                                "ev_percent": f"{ev:.4f}",
                                "stake": f"{stake:.4f}",
                                "raw_kelly": f"{raw_kelly:.4f}",
                                "baseline_consensus_prob": f"{float(baseline):.4f}",
                                "market_prob": f"{float(market_prob):.4f}",
                                "consensus_move": f"{consensus_move:.4f}",
                                "required_move": f"{float(required_move):.4f}" if required_move is not None else "",
                                "movement_confirmed": movement_confirmed,
                                "logged": logged,
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