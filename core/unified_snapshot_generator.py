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

from core.config import DEBUG_MODE, VERBOSE_MODE
import os
import sys
import json
import argparse
import shutil
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
    expand_snapshot_rows_with_kelly,
    _assign_snapshot_role,
    ensure_baseline_consensus_prob,
)
from core.snapshot_tracker_loader import find_latest_market_snapshot_path
from core.market_eval_tracker import (
    load_tracker,
    save_tracker,
)
from core.book_helpers import ensure_consensus_books
from core.market_pricer import kelly_fraction

logger = get_logger(__name__)

# Debug/verbose toggles
VERBOSE = False
DEBUG = False

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
            if lookup_fallback_odds(game_id, odds_json):
                print(f"\u2705 Matched odds for {game_id}")
            else:
                print(f"\u274C No odds found for {game_id}")
    return _core_build_snapshot_rows(sim_data, odds_json, min_ev=min_ev)




def _enrich_snapshot_row(row: dict) -> None:
    """Populate enrichment fields on a snapshot row."""
    # 🧩 Enrich: baseline
    baseline = row.get("baseline_consensus_prob")
    if baseline is None:
        baseline = row.get("market_prob") or row.get("consensus_prob")
    row["baseline_consensus_prob"] = baseline

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



def build_snapshot_for_date(
    date_str: str,
    odds_data: dict | None,
    ev_range: tuple[float, float] = (5.0, 20.0),
) -> list:
    """Return expanded snapshot rows for a single date."""
    sim_dir = os.path.join("backtest", "sims", date_str)
    sims = load_simulations(sim_dir)
    if not sims:
        logger.warning("❌ No simulation files found for %s", date_str)
        return []

    # Fetch or slice market odds
    if odds_data is None:
        odds = fetch_market_odds_from_api(list(sims.keys()))
    else:
        odds = {gid: lookup_fallback_odds(gid, odds_data) for gid in sims.keys()}

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
        _enrich_snapshot_row(row)

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
        args = parser.parse_args()

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
        MARKET_EVAL_TRACKER.update(load_tracker())
        MARKET_EVAL_TRACKER_BEFORE_UPDATE.clear()
        MARKET_EVAL_TRACKER_BEFORE_UPDATE.update(MARKET_EVAL_TRACKER)

        all_rows: list = []
        for date_str in date_list:
            rows_for_date = build_snapshot_for_date(date_str, odds_cache, (min_ev, max_ev))
            for row in rows_for_date:
                row["snapshot_for_date"] = date_str
            all_rows.extend(rows_for_date)

        if len(all_rows) == 0:
            logger.error(
                "❌ Failed to generate snapshot – no qualifying bets found."
            )
            sys.exit(1)

        # Save tracker after snapshot generation
        save_tracker(MARKET_EVAL_TRACKER)
        print(f"\U0001F4BE Saved market_eval_tracker with {len(MARKET_EVAL_TRACKER)} entries.")
    
        timestamp = now_eastern().strftime("%Y%m%dT%H%M")
        out_dir = "backtest"
        final_path = os.path.join(out_dir, f"market_snapshot_{timestamp}.json")
        tmp_path = os.path.join(out_dir, f"market_snapshot_{timestamp}.tmp")

        # 🧩 Enrich: baseline
        ensure_baseline_consensus_prob(all_rows)

        # 🔁 Merge persistent fields from prior snapshot
        prior_map = _load_prior_snapshot_map(out_dir)
        _merge_persistent_fields(all_rows, prior_map)

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
            os.rename(tmp_path, final_path)
        except Exception:
            logger.exception(
                "❌ Failed to finalize snapshot rename from %s to %s",
                tmp_path,
                final_path,
            )
            return

        logger.info("✅ Snapshot written: %s with %d rows", final_path, len(all_rows))
    except Exception:
        logger.exception("Snapshot generation failed:")
        sys.exit(1)


if __name__ == "__main__":
    main()