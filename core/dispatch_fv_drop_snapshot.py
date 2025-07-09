#!/usr/bin/env python
from core.config import DEBUG_MODE, VERBOSE_MODE
import os
from core.bootstrap import *  # noqa

"""Dispatch FV drop snapshot (market probability increases) using the latest snapshot."""
from core.utils import parse_game_id, safe_load_json

import argparse
from typing import List
from collections import Counter
import pandas as pd
from dotenv import load_dotenv

# Load environment variables from the project root .env file
load_dotenv()

from core.snapshot_core import format_for_display, send_bet_snapshot_to_discord
from core.logger import get_logger
from core.should_log_bet import MAX_POSITIVE_ODDS, MIN_NEGATIVE_ODDS
from core.book_helpers import filter_by_odds, ensure_side
from core.book_whitelist import ALLOWED_BOOKS
from core.snapshot_tracker_loader import find_latest_market_snapshot_path

# Subset of books to include when posting to the main FV Drop webhook
FV_DROP_ALLOWED_BOOKS = [
    "pinnacle",
    "bovada",
    "fanduel",
    "betonlineag",
]

logger = get_logger(__name__)

# Optional debug log to verify environment variables are loaded
logger.debug("✅ Loaded webhook: %s", os.getenv("DISCORD_FV_DROP_WEBHOOK_URL"))


def load_snapshot_rows(path: str | None = None) -> list:
    """Return snapshot rows from ``path`` or the most recent snapshot file."""
    if not path:
        path = find_latest_market_snapshot_path("backtest")
    rows = safe_load_json(path) if path else []
    logger.info("📊 Loaded %d snapshot rows from %s", len(rows), path)
    for r in rows:
        ensure_side(r)
    return rows


def filter_by_date(rows: list, date_str: str | None) -> list:
    if not date_str:
        return rows
    return [
        r
        for r in rows
        if parse_game_id(str(r.get("game_id", ""))).get("date") == date_str
    ]


def filter_by_books(df: pd.DataFrame, books: List[str] | None) -> pd.DataFrame:
    """Return df filtered to the given book keys."""
    if not books or "Book" not in df.columns:
        return df
    clean_books = [b.strip() for b in books if b.strip()]
    if not clean_books:
        return df
    return df[df["Book"].isin(clean_books)]


def filter_main_lines(df: pd.DataFrame) -> pd.DataFrame:
    """Return df filtered to only main market lines."""
    if "Market Class" in df.columns:
        return df[df["Market Class"] == "Main"]
    return df



def is_market_prob_increasing(val: str) -> bool:
    """Return True if val contains an upward market probability shift."""
    if not isinstance(val, str) or "→" not in val:
        return False
    try:
        left, right = val.split("→")
        left = float(left.strip().replace("%", ""))
        right = float(right.strip().replace("%", ""))
        return right > left
    except Exception:
        return False


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Dispatch FV drop snapshot (market probability increases)"
    )
    parser.add_argument("--date", default=None, help="Filter by game date")
    parser.add_argument("--output-discord", action="store_true")
    parser.add_argument(
        "--force-dispatch",
        action="store_true",
        help="Force image snapshot to Discord even if empty",
    )
    parser.add_argument(
        "--books",
        default=os.getenv("FV_DROP_BOOKS"),
        help="Comma-separated book keys to include",
    )
    parser.add_argument(
        "--min-ev",
        type=float,
        default=5.0,
        help="Minimum EV% required to dispatch",
    )
    parser.add_argument(
        "--max-ev",
        type=float,
        default=20.0,
        help="Maximum EV% allowed to dispatch",
    )
    parser.add_argument(
        "--snapshot",
        default=None,
        help="Path to snapshot JSON (defaults to latest)",
    )
    parser.add_argument(
        "--min-move",
        type=float,
        default=None,
        help="Override required market move threshold",
    )
    args = parser.parse_args()

    # Clamp EV range to 5%-20%
    args.min_ev = max(5.0, args.min_ev)
    args.max_ev = min(20.0, args.max_ev)
    if args.min_ev > args.max_ev:
        args.max_ev = args.min_ev

    rows = load_snapshot_rows(args.snapshot)
    if not rows:
        logger.warning("⚠️ No snapshot rows found – skipping dispatch")
        return

    rows = [r for r in rows if "fv_drop" in (r.get("snapshot_roles") or [])]
    logger.info("🧾 Snapshot rows for role='fv_drop': %d", len(rows))
    if not rows:
        logger.warning("⚠️ No snapshot rows for role='fv_drop' — skipping dispatch")
        return

    for r in rows:
        if "book" not in r and "best_book" in r:
            r["book"] = r["best_book"]

    # ✅ Apply filtering based on EV, stake and market movement
    skip_counts = Counter()

    rows = filter_by_date(rows, args.date)

    filtered = []
    for r in rows:
        try:
            ev = float(r.get("ev_percent", 0) or 0)
        except Exception:
            ev = 0.0
        try:
            stake = float(r.get("stake", r.get("raw_kelly", 0)) or 0)
        except Exception:
            stake = 0.0

        base = r.get("baseline_consensus_prob")
        curr = r.get("market_prob") or r.get("consensus_prob")
        try:
            consensus_move = float(curr) - float(base)
        except Exception:
            consensus_move = float(r.get("consensus_move", 0) or 0)
        r["consensus_move"] = consensus_move

        required_move = float(r.get("required_move", 0) or 0)
        move_threshold = args.min_move if args.min_move is not None else required_move
        r["required_move"] = required_move

        movement_confirmed = bool(
            r.get("movement_confirmed", consensus_move >= required_move)
        )
        r["movement_confirmed"] = movement_confirmed

        if ev < 5:
            skip_counts["ev_below_5"] += 1
            continue
        if stake < 1.0:
            skip_counts["stake_below_1"] += 1
            continue
        if consensus_move < move_threshold:
            skip_counts["move_below_req"] += 1
            continue
        if not movement_confirmed:
            skip_counts["move_not_confirmed"] += 1
            continue
        if r.get("logged"):
            skip_counts["logged"] += 1
            continue

        filtered.append(r)
    rows = filtered

    seen = set()
    deduped = []
    for r in rows:
        key = (r.get("game_id"), r.get("market"), r.get("side"), r.get("book"))
        if key not in seen:
            seen.add(key)
            deduped.append(r)
    rows = deduped

    df = format_for_display(rows, include_movement=False)

    if "consensus_move" in df.columns:
        df["Move"] = (
            pd.to_numeric(df["consensus_move"], errors="coerce") * 100
        ).map("{:+.1f}%".format)
    if "required_move" in df.columns:
        df["Req"] = (
            pd.to_numeric(df["required_move"], errors="coerce") * 100
        ).map("{:.1f}%".format)

    if "consensus_move" in df.columns:
        df = df.sort_values("consensus_move", ascending=False)
    elif "ev_percent" in df.columns:
        df = df.sort_values("ev_percent", ascending=False)

    # Ensure a valid market class column exists before any role filtering
    if "market_class" not in df.columns:
        df["market_class"] = "main"
    if "Market Class" not in df.columns and "market_class" in df.columns:
        df["Market Class"] = df["market_class"]

    if skip_counts:
        logger.info("⏭️ Skip diagnostics: %s", dict(skip_counts))

    if "ev_percent" in df.columns:
        mask_ev = (df["ev_percent"] >= args.min_ev) & (df["ev_percent"] <= args.max_ev)
        if "logged" in df.columns and "hours_to_game" in df.columns:
            logged_mask = df["logged"] & (df["hours_to_game"] > 0)
            df = df[mask_ev | logged_mask]
        else:
            df = df[mask_ev]
        logger.info(
            "🧪 Dispatch filter: %d rows with %.1f ≤ EV%% ≤ %.1f",
            len(df),
            args.min_ev,
            args.max_ev,
        )

    print(f"🧪 Pre-stake filter row count: {df.shape[0]}")
    try:
        print(df[["market", "side", "ev_percent", "raw_kelly"]].head())
    except Exception:
        pass
    # Stake filtering (fallback from stake to raw_kelly)
    if "stake" in df.columns:
        stake_vals = pd.to_numeric(df["stake"], errors="coerce")
    elif "raw_kelly" in df.columns:
        stake_vals = pd.to_numeric(df["raw_kelly"], errors="coerce")
    else:
        stake_vals = pd.Series([0] * len(df))

    mask = (stake_vals >= 1.0) | df.get("is_prospective", False)
    if "logged" in df.columns and "hours_to_game" in df.columns:
        mask = mask | (df["logged"] & (df["hours_to_game"] > 0))
    df = df[mask]
    print(f"🧪 Post-stake filter row count: {df.shape[0]}")
    try:
        print(df[["market", "side", "ev_percent", "raw_kelly"]].head())
    except Exception:
        pass

    if all(c in df.columns for c in ["game_id", "market", "side", "book"]):
        df = df.drop_duplicates(subset=["game_id", "market", "side", "book"])

    if "logged" in df.columns and "Logged?" not in df.columns:
        df["Logged?"] = df["logged"].apply(lambda x: "YES" if bool(x) else "NO")
    elif "Logged?" not in df.columns:
        df["Logged?"] = ""
    if "logged" in df.columns and "Status" not in df.columns:
        df["Status"] = df["logged"].apply(lambda x: "🟢 LOGGED" if bool(x) else "")

    if "label" in df.columns and "Bet" in df.columns:
        df["Bet"] = df["label"] + " " + df["Bet"]
    if "sim_prob_display" in df.columns:
        df["Sim %"] = df["sim_prob_display"]
    if "mkt_prob_display" in df.columns:
        df["Mkt %"] = df["mkt_prob_display"]
    if "odds_display" in df.columns:
        df["Odds"] = df["odds_display"]
    if "fv_display" in df.columns:
        df["FV"] = df["fv_display"]

    if df.empty:
        if args.force_dispatch:
            logger.warning(
                "⚠️ Snapshot DataFrame is empty — forcing dispatch due to --force-dispatch"
            )
        else:
            logger.warning(
                "⚠️ Snapshot DataFrame is empty — nothing to dispatch."
            )
            return

    if "market" in df.columns and "Market" not in df.columns:
        df["Market"] = df["market"].astype(str)

    if "market_class" in df.columns and "Market Class" not in df.columns:
        df["Market Class"] = df["market_class"]

    if "Market" not in df.columns or "Market Class" not in df.columns:
        logger.warning(
            "⚠️ 'Market' or 'Market Class' column missing — skipping dispatch."
        )
        return

    columns = [
        "Date",
        "Time",
        "Matchup",
        "Market Class",
        "Market",
        "Bet",
        "Book",
        "Odds",
        "Sim %",
        "Mkt %",
        "Move",
        "Req",
        "FV",
        "EV",
        "Stake",
        "Logged?",
    ]
    # ✅ Ensure df_fv_all exists before referencing
    if "Status" in df.columns:
        columns.append("Status")
    missing = [c for c in columns if c not in df.columns]
    if missing:
        logger.warning(f"⚠️ Missing required columns: {missing} — skipping dispatch.")
        return
    df = df[columns]

    # ✅ Filter to only show rows where market probability increased
    if "Mkt %" in df.columns:
        inc_mask = df["Mkt %"].apply(is_market_prob_increasing)
        if "logged" in df.columns and "hours_to_game" in df.columns:
            logged_mask = df["logged"] & (df["hours_to_game"] > 0)
            df = df[inc_mask | logged_mask]
        else:
            df = df[inc_mask]

    # Prepare DataFrame copies for sending to the different Discord channels
    df_main = filter_main_lines(df.copy())
    df_main = filter_by_odds(
        df_main,
        MIN_NEGATIVE_ODDS,
        MAX_POSITIVE_ODDS,
    )

    # Filter snapshot twice: primary books and all allowed books
    df_fv_filtered = filter_by_books(df_main, FV_DROP_ALLOWED_BOOKS)
    logger.info(f"🧾 Snapshot rows for 'primary': {df_fv_filtered.shape[0]}")
    df_fv_all = filter_by_books(df_main, list(ALLOWED_BOOKS))
    logger.info(f"🧾 Snapshot rows for 'all': {df_fv_all.shape[0]}")

    if df_fv_filtered.empty and df_fv_all.empty and not args.force_dispatch:
        logger.info("⚠️ No qualifying FV Drop rows with market movement to display.")
        return

    columns = [
        "Date",
        "Time",
        "Matchup",
        "Market Class",
        "Market",
        "Bet",
        "Book",
        "Odds",
        "Sim %",
        "Mkt %",
        "Move",
        "Req",
        "FV",
        "EV",
        "Stake",
        "Logged?",
    ]
    if "Status" in df_fv_filtered.columns or "Status" in df_fv_all.columns:
        columns.append("Status")
    missing = [c for c in columns if c not in df_fv_filtered.columns]
    missing_all = [c for c in columns if c not in df_fv_all.columns]
    if missing:
        logger.warning(
            f"⚠️ Missing required columns: {missing} — skipping dispatch."
        )
        return
    if missing_all:
        logger.warning(
            f"⚠️ Missing required columns in all-books view: {missing_all} — skipping dispatch."
        )
        return
    df_fv_filtered = df_fv_filtered[columns]
    df_fv_all = df_fv_all[columns]

    if args.output_discord:
        fv_drop_webhook = os.getenv("DISCORD_FV_DROP_WEBHOOK_URL")
        fv_drop_all_webhook = os.getenv("DISCORD_FV_DROP_ALL_WEBHOOK_URL")

        if fv_drop_webhook:
            if not df_fv_filtered.empty or args.force_dispatch:
                title = "FV Drop (Primary)"
                send_bet_snapshot_to_discord(
                    df_fv_filtered,
                    title,
                    fv_drop_webhook,
                    force_dispatch=args.force_dispatch,
                )
            else:
                logger.warning("⚠️ No FV Drop rows for primary books")
        else:
            logger.error("❌ DISCORD_FV_DROP_WEBHOOK_URL not configured")

        if fv_drop_all_webhook:
            if not df_fv_all.empty or args.force_dispatch:
                title = "FV Drop (All Allowed Books)"
                send_bet_snapshot_to_discord(
                    df_fv_all,
                    title,
                    fv_drop_all_webhook,
                    force_dispatch=args.force_dispatch,
                )
            else:
                logger.warning("⚠️ No FV Drop rows for all allowed books")
        else:
            logger.error("❌ DISCORD_FV_DROP_ALL_WEBHOOK_URL not configured")
    else:
        print(df_fv_filtered.to_string(index=False))


if __name__ == "__main__":
    main()
