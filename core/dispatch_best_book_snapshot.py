#!/usr/bin/env python
from core import config
import os
import sys
from core.bootstrap import *  # noqa

"""Dispatch best-book snapshot from pending_bets.json."""

import json
from core.utils import parse_game_id
from theme_exposure_tracker import build_theme_key
import argparse
from dotenv import load_dotenv

# Load environment variables from the project root .env file
load_dotenv()

from core.snapshot_core import format_for_display, send_bet_snapshot_to_discord
from core.book_helpers import ensure_side
import pandas as pd
from core.pending_bets import load_pending_bets
from core.logger import get_logger

logger = get_logger(__name__)

# Optional debug log to verify environment variables are loaded
logger.debug("✅ Loaded webhook: %s", os.getenv("DISCORD_BEST_BOOK_MAIN_WEBHOOK_URL"))


def load_pending_rows() -> list:
    """Return pending bets loaded from disk."""
    pending = load_pending_bets()
    rows = list(pending.values())
    logger.info(
        "📊 Rendering snapshot from %d entries in pending_bets.json", len(rows)
    )
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


def main() -> None:
    parser = argparse.ArgumentParser(description="Dispatch best-book snapshot")
    parser.add_argument("--date", default=None, help="Filter by game date")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    parser.add_argument("--output-discord", action="store_true")
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
    args = parser.parse_args()
    config.DEBUG_MODE = args.debug
    config.VERBOSE_MODE = args.verbose
    if config.DEBUG_MODE:
        print("🧪 DEBUG_MODE ENABLED — Verbose output activated")

    # Clamp EV range to 5%-20%
    args.min_ev = max(5.0, args.min_ev)
    args.max_ev = min(20.0, args.max_ev)
    if args.min_ev > args.max_ev:
        args.max_ev = args.min_ev

    rows = load_pending_rows()
    if not rows:
            logger.warning("⚠️ pending_bets.json empty or not found – skipping dispatch")
            return

    try:
        with open("logs/theme_exposure.json") as f:
            theme_stakes = json.load(f)
    except FileNotFoundError:
        theme_stakes = {}

    for r in rows:
        theme_key = build_theme_key(r)
        r["total_stake"] = theme_stakes.get(theme_key, 0.0)
        if "book" not in r and "best_book" in r:
            r["book"] = r["best_book"]

    rows = [r for r in rows if "best_book" in r.get("snapshot_roles", [])]
    rows = filter_by_date(rows, args.date)

    df = format_for_display(rows, include_movement=True)

    if "ev_percent" in df.columns:
        df = df[(df["ev_percent"] >= args.min_ev) & (df["ev_percent"] <= args.max_ev)]
        logger.info("🧪 Dispatch filter: %d rows with %.1f ≤ EV%% ≤ %.1f", len(df), args.min_ev, args.max_ev)

    if "total_stake" in df.columns:
        stake_vals = pd.to_numeric(df["total_stake"], errors="coerce")
    elif "stake" in df.columns:
        stake_vals = pd.to_numeric(df["stake"], errors="coerce")
    elif "snapshot_stake" in df.columns:
        stake_vals = pd.to_numeric(df["snapshot_stake"], errors="coerce")
    else:
        stake_vals = pd.Series([0] * len(df))
    if "is_prospective" in df.columns:
        mask = (stake_vals >= 1.0) | df["is_prospective"]
    else:
        mask = stake_vals >= 1.0
    df = df[mask]

    if all(c in df.columns for c in ["game_id", "market", "side", "book"]):
        df = df.drop_duplicates(subset=["game_id", "market", "side", "book"])

    df = df.reset_index(drop=True)
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
            logger.warning("⚠️ Snapshot DataFrame is empty — nothing to dispatch.")
            return

    if "market" in df.columns:
        df["Market"] = df["market"].astype(str)

    if "Market" not in df.columns:
            logger.warning("⚠️ 'Market' column missing — skipping dispatch.")
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
        "FV",
        "EV",
        "Stake",
        "Logged?",
    ]
    missing = [c for c in columns if c not in df.columns]
    if missing:
            logger.warning(
                        f"⚠️ Missing required columns: {missing} — skipping dispatch."
        )
            return
    df = df[columns]

    if args.output_discord:
        webhook = os.getenv("DISCORD_BEST_BOOK_MAIN_WEBHOOK_URL") or os.getenv("DISCORD_BEST_BOOK_ALT_WEBHOOK_URL")
        if not webhook:
            logger.warning("❌ No Discord webhook configured for best-book snapshots.")
            return
        logger.info("📡 Dispatching unified best-book snapshot (%s rows)", df.shape[0])
        send_bet_snapshot_to_discord(df, "Best Book Snapshot", webhook)
    else:
        print(df.to_string(index=False))


if __name__ == "__main__":
    main()
