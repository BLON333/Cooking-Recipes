#!/usr/bin/env python
from core import config
import os
import sys
from core.bootstrap import *  # noqa

"""Dispatch best-book snapshot from pending_bets.json."""

import json
from core.utils import parse_game_id
from core.theme_exposure_tracker import build_theme_key
import argparse
from dotenv import load_dotenv

# Load environment variables from the project root .env file
load_dotenv()

from core.snapshot_core import format_for_display, send_bet_snapshot_to_discord
from core.book_helpers import ensure_side
import pandas as pd
from core.pending_bets import load_pending_bets
from core.logger import get_logger
from collections import Counter

logger = get_logger(__name__)

# Optional debug log to verify environment variables are loaded
logger.debug("✅ Loaded webhook: %s", os.getenv("DISCORD_BEST_BOOK_MAIN_WEBHOOK_URL"))


def load_pending_rows() -> list:
    """Return pending bets loaded from disk."""
    pending = load_pending_bets()
    rows = list(pending.values())
    logger.info("📊 Rendering snapshot from %d entries in pending_bets.json", len(rows))
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
        "--force-dispatch",
        action="store_true",
        help="Force image snapshot to Discord even if empty",
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
    logger.info("🧾 Pending rows for role='best_book': %d", len(rows))
    if not rows:
        logger.warning("⚠️ No pending rows for role='best_book' — skipping dispatch")
        return

    rows = filter_by_date(rows, args.date)

    skip_counts = Counter()
    filtered = []
    for r in rows:
        if r.get("logged"):
            skip_counts["logged"] += 1
            continue
        try:
            ev = float(r.get("ev_percent", 0))
        except Exception:
            ev = 0.0
        try:
            rk = float(r.get("raw_kelly", 0))
        except Exception:
            rk = 0.0
        if ev < 5:
            skip_counts["ev_below_5"] += 1
            continue
        if rk < 1:
            skip_counts["kelly_below_1"] += 1
            continue
        filtered.append(r)
    if skip_counts:
        logger.info("⏭️ Skip diagnostics: %s", dict(skip_counts))

    df = format_for_display(filtered, include_movement=True)

    # Ensure a valid market class column exists before any role filtering
    if "market_class" not in df.columns:
        df["market_class"] = "main"
    if "Market Class" not in df.columns and "market_class" in df.columns:
        df["Market Class"] = df["market_class"]

    if "ev_percent" in df.columns:
        df = df[(df["ev_percent"] >= args.min_ev) & (df["ev_percent"] <= args.max_ev)]
        logger.info(
            "🧪 Dispatch filter: %d rows with %.1f ≤ EV%% ≤ %.1f",
            len(df),
            args.min_ev,
            args.max_ev,
        )

    df["__stake_check"] = 0.0
    if "total_stake" in df.columns:
        df["__stake_check"] = pd.to_numeric(df["total_stake"], errors="coerce")
    elif "stake" in df.columns:
        df["__stake_check"] = pd.to_numeric(df["stake"], errors="coerce")
    elif "snapshot_stake" in df.columns:
        df["__stake_check"] = pd.to_numeric(df["snapshot_stake"], errors="coerce")

    if "is_prospective" in df.columns:
        df = df[(df["__stake_check"] >= 1.0) | df["is_prospective"]]
    else:
        df = df[df["__stake_check"] >= 1.0]

    df.drop(columns=["__stake_check"], inplace=True)

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
    if "logged" in df.columns and "Logged?" not in df.columns:
        df["Logged?"] = df["logged"].apply(lambda x: "YES" if bool(x) else "NO")
    elif "Logged?" not in df.columns:
        df["Logged?"] = ""
    if df.empty and not args.force_dispatch:
        logger.warning("⚠️ Snapshot DataFrame is empty — nothing to dispatch.")
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
        "FV",
        "EV",
        "Stake",
        "Logged?",
    ]
    missing = [c for c in columns if c not in df.columns]
    if missing:
        logger.warning(f"⚠️ Missing required columns: {missing} — skipping dispatch.")
        return
    df = df[columns]

    if args.output_discord:
        main_hook = os.getenv("DISCORD_BEST_BOOK_MAIN_WEBHOOK_URL")
        alt_hook = os.getenv("DISCORD_BEST_BOOK_ALT_WEBHOOK_URL")

        if not any([main_hook, alt_hook]):
            logger.warning("❌ No Discord webhook configured for best-book snapshots.")
            return

        for label, hook in [("main", main_hook), ("alt", alt_hook)]:
            subset = df[df["Market Class"].str.lower() == label]
            logger.info(f"🧾 Snapshot rows for role='{label}': {subset.shape[0]}")
            if subset.empty and not args.force_dispatch:
                logger.warning(
                    f"⚠️ No snapshot rows for role='{label}' — skipping dispatch."
                )
                continue
            if not hook:
                logger.warning(f"⚠️ Discord webhook for role='{label}' not configured")
                continue
            logger.info(
                "📡 Dispatching %s best-book snapshot (%s rows)",
                label,
                subset.shape[0],
            )
            title = "Best Book Snapshot"
            send_bet_snapshot_to_discord(
                subset,
                title,
                hook,
                force_dispatch=args.force_dispatch,
            )
    else:
        print(df.to_string(index=False))


if __name__ == "__main__":
    main()
