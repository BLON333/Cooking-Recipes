#!/usr/bin/env python
from core.config import DEBUG_MODE, VERBOSE_MODE
import os
import sys
from core.bootstrap import *  # noqa

"""Dispatch FV drop snapshot (market probability increases) from pending_bets.json."""

import json
from core.utils import parse_game_id
from theme_exposure_tracker import build_theme_key
import argparse
from typing import List
import re
import pandas as pd
from dotenv import load_dotenv

# Load environment variables from the project root .env file
load_dotenv()

from core.snapshot_core import format_for_display, send_bet_snapshot_to_discord
from core.logger import get_logger
from core.should_log_bet import MAX_POSITIVE_ODDS, MIN_NEGATIVE_ODDS
from core.book_helpers import parse_american_odds, filter_by_odds, ensure_side
from core.book_whitelist import ALLOWED_BOOKS
from core.pending_bets import load_pending_bets
from core.market_eval_tracker import build_tracker_key

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


def apply_baseline_annotations(rows: list, pending: dict) -> None:
    """Inject baseline consensus display strings into ``rows``."""
    baseline_map = {
        key: (bet.get("baseline_consensus_prob"))
        for key, bet in pending.items()
        if isinstance(bet, dict)
    }

    for r in rows:
        key = build_tracker_key(r.get("game_id"), r.get("market"), r.get("side"))
        baseline = baseline_map.get(key)
        try:
            curr = float(r.get("consensus_prob", r.get("market_prob")))
        except Exception:
            curr = None
        try:
            base_val = float(baseline) if baseline is not None else None
        except Exception:
            base_val = None

        if base_val is not None and curr is not None:
            r["mkt_prob_display"] = f"{base_val * 100:.1f}% → {curr * 100:.1f}%"
        elif curr is not None:
            r["mkt_prob_display"] = f"{curr * 100:.1f}%"


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
    args = parser.parse_args()

    # Clamp EV range to 5%-20%
    args.min_ev = max(5.0, args.min_ev)
    args.max_ev = min(20.0, args.max_ev)
    if args.min_ev > args.max_ev:
        args.max_ev = args.min_ev

    rows = load_pending_rows()
    if not rows:
        logger.warning(
            "⚠️ pending_bets.json empty or not found – skipping dispatch"
        )
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

    # ✅ No role/movement filter — allow full snapshot set
    rows = filter_by_date(rows, args.date)

    rows = [r for r in rows if args.min_ev <= r.get("ev_percent", 0) <= args.max_ev]

    filtered = []
    for r in rows:
        stake_val = r.get("total_stake", r.get("stake") or r.get("snapshot_stake") or 0)
        if stake_val < 1.0 and not r.get("is_prospective"):
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

    # Lookup baseline consensus probabilities from pending_bets.json
    pending = load_pending_bets()
    apply_baseline_annotations(rows, pending)

    logger.info(
        "🧪 Dispatch filter: %d rows with %.1f ≤ EV%% ≤ %.1f",
        len(rows),
        args.min_ev,
        args.max_ev,
    )

    df = format_for_display(rows, include_movement=False)
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

    # ✅ Filter to only show rows where market probability increased
    if "Mkt %" in df.columns:
        df = df[df["Mkt %"].apply(is_market_prob_increasing)]

    # Prepare DataFrame copies for sending to the different Discord channels
    df_main = filter_main_lines(df.copy())
    df_main = filter_by_odds(
        df_main,
        MIN_NEGATIVE_ODDS,
        MAX_POSITIVE_ODDS,
    )

    # Filter snapshot twice: primary books and all allowed books
    df_fv_filtered = filter_by_books(df_main, FV_DROP_ALLOWED_BOOKS)
    df_fv_all = filter_by_books(df_main, list(ALLOWED_BOOKS))

    if df_fv_filtered.empty and df_fv_all.empty:
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
        "FV",
        "EV",
        "Stake",
        "Logged?",
    ]
    columns = [c for c in columns if c in df_fv_filtered.columns]
    df_fv_filtered = df_fv_filtered[columns]
    df_fv_all = df_fv_all[columns]

    if args.output_discord:
        fv_drop_webhook = os.getenv("DISCORD_FV_DROP_WEBHOOK_URL")
        fv_drop_all_webhook = os.getenv("DISCORD_FV_DROP_ALL_WEBHOOK_URL")

        if fv_drop_webhook:
            if not df_fv_filtered.empty:
                send_bet_snapshot_to_discord(
                    df_fv_filtered,
                    "FV Drop (Primary)",
                    fv_drop_webhook,
                )
            else:
                logger.warning("⚠️ No FV Drop rows for primary books")
        else:
            logger.error("❌ DISCORD_FV_DROP_WEBHOOK_URL not configured")

        if fv_drop_all_webhook:
            if not df_fv_all.empty:
                send_bet_snapshot_to_discord(
                    df_fv_all,
                    "FV Drop (All Allowed Books)",
                    fv_drop_all_webhook,
                )
            else:
                logger.warning("⚠️ No FV Drop rows for all allowed books")
        else:
            logger.error("❌ DISCORD_FV_DROP_ALL_WEBHOOK_URL not configured")
    else:
        print(df_fv_filtered.to_string(index=False))


if __name__ == "__main__":
    main()
