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
from collections import Counter
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
    skip_counts = Counter()

    rows = filter_by_date(rows, args.date)

    filtered = []
    for r in rows:
        if r.get("logged"):
            skip_counts["logged"] += 1
            continue
        try:
            ev = float(r.get("ev_percent", 0) or 0)
        except Exception:
            ev = 0.0
        if ev < 5:
            skip_counts["ev_below_5"] += 1
            continue
        try:
            rk = float(r.get("raw_kelly", 0) or 0)
        except Exception:
            rk = 0.0
        if rk < 1:
            skip_counts["kelly_below_1"] += 1
            continue
        if r.get("skip_reason"):
            skip_counts["skipped_unknown"] += 1
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

    df = format_for_display(rows, include_movement=False)

    if skip_counts:
        logger.info("⏭️ Skip diagnostics: %s", dict(skip_counts))

    if "ev_percent" in df.columns:
        df = df[(df["ev_percent"] >= args.min_ev) & (df["ev_percent"] <= args.max_ev)]
        logger.info(
            "🧪 Dispatch filter: %d rows with %.1f ≤ EV%% ≤ %.1f",
            len(df),
            args.min_ev,
            args.max_ev,
        )

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

    if "logged" in df.columns and "Logged?" not in df.columns:
        df["Logged?"] = df["logged"].apply(lambda x: "YES" if bool(x) else "NO")
    elif "Logged?" not in df.columns:
        df["Logged?"] = ""

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
        "FV",
        "EV",
        "Stake",
        "Logged?",
    ]
    missing = [c for c in columns if c not in df_fv_filtered.columns]
    if missing:
        logger.warning(
            f"⚠️ Missing required columns: {missing} — skipping dispatch."
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
