#!/usr/bin/env python
from core.config import DEBUG_MODE, VERBOSE_MODE
import os
from core.bootstrap import *  # noqa

"""Dispatch personal-book snapshot using the latest snapshot file."""
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
from core.book_whitelist import ALLOWED_BOOKS
from core.book_helpers import ensure_side
from core.snapshot_tracker_loader import find_latest_market_snapshot_path

logger = get_logger(__name__)

# Optional debug log to verify environment variables are loaded
logger.debug("✅ Loaded webhook: %s", os.getenv("PERSONAL_DISCORD_WEBHOOK_URL"))

PERSONAL_WEBHOOK_URL = os.getenv(
    "PERSONAL_DISCORD_WEBHOOK_URL",
    "https://discord.com/api/webhooks/1368408687559053332/2uhUud0fgdonV0xdIDorXX02HGQ1AWsEO_lQHMDqWLh-4THpMEe3mXb7u88JSvssSRtM",
)


def load_latest_snapshot_rows() -> list:
    """Return snapshot rows from the most recent snapshot file."""
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
    """Return DataFrame filtered to the specified sportsbook keys."""
    if not books or "Book" not in df.columns:
        return df
    clean_books = [b.strip() for b in books if b.strip()]
    if not clean_books:
        return df
    return df[df["Book"].isin(clean_books)]


def main() -> None:
    parser = argparse.ArgumentParser(description="Dispatch personal-book snapshot")
    parser.add_argument("--date", default=None, help="Filter by game date")
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

    # Clamp EV range to 5%-20%
    args.min_ev = max(5.0, args.min_ev)
    args.max_ev = min(20.0, args.max_ev)
    if args.min_ev > args.max_ev:
        args.max_ev = args.min_ev

    rows = load_latest_snapshot_rows()
    if not rows:
        logger.warning("⚠️ No snapshot rows found – skipping dispatch")
        return

    rows = [r for r in rows if "personal" in (r.get("snapshot_roles") or [])]
    logger.info("🧾 Snapshot rows for role='personal': %d", len(rows))
    if not rows:
        logger.warning("⚠️ No snapshot rows for role='personal' — skipping dispatch")
        return

    for r in rows:
        if "book" not in r and "best_book" in r:
            r["book"] = r["best_book"]

    rows = filter_by_date(rows, args.date)

    df = format_for_display(rows, include_movement=True)

    # Ensure a valid market class column exists before any role filtering
    if "market_class" not in df.columns:
        df["market_class"] = "main"
    if "Market Class" not in df.columns and "market_class" in df.columns:
        df["Market Class"] = df["market_class"]

    # Diagnostic summary of potential skip reasons
    skip_counts = Counter()
    for _, row in df.iterrows():
        if row.get("logged"):
            skip_counts["logged"] += 1
        elif float(row.get("ev_percent", 0) or 0) < 5:
            skip_counts["ev_below_5"] += 1
        elif float(row.get("raw_kelly", 0) or 0) < 1:
            skip_counts["kelly_below_1"] += 1
        elif row.get("skip_reason"):
            skip_counts["skipped_unknown"] += 1
    if skip_counts:
        logger.info("⏭️ Skip diagnostics: %s", dict(skip_counts))

    if "ev_percent" in df.columns:
        mask_ev = (df["ev_percent"] >= args.min_ev) & (df["ev_percent"] <= args.max_ev)
        if "logged" in df.columns and "hours_to_game" in df.columns:
            logged_mask = df["logged"] & (df["hours_to_game"] > 0)
            df = df[mask_ev | logged_mask]
        else:
            df = df[mask_ev]

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

    logger.info(
        "🧪 Dispatch filter: %d rows with %.1f ≤ EV%% ≤ %.1f",
        len(df),
        args.min_ev,
        args.max_ev,
    )

    if "label" in df.columns and "Bet" in df.columns:
        df["Bet"] = df["label"] + " " + df["Bet"]
    if "label" in df.columns and "Bet" in df.columns:
        df["Bet"] = df["label"] + " " + df["Bet"]
    allowed_books = list(ALLOWED_BOOKS)
    df = filter_by_books(df, allowed_books)
    if "sim_prob_display" in df.columns:
        df["Sim %"] = df["sim_prob_display"]
    if "mkt_prob_display" in df.columns:
        df["Mkt %"] = df["mkt_prob_display"]
    if "odds_display" in df.columns:
        df["Odds"] = df["odds_display"]
    if "fv_display" in df.columns:
        df["FV"] = df["fv_display"]
    if "logged" in df.columns and "Logged?" not in df.columns:
        df["Logged?"] = df["logged"].apply(lambda x: "✅" if bool(x) else "")
    elif "Logged?" not in df.columns:
        df["Logged?"] = ""
    if "logged" in df.columns and "Status" not in df.columns:
        df["Status"] = df["logged"].apply(lambda x: "🟢 LOGGED" if bool(x) else "")
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
            "⚠️ 'Market' or 'Market Class' column missing — cannot dispatch personal main/alt splits."
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
    if "Status" in df.columns:
        columns.append("Status")
    missing = [c for c in columns if c not in df.columns]
    if missing:
        logger.warning(
            f"⚠️ Missing required columns: {missing} — skipping dispatch."
        )
        return
    df = df[columns]

    if args.output_discord:
        webhook = PERSONAL_WEBHOOK_URL
        if not webhook:
            logger.error("❌ PERSONAL_DISCORD_WEBHOOK_URL not configured")
            return
        logger.info("📡 Dispatching unified personal snapshot (%s rows)", df.shape[0])
        title = "Personal Snapshot"
        send_bet_snapshot_to_discord(
            df,
            title,
            webhook,
            force_dispatch=args.force_dispatch,
        )
    else:
        print(df.to_string(index=False))


if __name__ == "__main__":
    main()
