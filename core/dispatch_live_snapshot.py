#!/usr/bin/env python
from core.config import DEBUG_MODE, VERBOSE_MODE
import os
import sys
from core.bootstrap import *  # noqa

"""Dispatch live snapshot from pending_bets.json."""

import json
from core.utils import parse_game_id
from theme_exposure_tracker import build_theme_key
import argparse
from dotenv import load_dotenv

# Load environment variables from the project root .env file
load_dotenv()

from core.snapshot_core import format_for_display, send_bet_snapshot_to_discord
from core.book_helpers import filter_snapshot_rows, ensure_side
from core.pending_bets import load_pending_bets
from core.logger import get_logger

logger = get_logger(__name__)

# Optional debug log to verify environment variables are loaded
logger.debug("✅ Loaded webhook: %s", os.getenv("DISCORD_SPREADS_WEBHOOK_URL"))


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
    parser = argparse.ArgumentParser(description="Dispatch live snapshot")
    parser.add_argument("--date", default=None, help="Filter by game date")
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

    # Clamp EV range to sensible bounds
    args.min_ev = max(0.0, args.min_ev)
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

    rows = filter_by_date(rows, args.date)

    rows = filter_snapshot_rows(rows, min_ev=args.min_ev)
    logger.info("🧪 Dispatch filter: %d rows (min EV %.1f%%)", len(rows), args.min_ev)

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

    df = format_for_display(rows, include_movement=True)
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

    if "market" in df.columns and "Market" not in df.columns:
        df["Market"] = df["market"]

    if "Market" not in df.columns:
        logger.warning("⚠️ 'Market' column missing — skipping live snapshot dispatch.")
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
    columns = [c for c in columns if c in df.columns]
    df = df[columns]

    if args.output_discord:
        webhook_map = {
            "h2h": os.getenv("DISCORD_H2H_WEBHOOK_URL"),
            "spreads": os.getenv("DISCORD_SPREADS_WEBHOOK_URL"),
            "totals": os.getenv("DISCORD_TOTALS_WEBHOOK_URL"),
        }
        for label in ["h2h", "spreads", "totals"]:
            subset = df[df["Market"].str.lower().str.startswith(label, na=False)]
            webhook = webhook_map.get(label)
            logger.info(
                "📡 Evaluating snapshot for: %s → %s rows", label, subset.shape[0]
            )
            if subset.empty:
                logger.warning("⚠️ No bets for %s", label)
                continue
            if webhook:
                send_bet_snapshot_to_discord(subset, label, webhook)
            else:
                logger.warning("❌ Discord webhook not configured for %s", label)
    else:
        print(df.to_string(index=False))


if __name__ == "__main__":
    main()
