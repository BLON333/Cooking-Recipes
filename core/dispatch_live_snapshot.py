#!/usr/bin/env python
from core.config import DEBUG_MODE, VERBOSE_MODE
import os
from core.bootstrap import *  # noqa

"""Dispatch live snapshot using the latest snapshot file."""
from core.utils import parse_game_id, safe_load_json

import argparse
from dotenv import load_dotenv

# Load environment variables from the project root .env file
load_dotenv()

from core.snapshot_core import format_for_display, send_bet_snapshot_to_discord
from core.book_helpers import ensure_side
import pandas as pd
from core.snapshot_tracker_loader import find_latest_market_snapshot_path
from core.logger import get_logger
from collections import Counter

logger = get_logger(__name__)

# Optional debug log to verify environment variables are loaded
logger.debug("✅ Loaded webhook: %s", os.getenv("DISCORD_SPREADS_WEBHOOK_URL"))


def load_latest_snapshot_rows() -> list:
    """Return snapshot rows from the most recent snapshot file."""
    path = find_latest_market_snapshot_path("backtest")
    rows = safe_load_json(path) if path else []
    logger.info("📊 Loaded %d snapshot rows from %s", len(rows), path)
    filtered = []
    for r in rows:
        ensure_side(r)

        try:
            stake = float(r.get("stake", r.get("raw_kelly", 0)) or 0)
        except Exception:
            stake = 0.0
        try:
            ev = float(r.get("ev_percent", 0) or 0)
        except Exception:
            ev = 0.0

        if stake < 1.0 and ev < 5.0 and not r.get("snapshot_roles"):
            continue

        filtered.append(r)

    return filtered


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

    # Clamp EV range to sensible bounds
    args.min_ev = max(0.0, args.min_ev)
    args.max_ev = min(20.0, args.max_ev)
    if args.min_ev > args.max_ev:
        args.max_ev = args.min_ev

    rows = load_latest_snapshot_rows()
    if not rows:
        logger.warning("⚠️ No snapshot rows found – skipping dispatch")
        return

    rows = [r for r in rows if "live" in (r.get("snapshot_roles") or [])]
    logger.info("🧾 Snapshot rows for role='live': %d", len(rows))
    if not rows:
        logger.warning("⚠️ No snapshot rows for role='live' — skipping dispatch")
        return

    for r in rows:
        if "book" not in r and "best_book" in r:
            r["book"] = r["best_book"]

    rows = filter_by_date(rows, args.date)


    df = format_for_display(rows, include_movement=True)

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
            if not args.force_dispatch:
                df = df[mask_ev | logged_mask]
            else:
                df = df[mask_ev | logged_mask]
        else:
            if not args.force_dispatch:
                df = df[mask_ev]
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
    if "Status" in df.columns:
        columns.append("Status")
    missing = [c for c in columns if c not in df.columns]
    if missing:
        logger.warning(f"⚠️ Missing required columns: {missing} — skipping dispatch.")
        return
    df = df[columns]

    if args.output_discord:
        unified_hook = os.getenv("DISCORD_LIVE_SNAPSHOT_WEBHOOK_URL")
        if unified_hook:
            logger.info("📡 Dispatching unified live snapshot (%s rows)", df.shape[0])
            title = "Live Snapshot"
            send_bet_snapshot_to_discord(
                df,
                title,
                unified_hook,
                force_dispatch=args.force_dispatch,
            )
            return

        role_hooks = {
            "h2h": os.getenv("DISCORD_H2H_WEBHOOK_URL"),
            "spreads": os.getenv("DISCORD_SPREADS_WEBHOOK_URL"),
            "totals": os.getenv("DISCORD_TOTALS_WEBHOOK_URL"),
        }
        if not any(role_hooks.values()):
            logger.error("❌ No Discord webhook configured for live snapshot")
            return

        role_counts = {
            label: df[df["Market"].str.lower().str.startswith(label, na=False)].shape[0]
            for label in role_hooks
        }
        logger.info("🧮 Role row counts: %s", role_counts)

        for label, hook in role_hooks.items():
            subset = df[df["Market"].str.lower().str.startswith(label, na=False)]
            logger.info(f"🧾 Snapshot rows for role='{label}': {subset.shape[0]}")
            if subset.empty:
                if args.force_dispatch:
                    logger.warning(
                        f"⚠️ No snapshot rows for role='{label}' — forcing dispatch"
                    )
                else:
                    logger.warning(
                        f"⚠️ No snapshot rows for role='{label}' — skipping dispatch."
                    )
                    continue
            if not hook:
                logger.warning(f"⚠️ Discord webhook for role='{label}' not configured")
                continue
            logger.info(
                "📡 Dispatching %s live snapshot (%s rows)", label, subset.shape[0]
            )
            title = "Live Snapshot"
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
