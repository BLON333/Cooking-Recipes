# Shared snapshot utilities for generator scripts
from core.config import DEBUG_MODE, VERBOSE_MODE
import os
import json
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from typing import List, Dict, Tuple
from typing import Optional
import io

import pandas as pd

import requests
from core.utils import post_with_retries, safe_load_json
import glob

try:
    import dataframe_image as dfi
except Exception:  # pragma: no cover - optional dep
    dfi = None

from core.logger import get_logger
from core.role_assignment import (
    DISCORD_WEBHOOK_BY_ROLE,
    BEST_BOOK_MAIN,
    BEST_BOOK_ALT,
    FV_DROP,
)

logger = get_logger(__name__)

from core.utils import (
    convert_full_team_spread_to_odds_key,
    normalize_to_abbreviation,
    get_market_entry_with_alternate_fallback,
    normalize_label_for_odds,
    get_segment_label,
    to_eastern,
    now_eastern,
    parse_game_id,
    normalize_game_id,
    fuzzy_match_game_id,
)
from core.odds_normalizer import canonical_game_id
from core.time_utils import compute_hours_to_game
from core.dispatch_clv_snapshot import parse_start_time
from core.should_log_bet import get_theme, get_theme_key
from core.market_pricer import (
    to_american_odds,
    kelly_fraction,
    calculate_ev_from_prob,
    decimal_odds,
    extract_best_book,
)
from core.confirmation_utils import required_market_move
from core.scaling_utils import blend_prob
from core.consensus_pricer import calculate_consensus_prob
from core.market_movement_tracker import track_and_update_market_movement
from core.snapshot_tracker_loader import find_latest_market_snapshot_path
import copy

from core.book_helpers import ensure_consensus_books

# Build keys for tracker dictionaries and snapshot rows
def build_key(game_id: str, market: str, side: str) -> str:
    """Return ``game_id:market:side`` without additional normalization."""
    return f"{game_id}:{market}:{side}"


def load_snapshot_tracker(directory: str = "backtest") -> dict:
    path = find_latest_market_snapshot_path(directory)
    if not path or not os.path.exists(path):
        return {}
    data = safe_load_json(path)
    tracker = {}
    rows = data if isinstance(data, list) else data.values() if isinstance(data, dict) else []
    for r in rows:
        canon = canonical_game_id(str(r.get("game_id")))
        key = build_key(canon, r.get("market"), r.get("side"))
        tracker[key] = r
    return tracker


MARKET_EVAL_TRACKER = load_snapshot_tracker()
MARKET_EVAL_TRACKER_BEFORE_UPDATE = copy.deepcopy(MARKET_EVAL_TRACKER)

# === Console Output Controls ===
MOVEMENT_LOG_LIMIT = 5
movement_log_count = 0


def load_latest_snapshot(folder: str = "backtest") -> list:
    """Return rows from the most recent ``market_snapshot_*.json`` in ``folder``."""
    pattern = os.path.join(folder, "market_snapshot_*.json")
    files = glob.glob(pattern)
    if not files:
        logger.warning("⚠️ No snapshot files found in %s", folder)
        return []

    latest = max(files, key=os.path.getmtime)
    data = safe_load_json(latest)
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return list(data.values())
    return []


def load_market_snapshot(path: str | None) -> list:
    """Return snapshot rows loaded from ``path``."""
    rows = safe_load_json(path) if path else []
    logger.info("\U0001F4CA Loaded %d snapshot rows from %s", len(rows), path)
    return rows


def should_log_movement() -> bool:
    global movement_log_count
    movement_log_count += 1
    if movement_log_count <= MOVEMENT_LOG_LIMIT:
        return True
    if movement_log_count == MOVEMENT_LOG_LIMIT + 1:
        print("🧠 ... (truncated additional movement logs)")
    return False


def warn_missing_baselines(rows: list) -> None:
    """Print diagnostics for rows missing ``baseline_consensus_prob``."""
    missing = [r for r in rows if r.get("baseline_consensus_prob") is None]
    for r in missing:
        key = f"{r.get('game_id')}:{r.get('market')}:{r.get('side')}"
        print(
            f"⚠️ MISSING BASELINE → {key} | consensus_prob = {r.get('consensus_prob')}"
        )
    if missing:
        print(f"⚠️ {len(missing)} snapshot rows missing baseline_consensus_prob")
        try:
            os.makedirs("logs/debug", exist_ok=True)
            with open("logs/debug/missing_baseline_keys.txt", "w") as f:
                for row in missing:
                    f.write(
                        f"{row.get('game_id')}:{row.get('market')}:{row.get('side')}\n"
                    )
        except Exception:
            pass


def ensure_baseline_consensus_prob(rows: list, tracker: dict | None = None) -> None:
    """Populate ``baseline_consensus_prob`` when missing."""
    # baseline_consensus_prob = original implied probability when bet first appeared; never overwritten
    if tracker is None:
        tracker = MARKET_EVAL_TRACKER_BEFORE_UPDATE

    for row in rows:
        if row.get("baseline_consensus_prob") is not None:
            continue

        canon_gid = canonical_game_id(str(row.get('game_id')))
        key = f"{canon_gid}:{row.get('market')}:{row.get('side')}"

        baseline = None
        if tracker:
            tracker_entry = tracker.get(key)
            if tracker_entry:
                baseline = tracker_entry.get("baseline_consensus_prob")

        if baseline is None:
            prior_row = row.get("_prior_snapshot")
            if prior_row:
                baseline = prior_row.get("baseline_consensus_prob")

        if baseline is None:
            baseline = row.get("consensus_prob")

        row["baseline_consensus_prob"] = baseline


def format_percentage(val: Optional[float]) -> str:
    """Return a percentage string like ``41.2%`` or ``–``."""
    try:
        return f"{val * 100:.1f}%" if val is not None else "–"
    except Exception:
        return "–"


def format_odds(val: Optional[float]) -> str:
    """Return American odds like ``+215`` or ``–``."""
    if val is None:
        return "–"
    try:
        return f"{int(float(val)):+}"
    except Exception:
        return str(val)


def format_display(
    curr: Optional[float], prior: Optional[float], movement: str, mode: str = "percent"
) -> str:
    """Return ``X → Y`` string for display based on movement."""
    fmt = format_percentage if mode == "percent" else format_odds
    if movement == "same" or prior is None:
        return fmt(curr)
    return f"{fmt(prior)} → {fmt(curr)}"


def annotate_display_deltas(entry: Dict, prior: Optional[Dict]) -> None:
    """Populate *_display fields on ``entry`` using the provided prior data."""

    def fmt_odds(val: Optional[float]) -> str:
        if val is None:
            return "N/A"
        try:
            return f"{val:+}" if isinstance(val, (int, float)) else str(val)
        except Exception:
            return str(val)

    def fmt_percent(val: Optional[float]) -> str:
        if val is None:
            return "N/A"
        try:
            return f"{val:+.1f}%"
        except Exception:
            return str(val)

    def fmt_prob(val: Optional[float]) -> str:
        if val is None:
            return "N/A"
        try:
            return f"{val * 100:.1f}%"
        except Exception:
            return str(val)

    def fmt_fv(val: Optional[float]) -> str:
        if val is None:
            return "N/A"
        try:
            return f"{round(val)}"
        except Exception:
            return str(val)

    def fmt_stake(val: Optional[float]) -> str:
        if val is None:
            return "N/A"
        out = f"{val:.2f}u"
        total = entry.get("cumulative_stake")
        try:
            if total is not None and float(total) != float(val):
                out = f"{val:.2f}u ({float(total):.2f}u)"
        except Exception:
            pass
        return out

    field_map = {
        "market_odds": ("odds_display", fmt_odds),
        "ev_percent": ("ev_display", fmt_percent),
        "market_prob": ("mkt_prob_display", fmt_prob),
        "sim_prob": ("sim_prob_display", fmt_prob),
        "blended_fv": ("fv_display", fmt_fv),
    }

    skip_deltas_for = {"ev_percent"}

    movement_fields = {
        "sim_prob": ("sim_movement", "percent"),
        "market_prob": ("mkt_movement", "percent"),
        "blended_fv": ("fv_movement", "odds"),
    }

    for field, (disp_key, fmt) in field_map.items():
        curr = entry.get(field)
        if field == "market_odds":
            prior_val = entry.get("prev_market_odds")
            movement = entry.get("odds_movement", "same")
        else:
            if field == "market_prob":
                prior_val = entry.get("baseline_consensus_prob")
                movement = entry.get("mkt_movement", "same")
            else:
                prior_val = entry.get(f"prev_{field}") or (
                    prior.get(field) if prior else None
                )
                movement = entry.get(
                    movement_fields.get(field, ("", ""))[0],
                    "same",
                )

        if field in movement_fields or field == "market_odds":
            if prior_val is not None and movement != "same":
                entry[disp_key] = f"{fmt(prior_val)} → {fmt(curr)}"
            else:
                entry[disp_key] = fmt(curr)
            continue

        if field in skip_deltas_for:
            entry[disp_key] = fmt(curr)
            continue

        if prior_val is not None and curr != prior_val:
            entry[disp_key] = f"{fmt(prior_val)} → {fmt(curr)}"
        else:
            entry[disp_key] = fmt(curr)


def _game_id_display_fields(game_id: str) -> tuple[str, str, str]:
    """Return Date, Matchup and Time strings from ``game_id``."""
    try:
        parts = parse_game_id(str(game_id))
    except Exception as e:  # pragma: no cover - defensive
        logger = get_logger(__name__)
        logger.warning(f"Could not parse game_id: {game_id} → {e}")
        parts = {}

    date = parts.get("date", "")
    matchup = f"{parts.get('away', '')} @ {parts.get('home', '')}".strip()
    if not date or not matchup.strip() or "@" not in matchup:
        logger = get_logger(__name__)
        logger.warning(f"Missing components after parsing game_id: {game_id}")

    time = ""
    time_part = parts.get("time", "")
    if isinstance(time_part, str) and time_part.startswith("T"):
        raw = time_part.split("-")[0][1:]
        try:
            time = datetime.strptime(raw, "%H%M").strftime("%-I:%M %p")
        except Exception:
            try:
                time = datetime.strptime(raw, "%H%M").strftime("%I:%M %p").lstrip("0")
            except Exception:
                time = ""
    return date, matchup, time


def build_argument_parser(
    description: str,
    output_discord_default: bool = True,
    include_stake_mode: bool = False,
    include_debug_json: bool = False,
) -> "argparse.ArgumentParser":
    """Return a parser with common snapshot CLI options."""
    import argparse

    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "--date",
        default=datetime.today().strftime("%Y-%m-%d"),
        help="Comma-separated list of dates",
    )
    parser.add_argument("--min-ev", type=float, default=0.05)
    parser.add_argument("--max-ev", type=float, default=0.20)
    if include_stake_mode:
        parser.add_argument("--stake-mode", default="model")
    parser.add_argument("--output-discord", dest="output_discord", action="store_true")
    parser.add_argument(
        "--no-output-discord", dest="output_discord", action="store_false"
    )
    if include_debug_json:
        parser.add_argument(
            "--debug-json", default=None, help="Path to write debug output"
        )
    parser.add_argument(
        "--reset-snapshot",
        action="store_true",
        help="Clear stored snapshot before running",
    )
    parser.set_defaults(output_discord=output_discord_default)
    return parser


def _style_dataframe(df: pd.DataFrame) -> pd.io.formats.style.Styler:
    """Return a styled DataFrame with conditional formatting."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M ET")

    def _apply_movement(col: str, move_col: str, invert: bool = False):
        def inner(series):
            colors = []
            moves = df.get(move_col)
            for mv in moves if moves is not None else []:
                if mv in ("better", "up"):
                    colors.append(
                        "background-color: #f8d7da"
                        if invert
                        else "background-color: #d4edda"
                    )
                elif mv in ("worse", "down"):
                    colors.append(
                        "background-color: #d4edda"
                        if invert
                        else "background-color: #f8d7da"
                    )
                else:
                    colors.append("")
            return colors

        return inner

    styled = df.style.set_caption(f"Generated: {timestamp}")
    if "odds_movement" in df.columns:
        styled = styled.apply(_apply_movement("Odds", "odds_movement"), subset=["Odds"])
    if "fv_movement" in df.columns:
        styled = styled.apply(
            _apply_movement("Fair Value", "fv_movement"),
            subset=["FV"],
        )
    if "ev_movement" in df.columns:
        styled = styled.apply(_apply_movement("EV", "ev_movement"), subset=["EV"])
    if "stake_movement" in df.columns:
        styled = styled.apply(
            _apply_movement("Stake", "stake_movement"), subset=["Stake"]
        )
    if "sim_movement" in df.columns:
        styled = styled.apply(
            _apply_movement("Sim %", "sim_movement"), subset=["Sim %"]
        )
    if "mkt_movement" in df.columns:
        styled = styled.apply(
            _apply_movement("Mkt %", "mkt_movement"), subset=["Mkt %"]
        )
    if "is_new" in df.columns:

        def highlight_new(row):
            return [
                "background-color: #e6ffe6" if row.get("is_new") else "" for _ in row
            ]

        styled = styled.apply(highlight_new, axis=1)

    styled = (
        styled.set_properties(
            subset=[c for c in df.columns if c != "Market Class"],
            **{
                "text-align": "center",
                "font-family": "monospace",
                "font-size": "10pt",
            },
        )
        .set_properties(
            subset=["Market Class"],
            **{
                "text-align": "center",
                "font-family": "monospace",
                "font-size": "10pt",
            },
        )
        .set_table_styles(
            [
                {
                    "selector": "th",
                    "props": [
                        ("font-weight", "bold"),
                        ("background-color", "#e0f7fa"),
                        ("color", "black"),
                        ("text-align", "center"),
                    ],
                }
            ]
        )
    )

    try:
        styled = styled.hide_index()
    except AttributeError:
        pass

    hide_cols = [
        c
        for c in [
            "odds_movement",
            "fv_movement",
            "ev_movement",
            "stake_movement",
            "sim_movement",
            "mkt_movement",
            "is_new",
            "market_class",
            "prev_sim_prob",
            "prev_market_prob",
            "prev_market_odds",
            "prev_blended_fv",
            "sim_prob",
            "market_prob",
            "market_odds",
            "blended_fv",
            "sim_prob_display",
            "mkt_prob_display",
            "odds_display",
            "fv_display",
        ]
        if c in df.columns
    ]
    if hide_cols:
        try:
            styled = styled.hide(axis="columns", subset=hide_cols)
        except Exception:
            try:
                styled = styled.hide_columns(hide_cols)
            except Exception:
                pass

    return styled


def send_bet_snapshot_to_discord(
    df: pd.DataFrame,
    market_type: str,
    webhook_url: str,
    debug_counts: dict | None = None,
    role: str | None = None,
    force_dispatch: bool = False,
) -> None:
    """Render a styled image and send it to a Discord webhook."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M ET")
    if (df is None or df.empty) and not force_dispatch:
        print("⚠️ No qualifying snapshot bets to dispatch")
        return

    # 🚫 Filter out bets with < 1 unit stake before rendering
    try:
        min_stake = 1.0
        stake_vals = None
        if "raw_kelly" in df.columns:
            stake_vals = pd.to_numeric(df["raw_kelly"], errors="coerce")
        elif "stake" in df.columns:
            stake_vals = pd.to_numeric(df["stake"], errors="coerce")
        elif "Stake" in df.columns:
            stake_vals = pd.to_numeric(
                df["Stake"].astype(str).str.replace("u", "", regex=False),
                errors="coerce",
            )
        if stake_vals is not None:
            mask = stake_vals >= min_stake
            if "is_prospective" in df.columns:
                mask = mask | df["is_prospective"]
            if "logged" in df.columns and "hours_to_game" in df.columns:
                mask = mask | (df["logged"] & (df["hours_to_game"] > 0))
            df = df[mask]
    except Exception:
        pass

    if df.empty and not force_dispatch:
        print("⚠️ No qualifying snapshot bets to dispatch")
        return

    df = df.copy()

    if "logged" in df.columns and "Logged?" not in df.columns:
        df["Logged?"] = df["logged"].apply(lambda x: "YES" if bool(x) else "NO")
    if "logged" in df.columns and "Status" not in df.columns:
        df["Status"] = df["logged"].apply(lambda x: "🟢 LOGGED" if bool(x) else "")

    if debug_counts is not None:
        for _, row in df.iterrows():
            label = "🔍" if row.get("is_prospective") else "🟢"
            matchup = row.get("Matchup") or row.get("matchup")
            market = row.get("Market") or row.get("market")
            side = row.get("Bet") or row.get("side")
            book = row.get("Book") or row.get("book")
            stake_val = (
                row.get("Stake") or row.get("stake") or row.get("snapshot_stake")
            )
            if stake_val is None:
                stake_val = 0
            try:
                stake_str = f"{float(str(stake_val).replace('u','')):.2f}u"
            except Exception:
                stake_str = str(stake_val)
            print(f"{label} {matchup} | {market} | {side} | {stake_str} @ {book}")
    if dfi is None:
        print("⚠️ dataframe_image is not available. Sending text fallback.")
        _send_table_text(df, market_type, webhook_url, force_dispatch=force_dispatch)
        return

    if "EV" in df.columns:
        sort_tmp = df["EV"].str.replace("%", "", regex=False)
        try:
            sort_vals = sort_tmp.astype(float)
            df = df.assign(_ev_sort=sort_vals)
            if "cumulative_stake" in df.columns:
                df = df.assign(
                    _stake_sort=pd.to_numeric(df["cumulative_stake"], errors="coerce")
                )
                df = df.sort_values(
                    by=["_ev_sort", "_stake_sort"],
                    ascending=[False, False],
                ).drop(columns=["_ev_sort", "_stake_sort"])
            else:
                df = df.sort_values("_ev_sort", ascending=False).drop(
                    columns="_ev_sort"
                )
        except Exception:
            by_cols = ["EV"]
            if "cumulative_stake" in df.columns:
                by_cols.append("cumulative_stake")
            df = df.sort_values(by=by_cols, ascending=False)
    else:
        by_cols = ["ev_percent"]
        if "cumulative_stake" in df.columns:
            df = df.assign(
                _stake_sort=pd.to_numeric(df["cumulative_stake"], errors="coerce")
            )
            by_cols.append("_stake_sort")
            df = df.sort_values(by=by_cols, ascending=False).drop(columns="_stake_sort")
        else:
            df = df.sort_values(by_cols[0], ascending=False)

    columns_to_exclude = [
        "prev_sim_prob",
        "prev_market_prob",
        "prev_market_odds",
        "prev_blended_fv",
        "sim_prob",
        "market_prob",
        "market_odds",
        "blended_fv",
        "market_class",
        "_raw_sportsbook",
        "raw_kelly",
        "blended_prob",
        "segment",
        "date_simulated",
        "logged",
        "skip_reason",
        "snapshot_stake",
        "is_prospective",
    ]

    df = df.drop(columns=[col for col in columns_to_exclude if col in df.columns])

    styled = _style_dataframe(df)

    buf = io.BytesIO()
    try:
        dfi.export(styled, buf, table_conversion="chrome", max_rows=-1)
        print(f"🧪 Chrome export buffer size: {buf.tell()} bytes")
    except Exception as e:
        print(f"❌ dfi.export failed: {e}")
        try:
            buf.seek(0)
            buf.truncate(0)
            dfi.export(styled, buf, table_conversion="matplotlib", max_rows=-1)
            print(f"🧪 Matplotlib export buffer size: {buf.tell()} bytes")
        except Exception as e2:
            print(f"⚠️ Fallback export failed: {e2}")
            buf.close()
            _send_table_text(
                df, market_type, webhook_url, force_dispatch=force_dispatch
            )
            return
    buf.seek(0)
    files = {"file": ("snapshot.png", buf, "image/png")}

    # Ensure webhook waits for the message to complete so we get a response
    if not webhook_url.endswith("?wait=true"):
        webhook_url += "?wait=true"

    content = f"📈 **{market_type}**"
    if force_dispatch:
        content += " (Forced Dispatch)"
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M ET")
    content += f"\n_Generated: {timestamp}_"

    logger.info(
        "🧪 Posting snapshot to Discord (market_type=%s, rows=%s)",
        market_type,
        df.shape[0],
    )
    try:
        payload = {
            "payload_json": json.dumps({"content": content})
        }
        resp = post_with_retries(
            webhook_url,
            data=payload,
            files=files,
            timeout=10,
        )
        if resp:
            logger.info("✅ Snapshot sent: %s bets dispatched", df.shape[0])
            try:
                rj = resp.json()
                logger.info("🧪 Discord Channel ID: %s", rj.get("channel_id"))
                logger.info(
                    "🧪 Message URL: https://discord.com/channels/@me/%s",
                    rj.get("id"),
                )
            except Exception as e:
                logger.warning("⚠️ Could not parse Discord response JSON: %s", e)
    except Exception as e:
        logger.error("❌ Failed to send snapshot for %s: %s", market_type, e)
    finally:
        buf.close()


def _send_table_text(
    df: pd.DataFrame,
    market_type: str,
    webhook_url: str,
    *,
    force_dispatch: bool = False,
) -> None:
    """Send the DataFrame as a Markdown code block to Discord."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M ET")
    if force_dispatch:
        caption = (
            f"📸 **Snapshot Test Mode — {market_type} (Forced Dispatch)**\n"
            f"_Generated: {timestamp}_"
        )
    else:
        caption = (
            f"📈 **Live Market Snapshot — {market_type}** (text fallback)\n"
            f"_Generated: {timestamp}_"
        )

    try:
        table = df.to_markdown(index=False)
    except Exception:
        table = df.to_string(index=False)

    if len(table) > 1900:
        table = table[:1900] + "\n...(truncated)"

    message = f"{caption}\n```\n{table}\n```"
    try:
        post_with_retries(webhook_url, json={"content": message}, timeout=10)
    except Exception as e:
        print(f"❌ Failed to send text snapshot for {market_type}: {e}")


def compare_and_flag_new_rows(
    current_entries: List[dict],
    snapshot_path: str,
    prior_snapshot: str | Dict[str, dict] | None = None,
) -> Tuple[List[dict], Dict[str, dict]]:
    """Return entries annotated with new-row and movement flags.

    Parameters
    ----------
    current_entries : List[dict]
        List of rows from the current evaluation.
    snapshot_path : str
        Path to write the updated snapshot for the next run.
    prior_snapshot : str | Dict[str, dict] | None, optional
        Previous snapshot data (or path) to use for movement detection when the
        tracker lacks a prior entry.  This enables highlighting bets that now
        qualify after being below the EV filter in the previous run.
    """
    try:
        with open(snapshot_path) as f:
            last_snapshot = json.load(f)
    except Exception:
        last_snapshot = {}

    prior_data: Dict[str, dict] = {}
    if isinstance(prior_snapshot, str):
        try:
            with open(prior_snapshot) as f:
                prior_data = json.load(f)
        except Exception:
            prior_data = {}
    elif isinstance(prior_snapshot, dict):
        prior_data = prior_snapshot

    seen = set()
    flagged = []
    next_snapshot = {}

    for entry in current_entries:
        game_id = entry.get("game_id", "")
        book = entry.get("book", entry.get("best_book", ""))
        market = str(entry.get("market", "")).strip()
        side = str(entry.get("side", "")).strip()
        key = f"{game_id}:{market}:{side}"
        prior = (
            MARKET_EVAL_TRACKER_BEFORE_UPDATE.get(key)
            or last_snapshot.get(key)
            or prior_data.get(key)
        )
        entry.update(
            {
                "prev_sim_prob": (prior or {}).get("sim_prob"),
                "prev_blended_fv": (prior or {}).get("blended_fv"),
            }
        )
        tracker = MARKET_EVAL_TRACKER_BEFORE_UPDATE
        baseline = (prior or {}).get("baseline_consensus_prob")
        if baseline is None:
            baseline = tracker.get(key, {}).get("baseline_consensus_prob")
        if baseline is None:
            baseline = entry.get("consensus_prob")

        entry["baseline_consensus_prob"] = baseline
        movement = track_and_update_market_movement(
            entry,
            MARKET_EVAL_TRACKER,
            MARKET_EVAL_TRACKER_BEFORE_UPDATE,
        )
        entry["is_new"] = (
            key not in MARKET_EVAL_TRACKER_BEFORE_UPDATE
            and key not in last_snapshot
            and key not in prior_data
        )
        entry["blended_fv"] = 1 / entry["blended_prob"]
        annotate_display_deltas(entry, prior)
        blended_fv = entry.get("blended_fv")
        market_odds = entry.get("market_odds")
        ev_pct = entry.get("ev_percent")

        if blended_fv is None or ev_pct is None or market_odds is None:
            print(
                f"⛔ Skipping {game_id} — missing required fields (FV:{blended_fv}, EV:{ev_pct}, Odds:{market_odds})"
            )
            continue

        next_snapshot[key] = {
            "game_id": game_id,
            "market": entry.get("market"),
            "side": entry.get("side"),
            "best_book": book,
            "book": entry.get("book", book),
            "sim_prob": entry.get("sim_prob"),
            "market_prob": entry.get("market_prob"),
            "blended_fv": blended_fv,
            "market_odds": market_odds,
            "ev_percent": ev_pct,
            "segment": entry.get("segment"),
            "stake": entry.get("stake"),
            "market_class": entry.get("market_class"),
            "date_simulated": entry.get("date_simulated"),
            "display": build_display_block(entry),
        }

        if VERBOSE_MODE:
            old_ev = (prior or {}).get("ev_percent")
            new_ev = entry.get("ev_percent")
            old_fv = (prior or {}).get("blended_fv")
            new_fv = entry.get("blended_fv")
            try:
                print(
                    f"🔍 Movement: {key} — EV {old_ev:.2f} → {new_ev:.2f}, FV {old_fv:.2f} → {new_fv:.2f}"
                )
            except Exception:
                print(
                    f"🔍 Movement: {key} — EV {old_ev} → {new_ev}, FV {old_fv} → {new_fv}"
                )

        j = json.dumps(entry, sort_keys=True)
        if j in seen:
            continue
        seen.add(j)
        flagged.append(entry)

    return flagged, next_snapshot


def format_table_with_highlights(entries: List[dict]) -> str:
    """Render rows for fallback text without emoji highlights."""
    lines = []
    for e in entries:
        # preserve is_new evaluation but avoid emoji output
        new_sym = "*" if e.get("is_new") else ""
        odds_sym = {"better": "", "worse": "", "same": ""}.get(
            e.get("odds_movement"), ""
        )
        ev_sym = {"better": "", "worse": "", "same": ""}.get(e.get("ev_movement"), "")
        fair = e.get("blended_fv")
        if isinstance(fair, (int, float)):
            fair_str = f"{fair:+}"
        else:
            fair_str = str(fair)
        ev = e.get("ev_percent", 0.0)
        ev_str = f"{ev:+.1f}%"
        line = f"{new_sym} {e.get('market', ''):<7} | {e.get('side', ''):<12} | {odds_sym} {fair_str:>6} | {ev_sym} {ev_str}"
        lines.append(line)
    return "\n".join(lines)


def load_simulations(sim_dir: str) -> dict:
    sims = {}
    if not os.path.isdir(sim_dir):
        logger.warning("❌ Sim directory not found: %s", sim_dir)
        return sims
    for f in os.listdir(sim_dir):
        if f.endswith(".json"):
            path = os.path.join(sim_dir, f)
            try:
                with open(path) as fh:
                    sims[f.replace(".json", "")] = json.load(fh)
            except Exception as e:
                logger.warning("❌ Failed to load %s: %s", path, e)
    return sims


def build_snapshot_rows(
    sim_data: dict, odds_data: dict, min_ev: float, debug_log=None
) -> list:
    if debug_log is None:
        debug_log = []
    rows = []
    for game_id, sim in sim_data.items():
        full_gid = str(game_id)
        canonical_gid = canonical_game_id(full_gid)
        markets = sim.get("markets", [])
        odds = odds_data.get(canonical_gid)
        if odds is None:
            fuzzy_id = fuzzy_match_game_id(canonical_gid, list(odds_data.keys()), window=3)
            if fuzzy_id:
                odds = odds_data.get(fuzzy_id)
                if odds is None:
                    logger.warning(
                        "⚠️ Fuzzy matched %s → %s, but odds entry is None — skipping.",
                        full_gid,
                        fuzzy_id,
                    )
                    continue
                logger.info("🔄 Fuzzy matched %s → %s", full_gid, fuzzy_id)
            else:
                normalized_gid = normalize_game_id(full_gid)
                if normalized_gid in odds_data and normalized_gid != full_gid:
                    logger.warning(
                        "❌ No odds for %s — found only normalized ID %s",
                        full_gid,
                        normalized_gid,
                    )
                else:
                    logger.warning(
                        "❌ No odds found for %s (even with fuzzy matching), skipping.",
                        full_gid,
                    )
                continue
        start_str = odds.get("start_time") if odds else None
        dt = None
        start_formatted = ""
        if start_str:
            try:
                dt = datetime.fromisoformat(start_str.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                dt = to_eastern(dt)
            except Exception:
                dt = None
        if dt is None:
            dt = parse_start_time(game_id, odds)
        if dt is None:
            logger.debug("⏱️ Skipping %s — start time not found", game_id)
            debug_log.append({"game_id": game_id, "reason": "no_start_time"})
            continue
        hours_to_game = compute_hours_to_game(to_eastern(dt))
        if DEBUG_MODE:
            logger.debug(
                "🕓 %s start=%s now=%s Δ=%.2fh",
                game_id,
                dt.isoformat(),
                now_eastern().isoformat(),
                hours_to_game,
            )
        start_et = to_eastern(dt)
        try:
            start_formatted = start_et.strftime("%-I:%M %p")
        except Exception:
            start_formatted = start_et.strftime("%I:%M %p").lstrip("0")
        if hours_to_game <= 0:
            logger.debug(
                "⏱️ Skipping %s — game has already started (%.2fh ago)",
                game_id,
                abs(hours_to_game),
            )
            debug_log.append(
                {
                    "game_id": game_id,
                    "reason": "game_live",
                    "hours_to_game": round(hours_to_game, 2),
                }
            )
            continue
        for entry in markets:
            market = entry.get("market")
            side = entry.get("side")
            sim_prob = entry.get("sim_prob")
            if market is None or side is None or sim_prob is None:
                continue

            lookup_side = (
                normalize_to_abbreviation(side.strip()) if market == "h2h" else side
            )
            market_entry, _, matched_key, segment, price_source = (
                get_market_entry_with_alternate_fallback(odds, market, lookup_side)
            )
            if not isinstance(market_entry, dict):
                alt = convert_full_team_spread_to_odds_key(lookup_side)
                market_entry, _, matched_key, segment, price_source = (
                    get_market_entry_with_alternate_fallback(odds, market, alt)
                )
            if not isinstance(market_entry, dict):
                logger.warning(
                    "❌ No odds for %s — market %s side %s",
                    game_id,
                    market,
                    lookup_side,
                )
                continue

            price = market_entry.get("price")
            if price is None:
                logger.warning(
                    "❌ No odds for %s — market %s side %s (missing price)",
                    game_id,
                    market,
                    lookup_side,
                )
                continue

            sportsbook_odds = market_entry.get("per_book", {})
            best_book = extract_best_book(sportsbook_odds)
            if best_book:
                sportsbook_odds[best_book] = price
                market_entry["per_book"] = sportsbook_odds
            result, _ = calculate_consensus_prob(
                game_id=canonical_gid,
                market_odds={canonical_gid: odds},
                market_key=matched_key,
                label=lookup_side,
            )
            consensus_prob = result.get("consensus_prob")
            if consensus_prob is None:
                consensus_prob = market_entry.get("consensus_prob")
            book_odds_list = list(result.get("bookwise_probs", {}).values())

            # Capture which sportsbooks formed the consensus line
            books_used = result.get("books_used")
            if books_used is None:
                books_used = market_entry.get("books_used", [])

            market_clean = matched_key.replace("alternate_", "")
            market_class = "alternate" if price_source == "alternate" else "main"

            tracker_key = build_key(canonical_gid, market_clean, side)
            prior_row = (
                MARKET_EVAL_TRACKER.get(tracker_key)
                or MARKET_EVAL_TRACKER_BEFORE_UPDATE.get(tracker_key)
                or {}
            )

            prev_prob = prior_row.get("market_prob")
            curr_prob = consensus_prob
            try:
                observed_move = float(curr_prob) - float(prev_prob)
            except Exception:
                observed_move = 0.0

            p_blended, _, _, p_market = blend_prob(
                sim_prob,
                price,
                market,
                hours_to_game,
                consensus_prob,
                book_odds_list=book_odds_list,
                line_move=0.0,
                observed_move=observed_move,
            )

            ev_pct = calculate_ev_from_prob(p_blended, price)
            required_move = required_market_move(
                hours_to_game,
                market=market_clean,
                ev_percent=ev_pct,
                book_count=len(book_odds_list),
            )
            ratio = observed_move / required_move if required_move > 0 else 0.0
            strength = max(0.0, ratio)
            stake_fraction = 0.125 if market_class == "alternate" else 0.25
            raw_kelly = kelly_fraction(p_blended, price, fraction=stake_fraction)

            stake = round(raw_kelly * (strength**1.5), 4)

            logger.debug(
                "✓ %s | %s | %s → EV %.2f%% | Stake %.2fu | Source %s",
                game_id,
                market_clean,
                side,
                ev_pct,
                stake,
                market_entry.get("pricing_method", "book"),
            )

            normalized_side = normalize_label_for_odds(side, matched_key)
            row = {
                "game_id": game_id,
                "market": market_clean,
                "side": normalized_side,
                "sim_prob": round(sim_prob, 4),
                "market_prob": round(p_market, 6),
                "blended_prob": round(p_blended, 4),
                "blended_fv": 1 / p_blended,
                "market_odds": price,
                "ev_percent": round(ev_pct, 2),
                "stake": stake,
                "raw_kelly": raw_kelly,
                "segment": segment,
                "market_class": market_class,
                "best_book": best_book,
                "books_used": books_used,
                "_raw_sportsbook": sportsbook_odds,
                "date_simulated": datetime.now().isoformat(),
                "hours_to_game": round(hours_to_game, 2),
                "logged": bool(entry.get("logged", False)),
                "skip_reason": entry.get("skip_reason"),
            }
            # \U0001f4cc Persisting logged bets until game start
            if row.get("logged") and row.get("hours_to_game", 0) > 0:
                row["snapshot_force_include"] = True
            parsed = parse_game_id(str(game_id))
            row["Date"] = parsed.get("date", "")
            row["Matchup"] = (
                f"{parsed.get('away', '')} @ {parsed.get('home', '')}".strip()
            )
            time_part = parsed.get("time")
            if time_part:
                raw = time_part.split("-")[0][1:]
                try:
                    time_str = datetime.strptime(raw, "%H%M").strftime("%-I:%M %p")
                except Exception:
                    try:
                        time_str = (
                            datetime.strptime(raw, "%H%M")
                            .strftime("%I:%M %p")
                            .lstrip("0")
                        )
                    except Exception:
                        time_str = ""
                if not time_str:
                    time_str = start_formatted
            else:
                time_str = start_formatted

            row["Time"] = time_str or ""
            row["segment_label"] = get_segment_label(matched_key, normalized_side)
            theme = get_theme({"side": normalized_side, "market": market_clean})
            row["theme_key"] = get_theme_key(market_clean, theme)
            row.setdefault("entry_type", "first")
            row["_tracker_entry"] = prior_row
            row["_prior_snapshot"] = prior_row

            row.update(
                {
                    "prev_sim_prob": (prior_row or {}).get("sim_prob"),
                    "prev_blended_fv": (prior_row or {}).get("blended_fv"),
                }
            )
            # --- Assign baseline_consensus_prob from tracker ---
            tracker = MARKET_EVAL_TRACKER_BEFORE_UPDATE
            key = tracker_key
            baseline = (prior_row or {}).get("baseline_consensus_prob")
            if baseline is None:
                baseline = tracker.get(key, {}).get("baseline_consensus_prob")
            if baseline is None:
                baseline = row.get("consensus_prob")

            row["baseline_consensus_prob"] = baseline

            # Compute movement and update tracker
            movement = track_and_update_market_movement(
                row,
                MARKET_EVAL_TRACKER,
                MARKET_EVAL_TRACKER_BEFORE_UPDATE,
            )
            annotate_display_deltas(row, prior_row)
            if VERBOSE_MODE:
                old_ev = (prior_row or {}).get("ev_percent")
                new_ev = row.get("ev_percent")
                old_fv = (prior_row or {}).get("blended_fv")
                new_fv = row.get("blended_fv")
                try:
                    print(
                        f"🔍 Movement: {tracker_key} — EV {old_ev:.2f} → {new_ev:.2f}, FV {old_fv:.2f} → {new_fv:.2f}"
                    )
                except Exception:
                    print(
                        f"🔍 Movement: {tracker_key} — EV {old_ev} → {new_ev}, FV {old_fv} → {new_fv}"
                    )
            rows.append(row)
    return rows


def format_for_display(rows: list, include_movement: bool = False) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    if df.empty:
        return df

    if "market" in df.columns and "Market" not in df.columns:
        df["Market"] = df["market"].astype(str)
    elif "Market" not in df.columns:
        df["Market"] = "N/A"

    if "market_class" not in df.columns:
        df["market_class"] = "main"
    df["Market Class"] = (
        df["market_class"].str.lower().map({"alternate": "Alt", "main": "Main"}).fillna("❓")
    )

    if "label" not in df.columns:
        if "is_prospective" in df.columns:
            df["label"] = df["is_prospective"].apply(lambda x: "🔍" if x else "🟢")
        else:
            df["label"] = "🟢"

    tmp = df["game_id"].apply(lambda gid: pd.Series(_game_id_display_fields(gid)))
    df["Date"] = tmp[0]
    df["Matchup"] = tmp[1]
    time_from_gid = tmp[2]
    if "Time" in df.columns:
        df["Time"] = df["Time"].where(
            df["Time"].astype(str).str.strip() != "", time_from_gid
        )
    else:
        df["Time"] = time_from_gid
    if df["Time"].eq("").all():
        df.drop(columns=["Time"], inplace=True)
    df["Bet"] = df["side"]
    if "book" in df.columns:
        df["Book"] = df["book"]
    else:
        df["Book"] = ""
    if "odds_display" in df.columns:
        df["Odds"] = df["odds_display"]
    else:
        df["Odds"] = df["market_odds"].apply(
            lambda x: f"{x:+}" if isinstance(x, (int, float)) else x
        )

    if "sim_prob_display" in df.columns:
        df["Sim %"] = df["sim_prob_display"]
    else:
        df["Sim %"] = (df["sim_prob"] * 100).map("{:.1f}%".format)

    if "mkt_prob_display" in df.columns:
        df["Mkt %"] = df["mkt_prob_display"]
    else:
        df["Mkt %"] = (df["market_prob"] * 100).map("{:.1f}%".format)

    if "fv_display" in df.columns:
        df["FV"] = df["fv_display"]
    else:
        df["FV"] = df["blended_fv"].apply(
            lambda x: f"{round(x)}" if isinstance(x, (int, float)) else "N/A"
        )

    if "ev_display" in df.columns:
        df["EV"] = df["ev_display"]
    else:
        df["EV"] = df["ev_percent"].map("{:+.1f}%".format)

    if "stake_display" in df.columns and "Stake" not in df.columns:
        df["Stake"] = df["stake_display"]

    if "snapshot_stake" in df.columns and "Stake" not in df.columns:

        def _apply_snapshot_stake(row):
            try:
                val = float(row.get("snapshot_stake", 0))
            except Exception:
                val = 0.0
            return f"{val:.2f}u"

        df["Stake"] = df.apply(_apply_snapshot_stake, axis=1)

    if "logged" in df.columns and "Logged?" not in df.columns:
        df["Logged?"] = df["logged"].apply(lambda x: "YES" if bool(x) else "NO")
    if "logged" in df.columns and "Status" not in df.columns:
        df["Status"] = df["logged"].apply(lambda x: "🟢 LOGGED" if bool(x) else "")

    # Derive Stake from raw_kelly
    if "raw_kelly" in df.columns and "Stake" not in df.columns:
        df["Stake"] = df["raw_kelly"].round(2).astype(str) + "u"

    required_cols = ["Date"]
    if "Time" in df.columns:
        required_cols.append("Time")
    required_cols += [
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
        "Status",
    ]
    for col in required_cols:
        if col not in df.columns:
            df[col] = "N/A"

    extra_cols = [
        "sim_prob_display",
        "mkt_prob_display",
        "odds_display",
        "fv_display",
        "ev_percent",
        "raw_kelly",
        "sim_prob",
        "market_prob",
        "market_odds",
        "blended_fv",
        "prev_sim_prob",
        "prev_market_prob",
        "prev_market_odds",
        "prev_blended_fv",
        "logged",
        "skip_reason",
    ]

    if include_movement:
        movement_cols = [
            "ev_movement",
            "mkt_movement",
            "fv_movement",
            "stake_movement",
            "sim_movement",
            "odds_movement",
            "is_new",
        ]

        for col in movement_cols:
            if col not in df.columns:
                df[col] = [row.get(col, "same") for row in rows]

        cols = required_cols + movement_cols
    else:
        cols = required_cols

    cols += [c for c in extra_cols if c in df.columns]
    cols.append("market_class")

    return df[cols]


def build_display_block(row: dict) -> Dict[str, str]:
    """Return formatted display fields for a snapshot row."""
    game_id = str(row.get("game_id", ""))
    date, matchup, time = _game_id_display_fields(game_id)
    time_from_row = row.get("Time")
    if isinstance(time_from_row, str) and time_from_row.strip():
        time = time_from_row

    market_class_key = row.get("market_class", "main")
    market_class = {
        "alternate": "Alt",
        "main": "Main",
    }.get(market_class_key, "❓")

    if "odds_display" in row:
        odds_str = row.get("odds_display", "N/A")
    else:
        odds = row.get("market_odds")
        if isinstance(odds, (int, float)):
            odds_str = f"{odds:+}"
        else:
            odds_str = str(odds) if odds is not None else "N/A"

    if "sim_prob_display" in row:
        sim_str = row.get("sim_prob_display", "N/A")
    else:
        sim_prob = row.get("sim_prob")
        sim_str = f"{sim_prob * 100:.1f}%" if sim_prob is not None else "N/A"

    if "mkt_prob_display" in row:
        mkt_str = row.get("mkt_prob_display", "N/A")
    else:
        mkt_prob = row.get("market_prob")
        mkt_str = f"{mkt_prob * 100:.1f}%" if mkt_prob is not None else "N/A"

    if "fv_display" in row:
        fv_str = row.get("fv_display", "N/A")
    else:
        fv = row.get("blended_fv")
        fv_str = f"{round(fv)}" if isinstance(fv, (int, float)) else "N/A"

    if "ev_display" in row:
        ev_str = row.get("ev_display", "N/A")
    else:
        ev = row.get("ev_percent")
        ev_str = f"{ev:+.1f}%" if ev is not None else "N/A"

    stake_val = row.get("stake")
    if "stake_display" in row:
        stake_str = row.get("stake_display", "N/A")
    else:
        stake_str = f"{stake_val:.2f}u" if stake_val is not None else "N/A"
    if row.get("cumulative_stake") is not None:
        try:
            total = float(row["cumulative_stake"])
            if stake_val is None:
                stake_val = 0.0
            if total != float(stake_val):
                stake_str = f"{stake_val:.2f}u ({total:.2f}u)"
        except Exception:
            pass

    return {
        "Date": date,
        "Time": time,
        "Matchup": matchup,
        "Market Class": market_class,
        "Market": row.get("market", ""),
        "Bet": row.get("side", ""),
        "Book": row.get("book", ""),
        "Odds": odds_str,
        "Sim %": sim_str,
        "Mkt %": mkt_str,
        "FV": fv_str,
        "EV": ev_str,
        "Stake": stake_str,
    }


def export_market_snapshots(df: pd.DataFrame, snapshot_paths: Dict[str, str]) -> None:
    """Write full market tables to JSON files."""
    if not snapshot_paths:
        return
    os.makedirs(os.path.dirname(list(snapshot_paths.values())[0]), exist_ok=True)
    for market, path in snapshot_paths.items():
        subset = df[df["Market"].str.lower().str.startswith(market.lower(), na=False)]
        try:
            subset.to_json(path, orient="records", indent=2)
        except Exception as e:
            print(f"❌ Failed to export {market} snapshot to {path}: {e}")


def _assign_snapshot_role(row: Dict) -> str:
    """Return snapshot role string based on market attributes."""
    market_class = str(row.get("market_class", "main")).lower()
    market_type = str(row.get("market_type") or row.get("market", "")).lower()

    if market_type.startswith("h2h"):
        return "h2h"
    if "spread" in market_type:
        return "spreads"
    if "total" in market_type:
        return "totals"
    if market_class.startswith("alt") or market_class == "alternate":
        return BEST_BOOK_ALT
    return BEST_BOOK_MAIN


def expand_snapshot_rows_with_kelly(
    rows: List[dict],
    allowed_books: List[str] | None = None,
    include_ev_stake_movement: bool = True,
    pending_bets: dict | None = None,
) -> List[dict]:
    """Expand rows into one row per sportsbook with updated EV and stake.

    If ``allowed_books`` is provided, only sportsbooks in that list will be
    expanded.  Each expanded row has its display fields refreshed to reflect the
    specific book price.

    ``pending_bets`` may be provided to append prospective bets without
    modification to the EV or market fields.  For these rows ``snapshot_stake``
    is derived from ``raw_kelly`` when ``stake`` is zero and the flag
    ``is_prospective`` is set accordingly.
    """

    expanded: List[dict] = []

    for row in rows:
        row["blended_fv"] = 1 / row["blended_prob"]
        per_book = row.get("_raw_sportsbook") or row.get("consensus_books", {})
        canon_gid = canonical_game_id(str(row.get("game_id")))
        tracker_key = build_key(
            canon_gid,
            row.get("market", ""),
            row.get("side", ""),
        )
        prior_row = MARKET_EVAL_TRACKER_BEFORE_UPDATE.get(
            tracker_key
        ) or MARKET_EVAL_TRACKER.get(tracker_key)

        row.update(
            {
                "prev_sim_prob": (prior_row or {}).get("sim_prob"),
                "prev_blended_fv": (prior_row or {}).get("blended_fv"),
            }
        )
        # --- Ensure baseline_consensus_prob is included ---
        tracker = MARKET_EVAL_TRACKER_BEFORE_UPDATE
        key = tracker_key
        baseline = (prior_row or {}).get("baseline_consensus_prob")
        if baseline is None:
            baseline = tracker.get(key, {}).get("baseline_consensus_prob")
        if baseline is None:
            baseline = row.get("consensus_prob")

        row["baseline_consensus_prob"] = baseline

        row["book"] = row.get("book", row.get("best_book"))

        if not isinstance(per_book, dict) or not per_book:
            if row.get("market_odds") is None:
                row["skip_reason"] = "no_odds"
            movement = track_and_update_market_movement(
                row,
                MARKET_EVAL_TRACKER,
                MARKET_EVAL_TRACKER_BEFORE_UPDATE,
            )
            annotate_display_deltas(row, prior_row)
            if not include_ev_stake_movement:
                movement.pop("ev_movement", None)
                movement.pop("stake_movement", None)
            row.update(movement)
            ensure_consensus_books(row)
            expanded.append(row)
            continue

        expanded_any = False
        for book, odds in per_book.items():
            if allowed_books and book not in allowed_books:
                continue

            p = row.get("blended_prob")
            if p is None:
                p = row.get("sim_prob")
            if p is None:
                p = row.get("market_prob")
            if p is None:
                continue

            try:
                odds_val = float(odds)
            except Exception:
                try:
                    odds_val = float(row.get("market_odds"))
                except Exception:
                    numeric = [
                        o for o in per_book.values() if isinstance(o, (int, float))
                    ]
                    odds_val = min(numeric) if numeric else None

            if isinstance(odds_val, float) and odds_val.is_integer():
                odds_val = int(odds_val)

            if odds_val is None:
                continue

            try:
                ev = calculate_ev_from_prob(p, odds_val)
                fraction = 0.125 if row.get("market_class") == "alternate" else 0.25
                raw_kelly = kelly_fraction(p, odds_val, fraction=fraction)

                stake = round(raw_kelly, 4)
            except Exception:
                continue

            expanded_any = True
            expanded_row = row.copy()
            expanded_row["logged"] = bool(row.get("logged", False))
            expanded_row["blended_fv"] = 1 / expanded_row["blended_prob"]
            expanded_row.update(
                {
                    "best_book": book,
                    "book": book,
                    "market_odds": odds_val,
                    "ev_percent": round(ev, 2),
                    "stake": stake,
                    "raw_kelly": raw_kelly,
                    "_raw_sportsbook": per_book,
                    "consensus_books": per_book,
                }
            )
            if expanded_row.get("logged") and expanded_row.get("hours_to_game", 0) > 0:
                expanded_row["snapshot_force_include"] = True
            if (
                expanded_row.get("stake", 0) == 0
                and expanded_row.get("raw_kelly", 0) > 0
            ):
                expanded_row["snapshot_stake"] = round(expanded_row["raw_kelly"], 2)
                expanded_row["is_prospective"] = True
            else:
                expanded_row["snapshot_stake"] = expanded_row.get("stake", 0)
                expanded_row["is_prospective"] = False

            # 🔖 Assign snapshot role based on market type
            role = _assign_snapshot_role(expanded_row)
            expanded_row["snapshot_role"] = role
            expanded_row.setdefault("snapshot_roles", []).append(role)
            canon_gid = canonical_game_id(str(expanded_row.get("game_id")))
            tracker_key = build_key(
                canon_gid,
                expanded_row["market"],
                expanded_row["side"],
            )
            prior_row = MARKET_EVAL_TRACKER_BEFORE_UPDATE.get(
                tracker_key
            ) or MARKET_EVAL_TRACKER.get(tracker_key)
            expanded_row.update(
                {
                    "prev_sim_prob": (prior_row or {}).get("sim_prob"),
                    "prev_blended_fv": (prior_row or {}).get("blended_fv"),
                }
            )
            # --- Ensure baseline_consensus_prob is included ---
            tracker = MARKET_EVAL_TRACKER_BEFORE_UPDATE
            key = tracker_key
            baseline = (prior_row or {}).get("baseline_consensus_prob")
            if baseline is None:
                baseline = tracker.get(key, {}).get("baseline_consensus_prob")
            if baseline is None:
                baseline = expanded_row.get("consensus_prob")

            expanded_row["baseline_consensus_prob"] = baseline
            movement = track_and_update_market_movement(
                expanded_row,
                MARKET_EVAL_TRACKER,
                MARKET_EVAL_TRACKER_BEFORE_UPDATE,
            )
            annotate_display_deltas(expanded_row, prior_row)
            if not include_ev_stake_movement:
                movement.pop("ev_movement", None)
                movement.pop("stake_movement", None)
            expanded_row.update(movement)
            ensure_consensus_books(expanded_row)
            expanded.append(expanded_row)

        if not expanded_any:
            row_copy = row.copy()
            row_copy["logged"] = bool(row.get("logged", False))
            if row_copy.get("logged") and row_copy.get("hours_to_game", 0) > 0:
                row_copy["snapshot_force_include"] = True
            if allowed_books:
                row_copy["skip_reason"] = "book_filter"
            elif row.get("market_odds") is None:
                row_copy.setdefault("skip_reason", "no_odds")
            else:
                row_copy["skip_reason"] = "invalid_data"
            stake_val = row_copy.get("stake", 0.0) or 0.0
            raw_val = row_copy.get("raw_kelly", 0.0)
            if stake_val == 0.0 and raw_val > 0:
                row_copy["snapshot_stake"] = round(raw_val, 2)
                row_copy["is_prospective"] = True
            else:
                row_copy["snapshot_stake"] = stake_val
                row_copy["is_prospective"] = False

            # 🔖 Assign snapshot role for rows that failed expansion
            role = _assign_snapshot_role(row_copy)
            row_copy["snapshot_role"] = role
            row_copy.setdefault("snapshot_roles", []).append(role)
            ensure_consensus_books(row_copy)
            expanded.append(row_copy)

    deduped: List[dict] = []
    seen = set()
    for r in expanded:
        key = (r.get("game_id"), r.get("market"), r.get("side"), r.get("book"))
        if key not in seen:
            deduped.append(r)
            seen.add(key)

    if pending_bets:
        bets_iter = (
            pending_bets.values() if isinstance(pending_bets, dict) else pending_bets
        )
        for bet in bets_iter:
            try:
                ev = float(bet.get("ev_percent", 0))
                rk = float(bet.get("raw_kelly", 0))
            except Exception:
                continue
            if ev < 5.0 or rk < 1.0:
                continue

            row = bet.copy()
            row["book"] = row.get("book", row.get("best_book"))

            row["snapshot_stake"] = round(rk, 2)
            row["is_prospective"] = True

            if "sim_prob" in row:
                row["sim_prob_display"] = f"{round(row['sim_prob'] * 100, 1)}%"
            else:
                row["sim_prob_display"] = "-"

            baseline = row.get("baseline_consensus_prob")
            if baseline is not None and "market_prob" in row:
                row["mkt_prob_display"] = f"{baseline * 100:.1f}% → {row['market_prob'] * 100:.1f}%"
            elif "market_prob" in row:
                row["mkt_prob_display"] = f"{round(row['market_prob'] * 100, 1)}%"
            else:
                row["mkt_prob_display"] = "-"

            if "ev_percent" in row:
                row["ev_display"] = f"+{round(row['ev_percent'], 1)}%"
            else:
                row["ev_display"] = "-"

            if "fair_odds" in row:
                fv = row["fair_odds"]
                if fv >= 2:
                    row["fv_display"] = f"+{int(fv)}"
                else:
                    try:
                        row["fv_display"] = f"{int(-100 / (fv - 1))}"
                    except Exception:
                        row["fv_display"] = f"{round(fv)}"
            else:
                row["fv_display"] = "-"

            row["odds_display"] = row.get("market_odds", "-")
            row["label"] = "🔍" if row.get("is_prospective") else "🟢"
            row["skip_reason"] = bet.get("skip_reason", None)
            row["logged"] = bet.get("logged", False)
            if row.get("logged") and row.get("hours_to_game", 0) > 0:
                row["snapshot_force_include"] = True
            row["movement_confirmed"] = bet.get("movement_confirmed", False)
            row["visible_in_snapshot"] = bet.get("visible_in_snapshot", True)
            if "last_skip_reason" in bet:
                row["last_skip_reason"] = bet["last_skip_reason"]

            role = _assign_snapshot_role(row)
            row["snapshot_role"] = role
            row.setdefault("snapshot_roles", []).append(role)
            ensure_consensus_books(row)
            key = (
                row.get("game_id"),
                row.get("market"),
                row.get("side"),
                row.get("book"),
            )
            if key not in seen:
                deduped.append(row)
                seen.add(key)

    warn_missing_baselines(deduped)
    return deduped


def dispatch_snapshot_rows(
    df: pd.DataFrame,
    market_type: str,
    webhook_url: str,
    ev_range: tuple[float, float] = (5.0, 20.0),
    min_stake: float = 1.0,
    role: str | None = None,
) -> None:
    """Filter snapshot rows and send to Discord with debug logging."""

    if all(c in df.columns for c in ["game_id", "market", "side"]):
        df = df.drop_duplicates(subset=["game_id", "market", "side"])

    counts = {
        "pre_ev": len(df),
        "post_ev": 0,
        "post_stake": 0,
        "post_role": 0,
    }

    # EV%% filter
    if "EV" in df.columns:
        tmp = df["EV"].astype(str).str.replace("%", "", regex=False)
        with pd.option_context("mode.use_inf_as_na", True):
            ev_vals = pd.to_numeric(tmp, errors="coerce")
        mask_ev = (ev_vals >= ev_range[0]) & (ev_vals <= ev_range[1])
    elif "ev_percent" in df.columns:
        mask_ev = (df["ev_percent"] >= ev_range[0]) & (df["ev_percent"] <= ev_range[1])
    else:
        mask_ev = pd.Series([True] * len(df), index=df.index)
    if "logged" in df.columns and "hours_to_game" in df.columns:
        logged_mask = df["logged"] & (df["hours_to_game"] > 0)
        df = df[mask_ev | logged_mask]
    else:
        df = df[mask_ev]
    counts["post_ev"] = len(df)

    # Stake filter (prospective bets bypass minimum)
    try:
        stake_vals = None
        if "raw_kelly" in df.columns:
            stake_vals = pd.to_numeric(df["raw_kelly"], errors="coerce")
        elif "stake" in df.columns:
            stake_vals = pd.to_numeric(df["stake"], errors="coerce")
        elif "Stake" in df.columns:
            stake_vals = pd.to_numeric(
                df["Stake"].astype(str).str.replace("u", "", regex=False),
                errors="coerce",
            )
        if stake_vals is not None:
            mask = stake_vals >= min_stake
            if "is_prospective" in df.columns:
                mask = mask | df["is_prospective"]
            df = df[mask]
    except Exception:
        pass
    counts["post_stake"] = len(df)

    # Role filter
    if role:
        if "snapshot_roles" in df.columns:
            df = df[
                df["snapshot_roles"].apply(
                    lambda r: role in r if isinstance(r, list) else False
                )
            ]
    counts["post_role"] = len(df)

    warn_missing_baselines(df.to_dict("records"))

    if counts["post_role"] == 0:
        send_bet_snapshot_to_discord(
            df, market_type, webhook_url, debug_counts=counts, role=role
        )
        return

    send_bet_snapshot_to_discord(df, market_type, webhook_url, role=role)


def send_snapshot_to_discord(df: pd.DataFrame) -> None:
    """Route snapshot rows to the appropriate Discord webhooks."""
    if df is None or df.empty:
        print("⚠️ No snapshot rows to dispatch")
        return

    role_labels = {
        BEST_BOOK_MAIN: "Best Confirmed Bets",
        BEST_BOOK_ALT: "Alt Markets",
        FV_DROP: "FV Drops",
    }

    for role, webhook in DISCORD_WEBHOOK_BY_ROLE.items():
        if not webhook:
            continue
        if "snapshot_roles" not in df.columns:
            continue
        subset = df[
            df["snapshot_roles"].apply(
                lambda r: role in r if isinstance(r, list) else False
            )
        ]
        if subset.empty:
            continue
        label = role_labels.get(role, role)
        send_bet_snapshot_to_discord(subset, label, webhook, role=role)
