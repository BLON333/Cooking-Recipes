import json
import logging
from datetime import datetime, timedelta

# Keep output noise low for CI environments
logging.getLogger().setLevel("WARNING")

# Minimal helper utilities to avoid external dependencies
def build_tracker_key(game_id: str, market: str, side: str) -> str:
    """Return a simple normalized key."""
    return f"{game_id}:{market}:{side}"

def annotate_display_deltas(entry: dict, prior: dict | None) -> None:
    """Populate ``mkt_prob_display`` using prior market_prob value."""
    def fmt_prob(val: float | None) -> str:
        return "N/A" if val is None else f"{val * 100:.1f}%"

    prev = entry.get("prev_market_prob")
    if prev is None and prior:
        prev = prior.get("market_prob")
    curr = entry.get("market_prob")
    if prev is not None and curr is not None and abs(curr - prev) > 1e-6:
        entry["mkt_prob_display"] = f"{fmt_prob(prev)} → {fmt_prob(curr)}"
    else:
        entry["mkt_prob_display"] = fmt_prob(curr)



def run_snapshot_persistence_test() -> None:
    game1 = "2025-09-01-NYY@BOS"
    market1 = "totals"
    side1 = "Under 9.5"

    game2 = "2025-09-01-LAD@SFN"
    market2 = "spreads"
    side2 = "SFN +1.5"

    # baseline values stored in pending_bets.json
    pending_bets = {
        build_tracker_key(game1, market1, side1): {
            "baseline_consensus_prob": 0.45,
            "logged": False,
        },
        build_tracker_key(game2, market2, side2): {
            "baseline_consensus_prob": 0.55,
            "logged": True,
        },
    }

    tracker: dict = {}
    snapshot_cache: dict = {}
    last_seen: dict = {}

    base_time = datetime(2025, 9, 1, 12, 0, 0)
    loop_times = [base_time + timedelta(minutes=5 * i) for i in range(3)]

    fv_market_probs = [0.46, 0.48, 0.475]
    logged_probs = [0.55, 0.54, 0.52]
    def detect_movement_and_update(row):
        key = build_tracker_key(row["game_id"], row["market"], row["side"])
        prev = tracker.get(key)
        if prev:
            row["prev_market_prob"] = prev.get("market_prob")
            diff = row["market_prob"] - prev.get("market_prob", 0)
            if abs(diff) < 1e-6:
                row["mkt_movement"] = "same"
            elif diff > 0:
                row["mkt_movement"] = "up"
            else:
                row["mkt_movement"] = "down"
        else:
            row["mkt_movement"] = "same"
        tracker[key] = {"market_prob": row["market_prob"]}
        return prev


    for idx, ts in enumerate(loop_times):
        rows = []
        row_fv = {
            "game_id": game1,
            "market": market1,
            "side": side1,
            "market_prob": fv_market_probs[idx],
            "sim_prob": 0.6,
            "market_odds": -110,
            "blended_fv": -105,
            "stake": 1.0,
            "hours_to_game": 5,
            "logged": False,
            "best_book": "fanduel",
            "_raw_sportsbook": {"fanduel": -110},
            "date_simulated": ts.isoformat(),
        }
        row_logged = {
            "game_id": game2,
            "market": market2,
            "side": side2,
            "market_prob": logged_probs[idx],
            "sim_prob": 0.58,
            "market_odds": 130,
            "blended_fv": -115,
            "stake": 1.0,
            "hours_to_game": 2,
            "logged": True,
            "best_book": "draftkings",
            "_raw_sportsbook": {"draftkings": 130},
            "date_simulated": ts.isoformat(),
        }
        rows.extend([row_fv, row_logged])

        output_rows = []
        for r in rows:
            key = build_tracker_key(r["game_id"], r["market"], r["side"])
            baseline = pending_bets.get(key, {}).get("baseline_consensus_prob")
            if baseline is None:
                baseline = snapshot_cache.get(key, {}).get(
                    "baseline_consensus_prob", r["market_prob"]
                )
            r["baseline_consensus_prob"] = baseline

            prior = detect_movement_and_update(r)
            annotate_display_deltas(r, prior)

            visible = False
            if r.get("logged") and r.get("hours_to_game", 0) > 0:
                visible = True
            elif idx == 0 or r.get("mkt_movement") == "up":
                # Show FV Drop bet on first loop and whenever market moves up
                last_seen[key] = ts
                visible = True
            else:
                last_ts = last_seen.get(key)
                if last_ts and ts - last_ts <= timedelta(minutes=30):
                    visible = True
            r["visible_in_snapshot"] = visible
            if visible:
                snapshot_cache[key] = r
            output_rows.append(r)

        print(f"\n--- Snapshot Loop {idx + 1} ({ts.isoformat()}) ---")
        for r in output_rows:
            short = {
                "game_id": r["game_id"],
                "market": r["market"],
                "side": r["side"],
                "market_prob": r["market_prob"],
                "mkt_prob_display": r.get("mkt_prob_display"),
                "mkt_movement": r.get("mkt_movement"),
                "logged": r.get("logged"),
                "visible": r.get("visible_in_snapshot"),
                "baseline_consensus_prob": r.get("baseline_consensus_prob"),
            }
            print(json.dumps(short, indent=2))

        fv_key = build_tracker_key(game1, market1, side1)
        log_key = build_tracker_key(game2, market2, side2)
        if not snapshot_cache.get(fv_key):
            print("WARNING: FV Drop bet disappeared prematurely!")
        if not snapshot_cache.get(log_key):
            print("WARNING: Logged bet disappeared prematurely!")


if __name__ == "__main__":
    run_snapshot_persistence_test()
