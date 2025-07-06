import random
import json
from datetime import datetime, timedelta


# We purposely keep this script standalone. It generates fake bets and
# walks through several snapshot loops to validate baseline anchoring
# and sticky visibility logic.

def run_snapshot_loop_audit() -> None:
    random.seed(42)

    # generate 20 fake bets with fixed baseline consensus probabilities
    bets = []
    for i in range(20):
        bet = {
            "game_id": f"GAME{i+1:02d}",
            "market": "totals" if i % 2 == 0 else "spreads",
            "side": f"Bet Side {i+1}",
            "baseline_consensus_prob": round(random.uniform(0.4, 0.6), 3),
            "logged": i % 3 == 0,
            # some bets already started (negative hours_to_game)
            "hours_to_game": random.randint(-2, 5),
        }
        bets.append(bet)

    snapshot_cache = {}
    last_seen = {}
    movement_history = {(b["game_id"], b["market"], b["side"]): [] for b in bets}

    base_time = datetime(2025, 1, 1, 12, 0, 0)
    loop_times = [base_time + timedelta(minutes=5 * i) for i in range(6)]

    for loop_idx, ts in enumerate(loop_times):
        counts = {"up": 0, "down": 0, "same": 0, "visible": 0, "logged": 0}
        sample_rows = []
        for b in bets:
            key = (b["game_id"], b["market"], b["side"])
            base_prob = b["baseline_consensus_prob"]
            curr_prob = max(0, min(1, base_prob + random.uniform(-0.05, 0.05)))
            diff = curr_prob - base_prob
            if abs(diff) < 1e-3:
                movement = "same"
            elif diff > 0:
                movement = "up"
            else:
                movement = "down"
            mkt_prob_display = f"{base_prob:.3f} → {curr_prob:.3f}"

            visible = False
            sticky = False
            if b.get("logged") and b.get("hours_to_game", 0) > 0:
                visible = True
            elif movement == "up":
                visible = True
                last_seen[key] = ts
            else:
                last_ts = last_seen.get(key)
                if last_ts and ts - last_ts <= timedelta(minutes=30):
                    visible = True
                    sticky = True
            if visible:
                snapshot_cache[key] = ts
            movement_history[key].append(movement)

            counts[movement] += 1
            if b.get("logged"):
                counts["logged"] += 1
            if visible:
                counts["visible"] += 1

            row = {
                "game_id": b["game_id"],
                "side": b["side"],
                "mkt_prob_display": mkt_prob_display,
                "mkt_movement": movement,
                "visible": visible,
                "sticky": sticky,
            }
            sample_rows.append(row)

        print(f"\n--- Snapshot Loop {loop_idx + 1} ({ts.isoformat()}) ---")
        print(
            f"Movements -> up: {counts['up']}, down: {counts['down']}, same: {counts['same']}"
        )
        print(f"Visible bets: {counts['visible']}  Logged bets: {counts['logged']}")
        for r in sample_rows[:5]:
            print(json.dumps(r, indent=2))

    never_moved = [
        key
        for key, hist in movement_history.items()
        if all(m == "same" for m in hist)
    ]
    if never_moved:
        print(f"WARNING: {len(never_moved)} bets never moved across loops")


if __name__ == "__main__":
    run_snapshot_loop_audit()
