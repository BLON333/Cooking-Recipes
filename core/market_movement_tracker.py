"""Utility for detecting line movement between snapshots."""

from core.config import DEBUG_MODE, VERBOSE_MODE
from typing import Dict, Optional

# Use the canonical game_id when tracking market movement to ensure
# consistency with snapshot keys. ``build_key`` from ``snapshot_core``
# does not perform normalization, so we handle canonicalization here
# when constructing the tracker key.
from core.utils import canonical_game_id

from core.logger import get_logger
from core.constants import market_prob_increase_threshold

logger = get_logger(__name__)

MOVEMENT_THRESHOLDS = {
    "ev_percent": 0.5,
    "market_prob": 0.00001,
    "blended_fv": 1,
    "market_odds": 1,
    "stake": 0.1,
    "sim_prob": 0.1,
}


def detect_baseline_movement(curr: float | None, baseline: float | None) -> str:
    """Return directional movement relative to a baseline."""
    if curr is None or baseline is None:
        return "same"
    try:
        c = float(curr)
        b = float(baseline)
    except Exception:
        return "same"
    if c > b:
        return "up"
    if c < b:
        return "down"
    return "same"

from core.market_pricer import decimal_odds


def _compare_change(curr, prev, threshold):
    if curr is None or prev is None:
        return "same"
    try:
        if abs(float(curr) - float(prev)) < threshold:
            return "same"
        return "better" if float(curr) > float(prev) else "worse"
    except:
        return "same"


def _compare_odds(curr, prev, threshold):
    from core.market_pricer import decimal_odds
    try:
        dec_curr = decimal_odds(float(curr))
        dec_prev = decimal_odds(float(prev))
        if abs(dec_curr - dec_prev) < threshold:
            return "same"
        return "better" if dec_curr > dec_prev else "worse"
    except:
        return "same"


def _compare_fv(curr, prev, threshold):
    base = _compare_odds(curr, prev, threshold)
    return {"better": "worse", "worse": "better"}.get(base, base)


def detect_market_movement(current: Dict, prior: Optional[Dict]) -> Dict[str, object]:
    movement = {"is_new": prior is None}

    field_map = {
        "ev_movement": ("ev_percent", _compare_change),
        "mkt_movement": ("market_prob", _compare_change),
        "fv_movement": ("blended_fv", _compare_fv),
        "odds_movement": ("market_odds", _compare_odds),
        "stake_movement": ("stake", _compare_change),
        "sim_movement": ("sim_prob", _compare_change),
    }

    for move_key, (field, fn) in field_map.items():
        curr = current.get(field)
        prev = (prior or {}).get(field)
        if field == "market_prob":
            baseline = current.get("baseline_consensus_prob")
            mkt = current.get("market", "")
            hours = current.get("hours_to_game")
            if hours is None:
                logger.warning(
                    "Missing hours_to_game for %s %s; using conservative threshold 0.01",
                    current.get("game_id"),
                    mkt,
                )
                threshold = 0.01
            else:
                threshold = market_prob_increase_threshold
            # Compare current vs. baseline market_prob with threshold
            if curr is None or baseline is None:
                movement["mkt_movement"] = "same"
            else:
                try:
                    diff = float(curr) - float(baseline)
                except Exception:
                    movement["mkt_movement"] = "same"
                else:
                    if abs(diff) < threshold:
                        movement["mkt_movement"] = "same"
                    elif diff > 0:
                        movement["mkt_movement"] = "up"
                    else:
                        movement["mkt_movement"] = "down"
        else:
            threshold = MOVEMENT_THRESHOLDS.get(field, 0.001)
            movement[move_key] = fn(curr, prev, threshold)

    return movement


def track_and_update_market_movement(
    entry: Dict,
    tracker: Dict,
    reference_tracker: Optional[Dict] | None = None,
) -> Dict[str, object]:
    """Detect movement for an entry and update the tracker in one step.

    Parameters
    ----------
    entry : Dict
        Current market evaluation row.
    tracker : Dict
        Tracker to update with the new values.
    reference_tracker : Optional[Dict], optional
        Frozen snapshot used for movement comparison.  If ``None`` the
        ``tracker`` itself is used, which preserves the previous behaviour.
    """

    # Use a canonical game_id to avoid mismatches between market movement
    # tracking and snapshot lookup.  ``canonical_game_id`` normalizes team codes
    # and preserves the time component (including any doubleheader suffix).
    gid = canonical_game_id(entry.get("game_id", ""))
    key = f"{gid}:{str(entry.get('market', '')).strip()}:{str(entry.get('side', '')).strip()}"
    base = reference_tracker if reference_tracker is not None else tracker
    prior_entry = base.get(key)
    prior = prior_entry or {}

    # Determine the sportsbook for this row
    book = entry.get("book") or entry.get("best_book")
    current_raw = entry.get("_raw_sportsbook", {}) or {}

    # Lookup prior odds for the same book
    prev_raw = prior.get("raw_sportsbook") or prior.get("prev_raw_sportsbook") or {}
    prev_market_odds = None
    if isinstance(prev_raw, dict):
        prev_market_odds = prev_raw.get(book)

    # Use prior book odds for movement detection
    if prior:
        prior_for_detect = prior.copy()
        prior_for_detect["market_odds"] = prev_market_odds
    else:
        prior_for_detect = None

    movement = detect_market_movement(entry, prior_for_detect)
    entry.update(movement)
    entry["prev_market_odds"] = prev_market_odds

    existing_baseline = tracker.get(key, {}).get("baseline_consensus_prob")
    if existing_baseline is None:
        existing_baseline = prior.get("baseline_consensus_prob")

    baseline_missing = existing_baseline is None and entry.get(
        "baseline_consensus_prob"
    ) is not None

    # Only update tracker if a meaningful change occurred for an entry that is
    # already persisted in the tracker.  This prevents skipping new rows that
    # also exist in the reference tracker but haven't yet been written to the
    # main tracker.
    if (
        prior_entry is not None
        and tracker.get(key) is not None
        and movement.get("mkt_movement") == "same"
        and all(
            movement.get(k) == "same"
            for k in [
                "ev_movement",
                "fv_movement",
                "odds_movement",
                "stake_movement",
                "sim_movement",
            ]
        )
        and not baseline_missing
    ):
        return movement

    if (
        prior.get("baseline_consensus_prob") is not None
        and entry.get("baseline_consensus_prob")
        != prior.get("baseline_consensus_prob")
    ):
        logger.warning(
            "\u26a0\ufe0f Attempted overwrite of baseline_consensus_prob for %s",
            key,
        )

    tracker_entry = {
        "ev_percent": entry.get("ev_percent"),
        "blended_fv": entry.get("blended_fv"),
        "market_odds": entry.get("market_odds"),
        "stake": entry.get("stake"),
        "sim_prob": entry.get("sim_prob"),
        "market_prob": entry.get("market_prob"),
        "date_simulated": entry.get("date_simulated"),
        "best_book": entry.get("best_book"),
        "raw_sportsbook": current_raw,
        "prev_raw_sportsbook": prev_raw,
        "baseline_consensus_prob": existing_baseline
        if existing_baseline is not None
        else entry.get("baseline_consensus_prob"),
    }

    if (
        existing_baseline is not None
        and entry.get("baseline_consensus_prob") is not None
        and entry.get("baseline_consensus_prob") != existing_baseline
    ):
        logger.warning("⚠️ baseline_consensus_prob mismatch for %s", key)

    changed_fields = []

    for field, val in tracker_entry.items():
        prev_val = prior.get(field)
        if prev_val is not None and val is not None and prev_val != val:
            if DEBUG_MODE:
                print(f"\U0001F501 {field} for {key} changed: {prev_val} → {val}")
            changed_fields.append(field)

    if changed_fields and not DEBUG_MODE:
        logger.info("🔁 %s updated fields: %s", key, ", ".join(changed_fields))

    tracker[key] = tracker_entry

    return movement
