from __future__ import annotations

from collections import defaultdict
from datetime import datetime

from core.should_log_bet import should_log_bet


def evaluate_snapshot_row_for_logging(
    row: dict,
    theme_stakes: dict,
    eval_tracker: dict,
    existing: dict,
) -> dict | None:
    """Evaluate a snapshot row and write to CSV if it qualifies.

    Parameters
    ----------
    row : dict
        Snapshot row to evaluate.
    theme_stakes : dict
        Current theme exposure totals.
    eval_tracker : dict
        Tracker used for movement confirmation.
    existing : dict
        Existing stakes keyed by ``(game_id, market, side)``.

    Returns
    -------
    dict | None
        Result from :func:`write_to_csv` when the bet is logged or the
        evaluation dictionary when skipped. ``None`` if evaluation failed.
    """
    evaluation = should_log_bet(
        row.copy(),
        theme_stakes,
        verbose=False,
        eval_tracker=eval_tracker,
        existing_csv_stakes=existing,
    )

    if not evaluation:
        return None

    if evaluation.get("log") and not evaluation.get("skip_reason"):
        from cli.log_betting_evals import (
            write_to_csv,
            record_successful_log,
        )
        session_exposure = defaultdict(set)
        result = write_to_csv(
            evaluation,
            "logs/market_evals.csv",
            existing,
            session_exposure,
            theme_stakes,
        )
        if result and not result.get("skip_reason"):
            record_successful_log(result, existing, theme_stakes)
        return result

    return evaluation
