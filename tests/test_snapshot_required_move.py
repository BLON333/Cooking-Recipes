import json
from pathlib import Path

from core.confirmation_utils import required_market_move, extract_book_count


def test_snapshot_required_move_alignment():
    path = Path(__file__).parent / "data" / "sample_snapshot_row.json"
    row = json.loads(path.read_text())

    recomputed = required_market_move(
        hours_to_game=row["hours_to_game"],
        book_count=extract_book_count(row),
        market=row.get("market"),
        ev_percent=row.get("ev_percent"),
    )

    assert round(row["required_move"], 5) == round(recomputed, 5)
