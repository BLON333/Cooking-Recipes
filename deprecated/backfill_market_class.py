from core.pending_bets import (
    load_pending_bets,
    save_pending_bets,
    PENDING_BETS_PATH,
)
from core.market_normalizer import normalize_market_key


def backfill(path: str = PENDING_BETS_PATH) -> int:
    """Backfill ``market_class`` for each pending bet entry."""
    pending = load_pending_bets(path)
    updated = 0

    for row in pending.values():
        meta = normalize_market_key(row.get("market"))
        mclass = meta.get("market_class", "main")
        if row.get("market_class") != mclass:
            row["market_class"] = mclass
            updated += 1

    save_pending_bets(pending, path)
    return updated


if __name__ == "__main__":
    count = backfill()
    print(f"\u2705 Backfilled market_class for {count} rows in {PENDING_BETS_PATH}")
