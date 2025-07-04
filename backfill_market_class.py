from core.pending_bets import (
    load_pending_bets,
    save_pending_bets,
    infer_market_class,
    PENDING_BETS_PATH,
)


def backfill(path: str = PENDING_BETS_PATH) -> int:
    """Backfill missing market_class fields in pending bets."""
    pending = load_pending_bets(path)
    updated = 0
    for row in pending.values():
        if not row.get("market_class"):
            row["market_class"] = infer_market_class(row.get("market"))
            updated += 1
    if updated:
        save_pending_bets(pending, path)
    return updated


if __name__ == "__main__":
    count = backfill()
    if count:
        print(f"\u2705 Backfilled 'market_class' for {count} entries.")
    else:
        print("\u2705 All entries already have 'market_class'. Nothing to patch.")
