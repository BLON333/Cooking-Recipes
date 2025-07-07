from core.utils import (
    normalize_market_key as _normalize_market_key_str,
    classify_market_segment,
    get_segment_label,
    normalize_lookup_side,
)


def normalize_market_key(
    market: str,
    *,
    side: str | None = None,
    game_id: str | None = None,
    market_odds: dict | None = None,
    debug: bool = False,
) -> dict:
    """Return metadata derived from a market key."""
    market = market or ""
    mkey = _normalize_market_key_str(market)
    segment = classify_market_segment(mkey)
    market_class = "alternate" if "alternate" in market else "main"
    label = get_segment_label(mkey, "")

    if debug:
        found = False
        if isinstance(market_odds, dict):
            found = mkey in market_odds
        print(
            f"[Normalize Debug] game_id={game_id} market={market} side={side} -> {mkey} | found={found}"
        )

    return {"segment": segment, "market_class": market_class, "label": label}


def normalize_side(side: str) -> str:
    """Normalize side label for lookup."""
    return normalize_lookup_side(side)
