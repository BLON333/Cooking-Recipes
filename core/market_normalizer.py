from core.utils import (
    normalize_market_key as _normalize_market_key_str,
    classify_market_segment,
    get_segment_label,
    normalize_lookup_side,
)


def normalize_market_key(market: str) -> dict:
    """Return metadata derived from a market key."""
    market = market or ""
    mkey = _normalize_market_key_str(market)
    segment = classify_market_segment(mkey)
    market_class = "alternate" if "alternate" in market else "main"
    label = get_segment_label(mkey, "")
    return {"segment": segment, "market_class": market_class, "label": label}


def normalize_side(side: str) -> str:
    """Normalize side label for lookup."""
    return normalize_lookup_side(side)
