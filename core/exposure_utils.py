from core.theme_key_utils import make_theme_key
from core.theme_utils import normalize_market_key, normalize_segment
from core.utils import TEAM_ABBR_TO_NAME, TEAM_NAME_TO_ABBR


def remap_side_key(side: str) -> str:
    """Expand team abbreviations and preserve Over/Under labels."""
    # If already a full team name (e.g., 'Pittsburgh Pirates'), keep it
    if side in TEAM_NAME_TO_ABBR:
        return side

    # Check for abbreviation + number (like 'PIT+0.5' or 'MIA-1.5')
    for abbr, full_name in TEAM_ABBR_TO_NAME.items():
        if side.startswith(abbr):
            rest = side[len(abbr):].strip()
            return f"{full_name} {rest}".strip()

    # If it's an Over/Under line like 'Over 4.5', 'Under 7.0', leave unchanged
    if side.startswith("Over") or side.startswith("Under"):
        return side

    # Fallback — if unknown, return side as-is
    return side


def get_exposure_key(row: dict) -> str:
    """Return a key for exposure tracking based on market and side."""
    market = row["market"]
    game_id = row["game_id"]
    side = remap_side_key(row["side"])

    market_type = normalize_market_key(market)
    if market_type not in {"total", "spread", "h2h"}:
        market_type = "other"

    segment = normalize_segment(market)

    for team in TEAM_NAME_TO_ABBR:
        if side.startswith(team):
            theme = team
            break
    else:
        if "Over" in side:
            theme = "Over"
        elif "Under" in side:
            theme = "Under"
        else:
            theme = "Other"

    theme_key = f"{theme}_{market_type}"
    return make_theme_key(game_id, theme_key, segment)
