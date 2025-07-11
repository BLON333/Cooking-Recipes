import json
import requests
from core.utils import parse_game_id
from core.logger import get_logger

logger = get_logger(__name__)

def get_park_name(game_id):
    park_by_home_team = {
        "LAA": "Angel Stadium",
        "STL": "Busch Stadium",
        "BAL": "Camden Yards",
        "ARI": "Chase Field",
        "PHI": "Citizens Bank Park",
        "NYM": "Citi Field",
        "DET": "Comerica Park",
        "COL": "Coors Field",
        "LAD": "Dodger Stadium",
        "BOS": "Fenway Park",
        "TEX": "Globe Life Field",
        "CIN": "Great American Ball Park",
        "CWS": "Guaranteed Rate Field",
        "KC": "Kauffman Stadium",
        "MIA": "loanDepot Park",
        "HOU": "Minute Maid Park",
        "WSH": "Nationals Park",
        "OAK": "Oakland Coliseum",
        "SF": "Oracle Park",
        "SD": "Petco Park",
        "PIT": "PNC Park",
        "CLE": "Progressive Field",
        "TOR": "Rogers Centre",
        "SEA": "T-Mobile Park",
        "MIN": "Target Field",
        "TB": "Tropicana Field",
        "ATL": "Truist Park",
        "CHC": "Wrigley Field",
        "NYY": "Yankee Stadium"
    }
    try:
        parsed = parse_game_id(str(game_id))
        home_abbr = parsed.get("home", "").upper()
        return park_by_home_team.get(home_abbr, "League Average")
    except Exception as e:
        print(f"[WARNING] Could not extract park from game_id '{game_id}': {e}")
        return "League Average"

def get_park_factors(park_name):
    try:
        with open("data/park_factors.json") as f:
            park_data = json.load(f)
        if park_name not in park_data:
            logger.warning(
                "⚠️ Using League Average park factors for %s", park_name
            )
        return park_data.get(park_name, park_data["League Average"])
    except Exception as e:
        logger.warning(
            "⚠️ Failed to load park factors for %s: %s", park_name, e
        )
        return {"hr_mult": 1.0, "single_mult": 1.0}

_DIR_DEG = {
    "n": 0,
    "ne": 45,
    "e": 90,
    "se": 135,
    "s": 180,
    "sw": 225,
    "w": 270,
    "nw": 315,
}


def _relative_wind_dir(direction: str, stadium_orientation: str | None) -> str:
    """Convert a compass wind direction into ``out``, ``in`` or ``cross``.

    Parameters
    ----------
    direction:
        Compass direction wind is coming *from* (e.g. ``"ne"``).
    stadium_orientation:
        Compass orientation that the ballpark faces. ``None`` will result in a
        ``neutral`` mapping.
    """

    direction = direction.lower()
    if direction in {"in", "out", "cross"}:
        return direction
    if direction in {"none", "calm", ""}:
        return "neutral"

    if stadium_orientation is None:
        return "neutral"

    wind_deg = _DIR_DEG.get(direction)
    park_deg = _DIR_DEG.get(str(stadium_orientation).lower())
    if wind_deg is None or park_deg is None:
        return "neutral"

    delta = (wind_deg - park_deg) % 360
    if delta <= 45 or delta >= 315:
        return "in"
    if 135 <= delta <= 225:
        return "out"
    return "cross"


def get_weather_hr_mult(weather_profile, stadium_orientation: str | None = None):
    direction = weather_profile.get("wind_direction", "").lower()
    speed = weather_profile.get("wind_speed", 0)

    rel = _relative_wind_dir(direction, stadium_orientation)

    if rel == "out":
        # allow a bit more juice for extreme out-blowing winds
        return 1.0 + min(speed * 0.01, 0.25)  # before: cap at 0.20
    elif rel == "in":
        return max(1.0 - speed * 0.01, 0.80)
    else:
        return 1.0

def get_noaa_weather(park_name):
    domes = {
        "Rogers Centre",        # TOR
        "Tropicana Field",      # TB
        "Chase Field",          # ARI
        "Globe Life Field",     # TEX (retractable roof)
        "loanDepot Park",       # MIA
        "Minute Maid Park",     # HOU
        "American Family Field" # MIL
    }

    if park_name in domes:
        logger.info("\U0001F30D Skipping NOAA fetch for dome stadium: %s", park_name)
        return {
            "wind_direction": "none",
            "wind_speed": 0,
            "temperature": 72,
            "humidity": 50
        }

    try:
        with open("data/stadium_locations.json") as f:
            stadiums = json.load(f)

        if park_name not in stadiums:
            logger.warning(
                "⚠️ Using League Average location for %s", park_name
            )
        location = stadiums.get(park_name, stadiums["League Average"])
        lat, lon = location["lat"], location["lon"]

        metadata_url = f"https://api.weather.gov/points/{lat},{lon}"
        meta_response = requests.get(metadata_url, timeout=5)
        meta_response.raise_for_status()
        grid_info = meta_response.json()["properties"]
        forecast_url = grid_info["forecastHourly"]

        forecast_response = requests.get(forecast_url, timeout=5)
        forecast_response.raise_for_status()
        forecast_data = forecast_response.json()["properties"]["periods"][0]

        wind_dir = forecast_data.get("windDirection", "none")
        wind_speed = int(forecast_data.get("windSpeed", "0 mph").split()[0])
        temperature = int(forecast_data.get("temperature", 70))

        humidity = 50  # fallback since NOAA doesn't expose it

        return {
            "wind_direction": wind_dir.lower(),
            "wind_speed": wind_speed,
            "temperature": temperature,
            "humidity": humidity
        }

    except Exception as e:
        logger.warning(
            "⚠️ NOAA weather fetch failed for %s: %s", park_name, e
        )
        return {
            "wind_direction": "none",
            "wind_speed": 0,
            "temperature": 70,
            "humidity": 50
        }

def compute_weather_multipliers(
    weather,
    hitter_side: str = "R",
    park_orientation: str = "center",
    stadium_orientation: str | None = None,
):
    temp = weather.get("temperature", 70)
    humidity = weather.get("humidity", 50)
    wind_dir = weather.get("wind_direction", "none").lower()
    wind_speed = weather.get("wind_speed", 0)

    rel_dir = _relative_wind_dir(wind_dir, stadium_orientation)

    temp_mult = 1.0 + 0.003 * (temp - 70)
    humidity_mult = 1.0 - 0.0015 * (humidity - 50)

    if park_orientation == "center":
        factor = 0.01
    elif park_orientation == "lf" and hitter_side == "R":
        factor = 0.015
    elif park_orientation == "rf" and hitter_side == "L":
        factor = 0.015
    else:
        factor = 0.0

    if factor == 0.0 or rel_dir not in {"in", "out"}:
        wind_angle_mult = 1.0
    elif rel_dir == "out":
        wind_angle_mult = 1.0 + factor * wind_speed
    else:  # "in"
        wind_angle_mult = 1.0 - factor * wind_speed

    adi_mult = temp_mult * humidity_mult * wind_angle_mult
    # widen allowable range slightly for extreme conditions
    adi_mult = max(0.85, min(adi_mult, 1.25))  # before: capped at 1.20

    return {
        "temp_mult": round(temp_mult, 4),
        "humidity_mult": round(humidity_mult, 4),
        "wind_angle_mult": round(wind_angle_mult, 4),
        "adi_mult": round(adi_mult, 4)
    }
