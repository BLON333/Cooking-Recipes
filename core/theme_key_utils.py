"""Utility functions for theme key handling."""

from typing import Tuple

from core.logger import get_logger


def make_theme_key(game_id: str, theme: str, segment: str | None = "") -> str:
    """Return a canonical theme key string.

    ``segment`` is optional. When empty or ``"full_game"``, the key omits the
    trailing segment portion.
    """
    seg = segment or ""
    if seg == "full_game":
        logger = get_logger(__name__)
        logger.warning("make_theme_key received 'full_game' segment")
        seg = ""

    return f"{game_id}::{theme}" if not seg else f"{game_id}::{theme}::{seg}"


def parse_theme_key(key: str) -> Tuple[str, str, str]:
    """Parse a theme key string into its components."""
    parts = key.split("::", 2)
    if len(parts) == 2:
        game_id, theme = parts
        segment = ""
    elif len(parts) >= 3:
        game_id, theme, segment = parts[0], parts[1], parts[2]
    else:
        # malformed key
        parts += ["", "", ""]
        game_id, theme, segment = parts[:3]

    if segment == "full_game":
        segment = ""
    return game_id, theme, segment


def theme_key_equals(a: str, b: str) -> bool:
    """Return True if two theme key strings represent the same key."""
    return parse_theme_key(a) == parse_theme_key(b)