from assets.env_builder import get_park_name


def test_game_id_maps_to_park():
    """Game ID should resolve to the correct park name."""
    game_id = "2025-07-10-ARI@LAA-T1905"
    assert get_park_name(game_id) == "Angel Stadium"
