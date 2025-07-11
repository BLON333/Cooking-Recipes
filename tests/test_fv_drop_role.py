import core.unified_snapshot_generator as usg


def test_fv_drop_role_assignment():
    row = {
        "game_id": "GAME1",
        "market": "totals",
        "side": "Over 8.5",
        "market_prob": 0.55,
        "baseline_consensus_prob": 0.50,
        "ev_percent": 6.0,
        "stake": 1.0,
        "hours_to_game": 5,
        "market_odds": -110,
        "blended_prob": 0.6,
        "market_class": "main",
        "book": "fanduel",
    }
    usg._enrich_snapshot_row(row)
    assert "fv_drop" in row.get("snapshot_roles", [])

