import types
import core.unified_snapshot_generator as usg


def test_baseline_persists_across_runs(monkeypatch):
    date = "2025-01-01"

    def fake_load_simulations(_):
        return {"GAME1": {}}

    def fake_build_snapshot_rows(sim_data, odds_json, min_ev=0.01):
        return [
            {
                "game_id": "GAME1",
                "market": "totals",
                "side": "Over 8.5",
                "consensus_prob": 0.55,
                "market_prob": 0.55,
                "blended_prob": 0.55,
                "blended_fv": 1 / 0.55,
                "market_odds": -110,
                "ev_percent": 6.0,
                "stake": 1.0,
                "raw_kelly": 1.0,
                "hours_to_game": 5,
                "best_book": "fanduel",
                "_raw_sportsbook": {"fanduel": -110},
            }
        ]

    def fake_expand(rows, allowed_books):
        return rows

    def fake_consensus(game_id, odds_data, market, side, debug=False):
        return {"consensus_prob": odds_data[game_id][market][side]["consensus_prob"]}, "mock"

    monkeypatch.setattr(usg, "load_simulations", fake_load_simulations)
    monkeypatch.setattr(usg, "build_snapshot_rows", fake_build_snapshot_rows)
    monkeypatch.setattr(usg, "expand_snapshot_rows_with_kelly", fake_expand)
    monkeypatch.setattr(usg, "calculate_consensus_prob", fake_consensus)

    canon_id = usg.canonical_game_id("GAME1")
    odds1 = {canon_id: {"totals": {"Over 8.5": {"consensus_prob": 0.45}}}}
    prior_map = {}
    first = usg.build_snapshot_for_date(date, odds1, (0.0, 10.0), prior_map=prior_map)
    assert first and first[0]["baseline_consensus_prob"] == 0.45

    prior_map = {
        (
            canon_id,
            first[0]["market"],
            first[0]["side"],
            first[0].get("book", first[0].get("best_book")),
        ): first[0]
    }
    odds2 = {canon_id: {"totals": {"Over 8.5": {"consensus_prob": 0.50}}}}
    second = usg.build_snapshot_for_date(date, odds2, (0.0, 10.0), prior_map=prior_map)
    assert second and second[0]["baseline_consensus_prob"] == 0.45

