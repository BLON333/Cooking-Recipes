import builtins
import logging
from assets import env_builder


def test_get_park_factors_missing_file_logs_warning(monkeypatch, caplog):
    def fake_open(*args, **kwargs):
        raise FileNotFoundError("missing")
    monkeypatch.setattr(builtins, "open", fake_open)
    with caplog.at_level(logging.WARNING):
        result = env_builder.get_park_factors("Angel Stadium")
    assert result == {"hr_mult": 1.0, "single_mult": 1.0}
    assert any("Failed to load park factors" in r.message for r in caplog.records)


def test_get_noaa_weather_missing_file_logs_warning(monkeypatch, caplog):
    def fake_open(*args, **kwargs):
        raise FileNotFoundError("missing")
    monkeypatch.setattr(builtins, "open", fake_open)
    with caplog.at_level(logging.WARNING):
        weather = env_builder.get_noaa_weather("Angel Stadium")
    assert weather["wind_direction"] == "none"
    assert any("NOAA weather fetch failed" in r.message for r in caplog.records)
