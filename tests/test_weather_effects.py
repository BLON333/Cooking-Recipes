from assets.env_builder import get_weather_hr_mult, compute_weather_multipliers


def test_weather_hr_mult_neutral_south():
    profile = {"wind_direction": "s", "wind_speed": 20}
    assert get_weather_hr_mult(profile) == 1.0


def test_weather_multipliers_neutral_west():
    profile = {"wind_direction": "w", "wind_speed": 15, "temperature": 70, "humidity": 50}
    result = compute_weather_multipliers(profile)
    assert result["adi_mult"] == 1.0

