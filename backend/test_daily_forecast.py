"""
Tests for daily_forecast: the next 24 hours told in Romanian sentences.

Run from backend/:
    python -m unittest test_daily_forecast -v
"""

import os
import sys
import unittest
from datetime import datetime, timedelta
from typing import Any

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import daily_forecast as df  # noqa: E402

MORNING = datetime(2027, 5, 10, 8, 5)
EVENING = datetime(2027, 5, 10, 20, 5)


def _hour(ts: datetime, **values: Any) -> dict[str, Any]:
    hour = {
        "time": ts.strftime("%Y-%m-%dT%H:%M"),
        "temperature": 18.0,
        "feels_like": 18.0,
        "precipitation_mm": 0.0,
        "precipitation_probability": 0,
        "wind_speed_kmh": 8.0,
        "wind_gusts_kmh": 15.0,
        "weather_code": 1,
        "visibility": 20000.0,
        "frost_risk": False,
        "wet_road": None,
        "moto_score": 88,
    }
    hour.update(values)
    return hour


def _weather(now: datetime, overrides: dict[int, dict[str, Any]] | None = None) -> dict[str, Any]:
    """30 hours from the current hour; ``overrides`` keyed by hours from now."""
    start = now.replace(minute=0)
    hourly = [_hour(start + timedelta(hours=i)) for i in range(30)]
    for rel, values in (overrides or {}).items():
        hourly[rel].update(values)
    return {"hourly": hourly}


def _body(now: datetime, overrides: dict[int, dict[str, Any]] | None = None) -> str:
    digest = df.build_digest(_weather(now, overrides), now, "Sibiu")
    assert digest is not None
    return digest.body


class DigestTests(unittest.TestCase):
    def test_a_fine_dry_day(self) -> None:
        temps = {i: {"temperature": 10.0 + i} for i in range(0, 8)}
        digest = df.build_digest(_weather(MORNING, temps), MORNING, "Sibiu")
        self.assertEqual(digest.title, "Prognoza zilei · Sibiu")
        self.assertIn("Temperaturi între 10 și 18°C", digest.body)
        self.assertIn("Nu se anunță precipitații.", digest.body)
        self.assertIn("Zi ideală de motor, toată ziua", digest.body)
        self.assertNotIn("mâine", digest.body.split("Nu se")[0])

    def test_a_rainy_afternoon_is_one_run_with_its_chance(self) -> None:
        rain = {i: {"precipitation_mm": 1.5, "precipitation_probability": 80, "weather_code": 61, "moto_score": 40}
                for i in (7, 8, 9)}
        body = _body(MORNING, rain)
        self.assertIn("Ploaie slabă între 15:00 și 18:00, cu 80% șanse.", body)

    def test_one_dry_hour_does_not_split_the_rain(self) -> None:
        rain = {i: {"precipitation_mm": 3.0, "precipitation_probability": 70, "weather_code": 63} for i in (4, 6)}
        self.assertIn("Ploaie moderată între 12:00 și 15:00", _body(MORNING, rain))

    def test_thunder_is_named(self) -> None:
        storm = {5: {"precipitation_mm": 6.0, "precipitation_probability": 70, "weather_code": 95}}
        self.assertIn("Averse cu descărcări electrice în jurul orei 13:00", _body(MORNING, storm))

    def test_a_trace_at_low_chance_is_not_rain(self) -> None:
        body = _body(MORNING, {6: {"precipitation_mm": 0.1, "precipitation_probability": 20}})
        self.assertIn("Nu se anunță precipitații.", body)

    def test_a_middling_chance_without_a_rainy_hour(self) -> None:
        body = _body(MORNING, {9: {"precipitation_probability": 35}})
        self.assertIn("Șanse mici de ploaie, de cel mult 35%, pe la 17:00.", body)

    def test_gusts_and_the_cold_they_bring(self) -> None:
        body = _body(MORNING, {2: {"wind_gusts_kmh": 65, "wind_speed_kmh": 30, "feels_like": 12.0}})
        self.assertIn("Rafale puternice, de până la 65 km/h, pe la 10:00.", body)
        self.assertIn("Din cauza vântului, pe la 10:00 se simte ca 12°C.", body)

    def test_cold_without_wind_is_not_blamed_on_the_wind(self) -> None:
        body = _body(MORNING, {1: {"feels_like": 13.0}})
        self.assertIn("Pe la 09:00 se simte ca 13°C.", body)
        self.assertNotIn("vântului", body)

    def test_frost_fog_and_wet_road(self) -> None:
        body = _body(MORNING, {
            0: {"frost_risk": True, "visibility": 400.0},
            1: {"frost_risk": True, "visibility": 600.0},
        })
        self.assertIn("Risc de polei între 08:00 și 10:00.", body)
        self.assertIn("Ceață între 08:00 și 10:00.", body)
        wet = _body(MORNING, {3: {"wet_road": "wet"}, 4: {"wet_road": "wet"}})
        self.assertIn("Asfaltul rămâne ud între 11:00 și 13:00.", wet)

    def test_a_poor_day_for_riding(self) -> None:
        poor = {i: {"moto_score": 35} for i in range(30)}
        self.assertIn("Nu e o zi bună de motor: scorul nu trece de 35/100.", _body(MORNING, poor))

    def test_a_single_good_hour(self) -> None:
        poor = {i: {"moto_score": 35} for i in range(30)}
        poor[5] = {"moto_score": 75}
        self.assertIn("De mers cu motorul e bine doar în jurul orei 13:00 (scor 75/100).", _body(MORNING, poor))

    def test_in_the_evening_tomorrow_goes_bare_and_tonight_is_named(self) -> None:
        digest = df.build_digest(_weather(EVENING, {
            1: {"precipitation_mm": 1.0, "precipitation_probability": 90, "weather_code": 61},
            15: {"wind_gusts_kmh": 45},
        }), EVENING, "Sibiu")
        self.assertEqual(digest.title, "Prognoza pentru mâine · Sibiu")
        self.assertIn("Ploaie slabă diseară în jurul orei 21:00", digest.body)
        self.assertIn("Vânt în rafale de până la 45 km/h pe la 11:00.", digest.body)

    def test_in_the_morning_tomorrows_hours_say_so(self) -> None:
        body = _body(MORNING, {18: {"precipitation_mm": 1.0, "precipitation_probability": 90, "weather_code": 61}})
        self.assertIn("Ploaie slabă mâine în jurul orei 02:00", body)

    def test_negative_temperatures(self) -> None:
        cold = {i: {"temperature": -3.0 + i * 0.5, "feels_like": -3.0 + i * 0.5} for i in range(30)}
        self.assertIn("Temperaturi între -3 și 8°C", _body(MORNING, cold))

    def test_too_few_hours_give_no_digest(self) -> None:
        weather = {"hourly": _weather(MORNING)["hourly"][:4]}
        self.assertIsNone(df.build_digest(weather, MORNING, "Sibiu"))

    def test_facts_carry_what_the_text_says(self) -> None:
        rain = {7: {"precipitation_mm": 1.5, "precipitation_probability": 80, "weather_code": 61}}
        facts = df.build_digest(_weather(MORNING, rain), MORNING, "Sibiu").facts
        self.assertEqual(facts["precipitation"]["runs"][0]["probability"], 80)
        self.assertEqual(facts["temperature"]["max"], 18)


if __name__ == "__main__":
    unittest.main()
