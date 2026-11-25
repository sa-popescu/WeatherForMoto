"""Saharan dust from CAMS (weather_service._dust_outlook) and the air quality of the hour we are in."""

import unittest

import weather_service as ws

NOW = {"current": {"time": "2026-11-24T14:15"}}


def air(dust: list[float], start_hour: int = 0, **extra: list[float]) -> dict:
    times = [f"2026-11-24T{h:02d}:00" if h < 24 else f"2026-11-25T{h - 24:02d}:00"
             for h in range(start_hour, start_hour + len(dust))]
    return {"hourly": {"time": times, "dust": dust, **extra}}


def hours(rain_at: dict[str, tuple[float, int]]) -> list[dict]:
    out = []
    for h in range(0, 48):
        slot = f"2026-11-24T{h:02d}:00" if h < 24 else f"2026-11-25T{h - 24:02d}:00"
        mm, chance = rain_at.get(slot, (0.0, 5))
        out.append({"time": slot, "precipitation_mm": mm, "precipitation_probability": chance})
    return out


class DustTests(unittest.TestCase):
    def test_clean_air_says_nothing(self) -> None:
        self.assertIsNone(ws._dust_outlook(air([10.0] * 48), NOW, hours({})))
        self.assertIsNone(ws._dust_outlook(None, NOW, hours({})))

    def test_dust_that_already_passed_does_not_count(self) -> None:
        # 400 at 03:00, clean from 14:00 on.
        dust = [400.0 if h == 3 else 5.0 for h in range(48)]
        self.assertIsNone(ws._dust_outlook(air(dust), NOW, hours({})))

    def test_peak_level_and_muddy_rain(self) -> None:
        dust = [5.0] * 48
        for h in range(18, 30):
            dust[h] = 90.0
        dust[22] = 180.0
        rain = {"2026-11-24T16:00": (2.0, 80),   # rain before the dust: clean rain
                "2026-11-24T20:00": (0.1, 90),   # too little to count
                "2026-11-24T21:00": (1.5, 60)}
        out = ws._dust_outlook(air(dust), NOW, hours(rain))
        self.assertEqual(out["level"], "strong")
        self.assertEqual((out["peak_ug"], out["peak_time"]), (180, "2026-11-24T22:00"))
        self.assertEqual(out["now_ug"], 5)
        self.assertEqual(out["muddy_rain_at"], "2026-11-24T21:00")

    def test_beyond_a_day_is_left_out(self) -> None:
        dust = [5.0] * 48
        dust[40] = 300.0  # tomorrow 16:00, 26 h ahead
        self.assertIsNone(ws._dust_outlook(air(dust), NOW, hours({})))

    def test_notable_without_rain(self) -> None:
        out = ws._dust_outlook(air([60.0] * 48), NOW, hours({}))
        self.assertEqual((out["level"], out["muddy_rain_at"]), ("notable", None))


class AirHourTests(unittest.TestCase):
    def test_air_quality_is_read_at_the_current_hour_not_midnight(self) -> None:
        aqi = [float(h) for h in range(48)]
        om_air = air([5.0] * 48, european_aqi=aqi, grass_pollen=[0.0] * 48)
        om_data = {"current": {"time": "2026-11-24T14:15", "temperature_2m": 10}, "hourly": {"time": []}}
        current = ws._merge_current(om_data, None, None, om_air, hourly=[])
        self.assertEqual(current["eu_aqi"], 14.0)
        self.assertIsNone(current["dust"])


if __name__ == "__main__":
    unittest.main()
