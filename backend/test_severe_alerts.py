"""Severe weather pushes: which warnings and storms ring the phone (severe_alerts.py)."""

import os
import sys
import unittest
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import severe_alerts as sa  # noqa: E402

NOW = datetime(2026, 7, 14, 13, 0, tzinfo=timezone.utc)  # 16:00 in Bucharest (EEST)


def warning(level="yellow", kind="thunderstorm", nowcast=False, minutes=90, onset_min=0, headline="Instabilitate atmosferică"):
    return {
        "id": ("anm-nowcast:" if nowcast else "urn:oid:2.49.0.") + f"{level}-{kind}-{minutes}",
        "event": "Avertizare nowcasting ANM" if nowcast else "Cod de instabilitate",
        "headline": headline, "description": None,
        "level": level, "awareness_type": kind,
        "onset": (NOW + timedelta(minutes=onset_min)).isoformat(),
        "expires": (NOW + timedelta(minutes=minutes)).isoformat(),
    }


def hours(**values):
    """Six local hours from 16:00 (forecast offset +3 h), with ``values`` in each."""
    base = datetime(2026, 7, 14, 16, 0)
    return [{"time": (base + timedelta(hours=i)).strftime("%Y-%m-%dT%H:%M"), "weather_code": 1,
             "precipitation_probability": 10, "precipitation_mm": 0, "wind_gusts_kmh": 20, "temperature": 28,
             **values} for i in range(6)]


class WarningTests(unittest.TestCase):
    def test_an_anm_nowcast_names_what_it_warns_of(self):
        ev = sa.warning_event(warning(nowcast=True, headline="Vijelie, grindină de mari dimensiuni"), NOW)
        self.assertEqual(ev["title"], "Cod galben: vijelie și grindină")
        self.assertIn("ANM nowcasting, până la 17:30.", ev["body"])
        self.assertEqual(ev["level"], "ATENȚIE")
        self.assertEqual(ev["cooldown_hours"], sa.NOWCAST_COOLDOWN_H)

    def test_a_yellow_county_warning_needs_the_forecast_to_agree(self):
        w = warning(minutes=6 * 60, headline="Cod galben de instabilitate atmosferică")
        self.assertIsNone(sa.warning_event(w, NOW))                       # no forecast at hand
        self.assertIsNone(sa.warning_event(w, NOW, hourly=hours(), utc_offset_s=3 * 3600))
        backed = sa.warning_event(w, NOW, hourly=hours(weather_code=95, precipitation_probability=55), utc_offset_s=3 * 3600)
        self.assertIsNotNone(backed)
        self.assertEqual(backed["title"], "Cod galben: furtună")

    def test_wind_is_backed_by_gusts(self):
        w = warning(kind="wind", minutes=300, headline="Intensificări ale vântului")
        self.assertIsNone(sa.warning_event(w, NOW, hourly=hours(wind_gusts_kmh=40), utc_offset_s=3 * 3600))
        self.assertIsNotNone(sa.warning_event(w, NOW, hourly=hours(wind_gusts_kmh=65), utc_offset_s=3 * 3600))

    def test_orange_and_red_go_out_on_their_own(self):
        ev = sa.warning_event(warning(level="orange", kind="wind", headline="Vânt puternic"), NOW)
        self.assertEqual((ev["title"], ev["level"], ev["bypass_quiet"]), ("Cod portocaliu: vânt puternic", "EVITĂ", False))
        self.assertTrue(sa.warning_event(warning(level="red"), NOW)["bypass_quiet"])

    def test_heat_and_fog_only_from_orange(self):
        self.assertIsNone(sa.warning_event(warning(kind="high-temperature", nowcast=True), NOW))
        self.assertIsNotNone(sa.warning_event(warning(level="orange", kind="high-temperature", headline="Caniculă"), NOW))

    def test_an_expired_warning_or_an_unknown_colour_is_dropped(self):
        self.assertIsNone(sa.warning_event(warning(nowcast=True, minutes=-5), NOW))
        self.assertIsNone(sa.warning_event({**warning(nowcast=True), "level": "green"}, NOW))

    def test_a_later_start_is_said(self):
        ev = sa.warning_event(warning(level="orange", onset_min=120, minutes=600), NOW)
        self.assertIn("de la 18:00 până la 02:00", ev["body"].replace("mâine ", ""))

    def test_each_warning_has_its_own_key(self):
        a = sa.warning_event(warning(nowcast=True, minutes=60), NOW)
        b = sa.warning_event(warning(nowcast=True, minutes=90), NOW)
        self.assertEqual(a["type"], b["type"])
        self.assertNotEqual(a["key"], b["key"])


class LightningTests(unittest.TestCase):
    def strike(self, near, count, nearest=4.0):
        return {"count": count, "near_count": near, "nearest_km": nearest, "bearing_deg": 270, "last_minutes_ago": 3}

    def test_a_stray_flash_rings_nothing(self):
        self.assertIsNone(sa.lightning_event(self.strike(1, 1), NOW))
        self.assertIsNone(sa.lightning_event(self.strike(2, 3), NOW))

    def test_a_cluster_close_by_is_a_storm(self):
        ev = sa.lightning_event(self.strike(4, 9), NOW)
        self.assertEqual(ev["title"], "Furtună în apropiere")
        self.assertIn("la 4 km spre vest", ev["body"])

    def test_the_arbiter_must_agree_when_it_spoke(self):
        self.assertIsNone(sa.lightning_event(self.strike(4, 9), NOW, {"state": "none"}))
        self.assertIsNotNone(sa.lightning_event(self.strike(4, 9), NOW, {"state": "storm"}))

    def test_a_wide_storm_counts_when_near_enough(self):
        self.assertIsNotNone(sa.lightning_event(self.strike(0, 12, nearest=15), NOW))
        self.assertIsNone(sa.lightning_event(self.strike(0, 12, nearest=28), NOW))

    def test_one_key_per_window(self):
        a = sa.lightning_event(self.strike(4, 9), NOW)
        b = sa.lightning_event(self.strike(4, 9), NOW + timedelta(minutes=50))
        self.assertEqual(a["key"], b["key"])


class TogetherTests(unittest.TestCase):
    def test_a_nowcast_storm_needs_no_second_lightning_ring(self):
        events = sa.severe_events([warning(nowcast=True)], {"count": 9, "near_count": 5, "nearest_km": 3}, NOW)
        self.assertEqual([e["type"] for e in events], ["severe-anm-thunderstorm-yellow"])

    def test_worst_first(self):
        events = sa.severe_events([warning(nowcast=True), warning(level="red", kind="wind")], None, NOW)
        self.assertEqual([e["value"] for e in events], ["red", "yellow"])

    def test_the_setting(self):
        self.assertTrue(sa.enabled({}))
        self.assertTrue(sa.enabled({"alert_states": '{"rain": false}'}))
        self.assertTrue(sa.enabled({"alert_states": "not json"}))
        self.assertFalse(sa.enabled({"alert_states": '{"severe": false}'}))


if __name__ == "__main__":
    unittest.main()
