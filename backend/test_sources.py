"""Tests for the added data sources: official stations, airports, ANM nowcasting."""

from __future__ import annotations

import unittest
from datetime import datetime, timezone

import anm_nowcast
import metar
import official_stations
from weather_service import _merge_current, _source_status
from test_scoring import make_om_payload

NOW = datetime(2026, 9, 14, 8, 12, tzinfo=timezone.utc)


def coverage(station: str, lon: float, lat: float, times: list[str], ranges: dict[str, list[float | None]]) -> dict:
    """A CoverageJSON point series shaped like MeteoGate's (September 2026)."""
    return {
        "type": "Coverage",
        "domain": {"type": "Domain", "domainType": "PointSeries",
                   "axes": {"x": {"values": [lon]}, "y": {"values": [lat]}, "t": {"values": times}}},
        "ranges": {name: {"type": "NdArray", "values": values} for name, values in ranges.items()},
        "metocean:wigosId": station,
    }


HOURS = ["2026-09-14T06:00:00Z", "2026-09-14T07:00:00Z", "2026-09-14T08:00:00Z"]
BUCHAREST = (44.43, 26.10)


def stations_payload() -> dict:
    return {
        "ref_lat": BUCHAREST[0],
        "ref_lon": BUCHAREST[1],
        "coverages": [
            # Filaret, 2 km away: temperature and humidity, but its gust series stopped yesterday.
            coverage("0-20000-0-15422", 26.09361, 44.41195, HOURS, {
                "air_temperature:2.0:point:PT0S": [17.0, 18.5, 19.7],
                "relative_humidity:2.0:point:PT0S": [80, 75, None],
                "wind_speed:10.0:point:PT10M": [1.0, 2.0, 2.5],
                "precipitation_amount:1.4:sum:PT1H": [0.0, 0.2, 0.0],
            }),
            coverage("0-20000-0-15422", 26.09361, 44.41195, ["2026-09-13T15:00:00Z"], {
                "wind_speed_of_gust:10.0:point:PT10M": [9.0],
            }),
            # Baneasa, 9 km away, reports a fresh gust.
            coverage("0-20000-0-15420", 26.2111, 44.5008, HOURS, {
                "air_temperature:2.0:point:PT0S": [16.0, 17.5, 18.9],
                "wind_speed_of_gust:10.0:point:PT10M": [4.0, 5.0, 3.0],
                "air_pressure_at_mean_sea_level:0.0:point:PT0S": [1019, 1019, 1018.6],
            }),
            # Far outside the reach of a rider in Bucharest.
            coverage("0-20000-0-15480", 28.65, 44.17, HOURS, {"air_temperature:2.0:point:PT0S": [20, 21, 22]}),
        ],
        "locations": [
            {"type": "Feature", "id": "0-20000-0-15422", "properties": {"name": "BUCURESTI_FILARET"}},
            {"type": "Feature", "id": "0-20000-0-15420", "properties": {"name": "BUCURESTI_BANEASA"}},
        ],
    }


class OfficialStationsTests(unittest.TestCase):
    def test_takes_each_field_from_the_nearest_station_that_reports_it_fresh(self) -> None:
        obs = official_stations.normalize(stations_payload(), now=NOW)
        self.assertIsNotNone(obs)
        assert obs is not None
        self.assertEqual(obs["temp"], 19.7)
        self.assertEqual(obs["station"], "Bucuresti Filaret")
        self.assertAlmostEqual(obs["distance_km"], 2.1, delta=0.3)
        self.assertEqual(obs["observed_at"], "2026-09-14T08:00:00Z")
        # Humidity: the last known value at Filaret is from 07:00, still fresh.
        self.assertEqual(obs["humidity"], 75)
        # m/s turned into km/h; the gust comes from Baneasa, since Filaret's is a day old.
        self.assertEqual(obs["wind_speed_kmh"], 9.0)
        self.assertEqual(obs["wind_gusts_kmh"], 10.8)
        self.assertEqual(obs["pressure"], 1018.6)
        self.assertEqual(obs["precipitation"], 0.0)

    def test_nothing_fresh_or_nothing_close_gives_no_reading(self) -> None:
        self.assertIsNone(official_stations.normalize(stations_payload(), now=datetime(2026, 9, 14, 12, 0, tzinfo=timezone.utc)))
        far = stations_payload()
        far["ref_lat"], far["ref_lon"] = 46.77, 23.59
        self.assertIsNone(official_stations.normalize(far, now=NOW))
        self.assertIsNone(official_stations.normalize({"ref_lat": 44, "ref_lon": 26, "coverages": [{"broken": True}], "locations": []}, now=NOW))


class MetarTests(unittest.TestCase):
    def test_present_weather_codes(self) -> None:
        self.assertEqual(metar.weather_code("-RA"), 61)
        self.assertEqual(metar.weather_code("+SHRA"), 82)
        self.assertEqual(metar.weather_code("TSRA"), 95)
        self.assertEqual(metar.weather_code("FG"), 45)
        self.assertEqual(metar.weather_code("FZFG"), 48)
        self.assertEqual(metar.weather_code("-FZRA BR"), 66)
        self.assertIsNone(metar.weather_code("BR HZ"))
        self.assertIsNone(metar.weather_code("VCSH"))
        # A storm beats the freezing drizzle reported with it.
        self.assertEqual(metar.weather_code("-FZDZ TS"), 95)

    def test_visibility_in_metres(self) -> None:
        self.assertEqual(metar.visibility_m("6+"), 10_000)
        self.assertEqual(metar.visibility_m(0.25), 402)
        self.assertIsNone(metar.visibility_m("n/a"))

    def test_nearest_fresh_report_within_reach(self) -> None:
        reports = [
            {"icaoId": "LROP", "name": "Bucharest/Coandă Intl", "lat": 44.572, "lon": 26.102, "wxString": None, "visib": "6+",
             "reportTime": "2026-09-14T08:00:00.000Z"},
            {"icaoId": "LRBS", "name": "Bucharest/Băneasa Intl, B, RO", "lat": 44.511, "lon": 26.078, "wxString": "-RA", "visib": 3,
             "reportTime": "2026-09-14T08:00:00.000Z"},
            {"icaoId": "LRTC", "name": "Tulcea Arpt", "lat": 45.065, "lon": 28.716, "wxString": "TSRA", "visib": "6+",
             "reportTime": "2026-09-14T08:00:00.000Z"},
        ]
        obs = metar.nearest(reports, *BUCHAREST, now=NOW)
        assert obs is not None
        self.assertEqual(obs["station"], "LRBS")
        self.assertEqual(obs["name"], "Bucharest/Băneasa Intl")
        self.assertEqual(obs["wmo_code"], 61)
        self.assertEqual(obs["visibility_m"], 4828)
        self.assertIsNone(metar.nearest(reports, *BUCHAREST, now=datetime(2026, 9, 14, 10, 0, tzinfo=timezone.utc)))
        self.assertIsNone(metar.nearest(reports, 46.77, 23.59, now=NOW))


# Shape of the real ANM GIS feed (trimmed); the polygon is a small square around
# Tulcea in Web Mercator metres.
def square_3857(lat: float, lon: float, half_deg: float) -> str:
    import math

    def x(lon_: float) -> float:
        return lon_ * 20037508.342789244 / 180

    def y(lat_: float) -> float:
        return math.log(math.tan(math.pi / 4 + math.radians(lat_) / 2)) * 20037508.342789244 / math.pi

    s, n, w, e = lat - half_deg, lat + half_deg, lon - half_deg, lon + half_deg
    return f"MULTIPOLYGON ((({x(w)} {y(s)}, {x(e)} {y(s)}, {x(e)} {y(n)}, {x(w)} {y(n)}, {x(w)} {y(s)})))"


NOWCAST_XML = f"""<?xml version="1.0" encoding="UTF-8"?>
<avertizare numarAvertizare="278" avertizareId="38847743" avertizareNivelCod="GA" avertizareNivelDenumire="Galben"
 entitateOrganizatorica="Serviciul Regional de Prognoza a Vremii Constanta" dataInceput="2026-09-14 10:35" dataSfarsit="2026-09-14 11:35"
 fenomenAvertizat="Local vor fi averse și se vor acumula cantități de apă de 15...25 l/mp." fenomeneAsociate=""
 useCoordsGis="true" coordsGis="{square_3857(45.18, 28.80, 0.2)}" srid="3857">
 <zona_afectata text="Județul Tulcea: Tulcea, Nufăru, Mahmudia;"/>
</avertizare>"""


class AnmNowcastTests(unittest.TestCase):
    def test_reads_the_warning_in_romanian_time_with_its_drawn_area(self) -> None:
        [warning] = anm_nowcast.parse_feed(NOWCAST_XML)
        self.assertEqual(warning["level"], "yellow")
        self.assertEqual(warning["awareness_type"], "rain")
        self.assertEqual(warning["onset"], "2026-09-14T10:35:00+03:00")
        self.assertEqual(len(warning["areas"][0]["polygons"]), 1)
        lat, lon = warning["areas"][0]["polygons"][0][0]
        self.assertAlmostEqual(lat, 44.98, places=2)
        self.assertAlmostEqual(lon, 28.60, places=2)

    def test_only_inside_the_drawn_area_and_only_until_it_expires(self) -> None:
        during = datetime(2026, 9, 14, 7, 50, tzinfo=timezone.utc)
        inside = anm_nowcast.warnings_for(NOWCAST_XML, 45.18, 28.80, "Tulcea", now=during)
        self.assertEqual([w["match"] for w in inside], ["geometry"])
        self.assertEqual(inside[0]["areas"], ["Județul Tulcea: Tulcea, Nufăru, Mahmudia"])
        # Named in the zone text but outside the polygon: not warned.
        self.assertEqual(anm_nowcast.warnings_for(NOWCAST_XML, 45.60, 29.60, "Nufăru", now=during), [])
        after = datetime(2026, 9, 14, 8, 40, tzinfo=timezone.utc)
        self.assertEqual(anm_nowcast.warnings_for(NOWCAST_XML, 45.18, 28.80, "Tulcea", now=after), [])

    def test_romanian_summer_and_winter_time(self) -> None:
        self.assertEqual(anm_nowcast.local_time("2026-01-10 12:00").utcoffset().total_seconds(), 7200)
        self.assertEqual(anm_nowcast.local_time("2026-07-10 12:00").utcoffset().total_seconds(), 10800)
        self.assertIsNone(anm_nowcast.local_time("mâine"))
        self.assertEqual(anm_nowcast.parse_feed("not xml"), [])


class MergeTests(unittest.TestCase):
    def test_official_station_weighs_in_and_an_airport_storm_sets_the_code(self) -> None:
        om = make_om_payload()
        without = _merge_current(om, None, None)
        station = official_stations.normalize(stations_payload(), now=NOW)
        airport = {"station": "LRBS", "name": "Băneasa", "distance_km": 9.2, "observed_at": "2026-09-14T08:00:00Z",
                   "wmo_code": 95, "visibility_m": 4000.0}
        merged = _merge_current(om, None, None, official_norm=station, metar_obs=airport)
        self.assertNotEqual(merged["temperature"], without["temperature"])
        self.assertEqual(merged["visibility_km"], 4.0)
        self.assertIn("official-stations", merged["sources"])
        self.assertIn("metar", merged["sources"])

    def test_source_status_explains_each_source(self) -> None:
        current = {"model_count": 5}
        statuses = _source_status(
            current=current,
            keys={"openweathermap": True, "pirate-weather": False, "weatherxm": True, "netatmo": False},
            used={"openweathermap": True, "met-norway": False, "pirate-weather": False, "weatherxm": False, "netatmo": False},
            stations=({"coverages": []}, None),
            airports=(None, None),
            warnings={"anm-nowcast": ("<xml/>", 1), "meteoalarm": (None, 0)},
        )
        by_id = {s["id"]: s for s in statuses}
        self.assertEqual(by_id["open-meteo"]["status"], "used")
        self.assertEqual(by_id["model-ensemble"]["models"], 5)
        self.assertEqual(by_id["openweathermap"]["status"], "used")
        self.assertEqual(by_id["met-norway"]["status"], "no-data")
        self.assertEqual(by_id["pirate-weather"]["status"], "off")
        self.assertEqual(by_id["official-stations"]["status"], "none-nearby")
        self.assertEqual(by_id["metar"]["status"], "no-data")
        self.assertEqual(by_id["netatmo"]["status"], "off")
        self.assertEqual((by_id["anm-nowcast"]["status"], by_id["anm-nowcast"]["count"]), ("used", 1))
        self.assertEqual(by_id["meteoalarm"]["status"], "no-data")


if __name__ == "__main__":
    unittest.main()
