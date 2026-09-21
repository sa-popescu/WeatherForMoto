"""Tests for the added data sources: official stations, airports, ANM nowcasting, rain fusion, verification log."""

from __future__ import annotations

import unittest
from datetime import datetime, timezone

import anm_nowcast
import metar
import official_stations
import rain_fusion
import verification
import verify_report
from weather_service import (
    _build_hourly,
    _ensemble_by_time,
    _fuse_rain,
    _merge_current,
    _source_status,
    _verification_snapshot,
)
from test_scoring import make_ensemble_payload, make_om_payload

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



def ensemble_payload(times: list[str], wet_runs: int, runs: int = 20, amount: float = 1.5) -> dict:
    """Open-Meteo ensemble shape: one key per run, the first `wet_runs` with rain."""
    hourly: dict = {"time": times}
    for run in range(runs):
        key = "precipitation_icon_eu_eps" if run == 0 else f"precipitation_member{run:02d}_icon_eu_eps"
        hourly[key] = [amount if run < wet_runs else 0.0] * len(times)
    return {"hourly": hourly}


class RainFusionTests(unittest.TestCase):
    def test_the_ensemble_probability_is_the_share_of_wet_runs(self) -> None:
        votes = rain_fusion.ensemble_votes(ensemble_payload(["2026-09-14T15:00"], wet_runs=5))
        vote = votes["2026-09-14T15:00"]
        self.assertEqual(vote["probability"], 0.25)
        self.assertEqual(vote["members"], 20)
        self.assertEqual(vote["range"], (0.0, 1.5))

    def test_too_few_runs_and_a_dry_ensemble(self) -> None:
        self.assertEqual(rain_fusion.ensemble_votes(ensemble_payload(["2026-09-14T15:00"], 1, runs=5)), {})
        dry = rain_fusion.ensemble_votes(ensemble_payload(["2026-09-14T15:00"], 0))["2026-09-14T15:00"]
        self.assertEqual((dry["probability"], dry["range"]), (0.0, None))

    def test_provider_hours_are_moved_to_local_time(self) -> None:
        offset = 3 * 3600
        pirate = {"hourly": {"data": [{"time": 1789387200, "precipProbability": 0.6, "precipIntensity": 0.8}]}}
        self.assertEqual(rain_fusion.pirate_votes(pirate, offset),
                         {"2026-09-14T15:00": {"probability": 0.6, "amount": 0.8}})
        met = {"properties": {"timeseries": [
            {"time": "2026-09-14T12:00:00Z", "data": {"next_1_hours": {"details": {"precipitation_amount": 0.4}}}},
            {"time": "2026-09-14T18:00:00Z", "data": {"next_6_hours": {"details": {"precipitation_amount": 3.0}}}},
        ]}}
        self.assertEqual(rain_fusion.met_votes(met, offset),
                         {"2026-09-14T15:00": {"probability": 1.0, "amount": 0.4}})

    def test_owm_spreads_its_block_over_three_hours(self) -> None:
        owm = {"list": [{"dt": 1789387200, "pop": 0.3, "rain": {"3h": 0.9}}]}
        votes = rain_fusion.owm_votes(owm, 3 * 3600)
        self.assertEqual(sorted(votes), ["2026-09-14T14:00", "2026-09-14T15:00", "2026-09-14T16:00"])
        self.assertAlmostEqual(votes["2026-09-14T15:00"]["amount"], 0.3)

    def test_the_hour_is_the_weighted_mean_of_the_votes(self) -> None:
        fused = rain_fusion.fuse_hour(
            open_meteo_probability=20, wet_models=4, rain_models=6,
            votes={"ensemble": {"probability": 0.5, "range": (0.0, 2.0), "members": 122},
                   "pirate-weather": {"probability": 0.9}, "openweathermap": None, "met-norway": None},
        )
        # (0.2 * 1.5 + 4/6 * 2 + 0.5 * 3 + 0.9 * 1) / 7.5
        self.assertEqual(fused["probability"], 54)
        # Open-Meteo dry, 4 of 6 models wet, ensemble wet (50 %), Pirate wet.
        self.assertEqual((fused["wet_sources"], fused["total_sources"]), (6, 9))
        self.assertEqual(fused["range"], (0.0, 2.0))

    def test_no_source_no_answer(self) -> None:
        self.assertIsNone(rain_fusion.fuse_hour(open_meteo_probability=None, votes={"ensemble": None}))

    def test_fused_hours_are_rescored(self) -> None:
        om = make_om_payload(precipitation=[1.2] * 48, precipitation_probability=[10] * 48, weather_code=[61] * 48)
        times = om["hourly"]["time"]
        ensemble = _ensemble_by_time(make_ensemble_payload(times, precipitation=[[1.2] * 48, [1.0] * 48]))
        hourly = _build_hourly(om, ensemble)
        before = hourly[12]["moto_score"]
        _fuse_rain(hourly, ensemble, rain_ensemble=ensemble_payload(times, wet_runs=20), pirate=None,
                   owm_forecast=None, met=None, utc_offset_seconds=3 * 3600)
        self.assertGreater(hourly[12]["precipitation_probability"], 10)
        self.assertEqual(hourly[12]["rain_sources"], {"wet": 3, "total": 4})
        self.assertLess(hourly[12]["moto_score"], before)



class VerificationTests(unittest.TestCase):
    def test_snapshot_logs_the_lead_hours_and_what_was_measured(self) -> None:
        om = make_om_payload()
        hourly = _build_hourly(om)
        times = om["hourly"]["time"]
        rain_by_source = {times[11]: {"ensemble": 0.4, "open-meteo": 0.1}}
        snapshot = _verification_snapshot(
            cell=(44.45, 26.1), om_data=om, hourly=hourly, rain_by_source=rain_by_source,
            stations={"official": {"temp": 17.2, "precipitation": 0.4, "station": "Afumati", "distance_km": 9.0,
                                   "observed_at": "2024-06-01T07:00:00Z"},
                      "weatherxm": None},
            metar_obs={"station": "LROP", "distance_km": 12.0, "wmo_code": 61, "observed_at": "2024-06-01T07:00:00Z"},
            now_utc=datetime(2024, 6, 1, 7, 15, tzinfo=timezone.utc),
        )
        self.assertEqual(snapshot["cell"], "44.45,26.10")
        self.assertEqual(snapshot["issued_hour"], "2024-06-01T07")
        leads = [row["lead_h"] for row in snapshot["forecasts"]]
        self.assertEqual(leads, [1, 3, 6, 12, 24])  # 48 h is past the payload's end
        first = snapshot["forecasts"][0]
        # The payload's clock reads 10:15 local, so lead 1 is 11:00 local, 08:00 UTC in a Bucharest summer.
        self.assertEqual(first["valid_hour"], "2024-06-01T08")
        self.assertEqual((first["prob_ensemble"], first["prob_open_meteo"], first["prob_pirate_weather"]), (0.4, 0.1, None))
        self.assertEqual(first["om_temp"], 18.0)
        by_source = {row["source"]: row for row in snapshot["observations"]}
        self.assertEqual(sorted(by_source), ["metar", "official"])
        self.assertEqual((by_source["official"]["raining"], by_source["official"]["temp"]), (1, 17.2))
        self.assertEqual(by_source["metar"]["raining"], 1)

    def test_one_snapshot_per_cell_and_hour(self) -> None:
        verification._seen.clear()
        self.assertTrue(verification._first_time(("44.45,26.10", "2024-06-01T07")))
        self.assertFalse(verification._first_time(("44.45,26.10", "2024-06-01T07")))
        self.assertTrue(verification._first_time(("44.45,26.10", "2024-06-01T08")))

    def test_record_outside_an_event_loop_does_nothing(self) -> None:
        verification._seen.clear()
        verification.record({"cell": "x", "issued_hour": "h", "forecasts": [], "observations": []})


    def test_report_scores_each_source_against_what_fell(self) -> None:
        def row(ensemble: float, open_meteo: float, raining: int) -> dict:
            base = {f"prob_{n.replace('-', '_')}": None for n in verification.RAIN_SOURCES}
            return {**base, "lead_h": 1, "precip_prob": 50, "prob_ensemble": ensemble,
                    "prob_open_meteo": open_meteo, "raining": raining}
        # Half the hours wet; the ensemble calls them right, Open-Meteo says 50 % every time.
        rows = [row(0.9, 0.5, 1), row(0.1, 0.5, 0)] * 20
        scores = verify_report.rain_scores(rows)
        brier, skill, pairs = scores["ensemble"]["0-3 h"]
        self.assertAlmostEqual(brier, 0.01)
        self.assertAlmostEqual(skill, 0.96)
        self.assertEqual(pairs, 40)
        self.assertAlmostEqual(scores["open-meteo"]["0-3 h"][1], 0.0)
        weights = verify_report.suggested_weights(scores)
        self.assertEqual(weights["ensemble"], rain_fusion.WEIGHTS["ensemble"])
        self.assertEqual(weights["open-meteo"], 0.0)

    def test_report_compares_final_and_raw_temperature(self) -> None:
        rows = [{"lead_h": 3, "temp": 16.0, "om_temp": 19.0, "measured": 15.0}] * 3
        scores = verify_report.temp_scores(rows)
        self.assertEqual(scores["final"]["0-3 h"], (1.0, 1.0, 3))
        self.assertEqual(scores["open-meteo raw"]["0-3 h"], (4.0, 4.0, 3))


if __name__ == "__main__":
    unittest.main()
