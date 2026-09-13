"""Meteoalarm CAP parsing and area matching. No network: the feed is a string."""

import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import meteoalarm  # noqa: E402

NOW = datetime(2026, 9, 13, 8, 0, tzinfo=timezone.utc)

CAP_WITH_POLYGON = """<?xml version="1.0" encoding="UTF-8"?>
<alert xmlns="urn:oasis:names:tc:emergency:cap:1.2">
  <identifier>2.49.0.0.642.0.RO.260913080000</identifier>
  <sender>anm@meteoromania.ro</sender>
  <info>
    <language>ro-RO</language>
    <event>Vânt puternic</event>
    <severity>Severe</severity>
    <senderName>Administrația Națională de Meteorologie</senderName>
    <headline>Cod portocaliu de vânt</headline>
    <description>Rafale de 90 km/h în zona montană.</description>
    <instruction>Evitați drumurile deschise.</instruction>
    <onset>2026-09-13T09:00:00+03:00</onset>
    <expires>2026-09-13T21:00:00+03:00</expires>
    <parameter><valueName>awareness_level</valueName><value>3; orange; Severe</value></parameter>
    <parameter><valueName>awareness_type</valueName><value>1; Wind</value></parameter>
    <area>
      <areaDesc>Prahova</areaDesc>
      <polygon>45.6,25.5 45.6,26.2 45.0,26.2 45.0,25.5 45.6,25.5</polygon>
    </area>
  </info>
  <info>
    <language>en-GB</language>
    <event>Strong wind</event>
    <severity>Severe</severity>
    <onset>2026-09-13T09:00:00+03:00</onset>
    <expires>2026-09-13T21:00:00+03:00</expires>
    <area><areaDesc>Prahova</areaDesc><polygon>45.6,25.5 45.6,26.2 45.0,26.2 45.0,25.5 45.6,25.5</polygon></area>
  </info>
</alert>
"""

ATOM_WITH_NAMED_AREA = """<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom" xmlns:cap="urn:oasis:names:tc:emergency:cap:1.2">
  <entry>
    <id>meteoalarm-ro-1</id>
    <cap:info>
      <cap:language>ro-RO</cap:language>
      <cap:event>Ploi torențiale</cap:event>
      <cap:severity>Moderate</cap:severity>
      <cap:expires>2026-09-13T18:00:00+03:00</cap:expires>
      <cap:parameter><cap:valueName>awareness_level</cap:valueName><cap:value>2; yellow; Moderate</cap:value></cap:parameter>
      <cap:parameter><cap:valueName>awareness_type</cap:valueName><cap:value>10; Rain</cap:value></cap:parameter>
      <cap:area><cap:areaDesc>Brașov</cap:areaDesc></cap:area>
    </cap:info>
  </entry>
</feed>
"""

EXPIRED = CAP_WITH_POLYGON.replace("2026-09-13T21:00:00+03:00", "2026-09-13T07:00:00+03:00")


class ParsingTests(unittest.TestCase):
    def test_reads_a_cap_alert(self) -> None:
        warnings = meteoalarm.parse_feed(CAP_WITH_POLYGON)
        self.assertTrue(warnings)
        first = warnings[0]
        self.assertEqual(first["event"], "Vânt puternic")
        self.assertEqual(first["level"], "orange")
        self.assertEqual(first["awareness_type"], "wind")
        self.assertEqual(first["sender"], "Administrația Națională de Meteorologie")
        self.assertEqual(first["areas"][0]["description"], "Prahova")

    def test_prefers_romanian_when_the_feed_is_bilingual(self) -> None:
        warnings = meteoalarm.parse_feed(CAP_WITH_POLYGON)
        self.assertEqual([w["event"] for w in warnings], ["Vânt puternic"])

    def test_reads_an_atom_entry_carrying_cap_fields(self) -> None:
        warnings = meteoalarm.parse_feed(ATOM_WITH_NAMED_AREA)
        self.assertEqual(len(warnings), 1)
        self.assertEqual(warnings[0]["level"], "yellow")
        self.assertEqual(warnings[0]["awareness_type"], "rain")

    def test_junk_is_skipped_rather_than_raised(self) -> None:
        self.assertEqual(meteoalarm.parse_feed("not xml at all"), [])
        self.assertEqual(meteoalarm.parse_feed("<feed></feed>"), [])

    def test_severity_stands_in_for_a_missing_level(self) -> None:
        without_level = CAP_WITH_POLYGON.replace(
            "<parameter><valueName>awareness_level</valueName><value>3; orange; Severe</value></parameter>", ""
        )
        self.assertEqual(meteoalarm.parse_feed(without_level)[0]["level"], "orange")


class MatchingTests(unittest.TestCase):
    def test_a_point_inside_the_polygon_is_covered(self) -> None:
        found = meteoalarm.warnings_for(CAP_WITH_POLYGON, 45.18, 25.66, "Breaza", now=NOW)
        self.assertEqual(len(found), 1)
        self.assertEqual(found[0]["match"], "geometry")
        self.assertEqual(found[0]["areas"], ["Prahova"])

    def test_a_point_outside_the_polygon_is_not(self) -> None:
        self.assertEqual(meteoalarm.warnings_for(CAP_WITH_POLYGON, 47.15, 27.58, "Iași", now=NOW), [])

    def test_a_named_area_matches_the_place_without_diacritics(self) -> None:
        found = meteoalarm.warnings_for(ATOM_WITH_NAMED_AREA, 45.65, 25.6, "Brasov", now=NOW)
        self.assertEqual(len(found), 1)
        self.assertEqual(found[0]["match"], "name")

    def test_a_named_area_elsewhere_is_ignored(self) -> None:
        self.assertEqual(meteoalarm.warnings_for(ATOM_WITH_NAMED_AREA, 44.43, 26.1, "București", now=NOW), [])

    def test_expired_warnings_are_dropped(self) -> None:
        self.assertEqual(meteoalarm.warnings_for(EXPIRED, 45.18, 25.66, "Breaza", now=NOW), [])

    def test_a_circle_covers_its_radius(self) -> None:
        circle = CAP_WITH_POLYGON.replace(
            "<polygon>45.6,25.5 45.6,26.2 45.0,26.2 45.0,25.5 45.6,25.5</polygon>",
            "<circle>45.18,25.66 20</circle>",
        )
        self.assertEqual(meteoalarm.warnings_for(circle, 45.2, 25.7, "Breaza", now=NOW)[0]["match"], "geometry")
        self.assertEqual(meteoalarm.warnings_for(circle, 46.5, 25.7, "Altundeva", now=NOW), [])

    def test_the_worst_colour_comes_first(self) -> None:
        both = CAP_WITH_POLYGON.replace("</alert>", "</alert>")
        orange = meteoalarm.warnings_for(both, 45.18, 25.66, "Breaza", now=NOW)
        self.assertEqual(orange[0]["level"], "orange")
        self.assertLess(meteoalarm.LEVEL_ORDER["red"], meteoalarm.LEVEL_ORDER["yellow"])

    def test_geometry_wins_over_a_name_that_does_not_match(self) -> None:
        found = meteoalarm.warnings_for(CAP_WITH_POLYGON, 45.18, 25.66, "", now=NOW)
        self.assertEqual(found[0]["match"], "geometry")


class HelperTests(unittest.TestCase):
    def test_fold_ignores_diacritics_and_case(self) -> None:
        self.assertEqual(meteoalarm.fold("Brașov"), meteoalarm.fold("brasov"))
        self.assertEqual(meteoalarm.fold(None), "")

    def test_point_in_polygon_on_a_square(self) -> None:
        square = [(0.0, 0.0), (0.0, 2.0), (2.0, 2.0), (2.0, 0.0)]
        self.assertTrue(meteoalarm.point_in_polygon(1.0, 1.0, square))
        self.assertFalse(meteoalarm.point_in_polygon(3.0, 1.0, square))


if __name__ == "__main__":
    unittest.main(verbosity=2)
