"""Tests for what the verification log teaches (calibration.py). No network, no database."""

import sys
import time
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))

import calibration  # noqa: E402
import rain_fusion  # noqa: E402

SHARP = [(0.9, 1), (0.1, 0)] * 60   # right, and says so
VAGUE = [(0.5, 1), (0.5, 0)] * 60   # always "maybe"


class ScoreTests(unittest.TestCase):
    def test_brier_rewards_being_right_and_sure(self) -> None:
        self.assertLess(calibration.brier(SHARP), calibration.brier(VAGUE))
        self.assertEqual(calibration.brier([(1.0, 1), (0.0, 0)]), 0.0)
        self.assertIsNone(calibration.brier([]))

    def test_skill_is_measured_against_the_local_base_rate(self) -> None:
        self.assertGreater(calibration.skill(SHARP), 0.9)
        self.assertAlmostEqual(calibration.skill(VAGUE), 0.0, places=6)

    def test_nothing_to_beat_is_not_skill(self) -> None:
        # It rained every hour in the sample: any forecast "wins", so none counts.
        self.assertIsNone(calibration.skill([(0.5, 1)] * 50))


class WeightTests(unittest.TestCase):
    DEFAULTS = {"ensemble": 3.0, "models": 2.0}

    def test_the_source_that_is_right_here_gains_and_the_chorus_stays_as_loud(self) -> None:
        learned = calibration.learned_weights({"ensemble": SHARP, "models": VAGUE}, self.DEFAULTS)
        self.assertGreater(learned["ensemble"], self.DEFAULTS["ensemble"])
        self.assertLess(learned["models"], self.DEFAULTS["models"])
        self.assertAlmostEqual(sum(learned.values()), sum(self.DEFAULTS.values()), places=6)

    def test_too_few_pairs_keeps_the_hand_set_weight(self) -> None:
        few = SHARP[: calibration.MIN_SOURCE_PAIRS - 1]
        self.assertEqual(calibration.learned_weights({"ensemble": few}, self.DEFAULTS), self.DEFAULTS)
        self.assertEqual(calibration.learned_weights({}, self.DEFAULTS), self.DEFAULTS)

    def test_every_real_source_is_covered_by_the_defaults(self) -> None:
        # A source that can be logged but has no weight would silently vanish.
        import verification
        for name in verification.RAIN_SOURCES:
            self.assertIn(name, rain_fusion.WEIGHTS)


class ReliabilityTests(unittest.TestCase):
    # 210 hours called 40%, of which 30 rained; 50 called 80%, all of which did.
    PAIRS = [(0.4, 0)] * 180 + [(0.4, 1)] * 30 + [(0.8, 1)] * 50

    def test_a_thin_log_teaches_nothing(self) -> None:
        self.assertEqual(calibration.reliability_table(self.PAIRS[:10]), ())
        self.assertEqual(calibration.calibrated(40, ()), 40)

    def test_bins_carry_what_was_forecast_and_what_happened(self) -> None:
        table = calibration.reliability_table(self.PAIRS)
        self.assertEqual(len(table), 2)
        self.assertAlmostEqual(table[0][0], 0.4, places=3)
        self.assertAlmostEqual(table[0][1], 30 / 210, places=3)

    def test_a_percentage_moves_towards_what_those_hours_did(self) -> None:
        table = calibration.reliability_table(self.PAIRS)
        moved = calibration.calibrated(40, table)
        # 40% that rained 14% of the time lands between the two, not on either.
        self.assertLess(moved, 40)
        self.assertGreater(moved, 14)

    def test_outside_the_bins_the_correction_is_held_flat(self) -> None:
        # Below the lowest bin there is nothing measured to interpolate towards,
        # so that bin's observed rate is reused; the forecast itself still moves.
        table = ((0.4, 0.2), (0.8, 0.7))
        share = calibration.LEARNED_SHARE
        for pct in (0, 10, 30):
            expected = round((pct / 100 * (1 - share) + 0.2 * share) * 100, 1)
            self.assertEqual(calibration.calibrated(pct, table), expected)
        self.assertEqual(calibration.calibrated(100, table),
                         round((1.0 * (1 - share) + 0.7 * share) * 100, 1))

    def test_nothing_is_ever_pushed_out_of_range(self) -> None:
        table = ((0.1, 0.0), (0.9, 1.0))
        for value in (0, 1, 50, 99, 100):
            moved = calibration.calibrated(value, table)
            self.assertGreaterEqual(moved, 0)
            self.assertLessEqual(moved, 100)
        self.assertIsNone(calibration.calibrated(None, table))


class SnapshotTests(unittest.TestCase):
    def setUp(self) -> None:
        calibration.reset_for_tests()

    tearDown = setUp

    def test_until_something_is_learned_the_hand_set_weights_stand(self) -> None:
        snapshot = calibration.current(rain_fusion.WEIGHTS)
        self.assertEqual(snapshot.weights, rain_fusion.WEIGHTS)
        self.assertFalse(snapshot.learned)
        self.assertEqual(snapshot.as_meta()["reliability"], [])

    def test_compute_reads_the_joined_rows(self) -> None:
        rows = [
            {"precip_prob": 90, "prob_ensemble": 0.9, "prob_models": 0.5, "raining": 1},
            {"precip_prob": 10, "prob_ensemble": 0.1, "prob_models": 0.5, "raining": 0},
        ] * 60
        snapshot = calibration.compute(rows, ("ensemble", "models"),
                                       {"ensemble": 3.0, "models": 2.0})
        self.assertEqual(snapshot.pairs, 120)
        self.assertGreater(snapshot.weights["ensemble"], 3.0)
        self.assertTrue(snapshot.learned)

    def test_rows_without_a_measurement_are_skipped(self) -> None:
        rows = [{"precip_prob": 50, "prob_ensemble": 0.5, "raining": None}] * 100
        snapshot = calibration.compute(rows, ("ensemble",), {"ensemble": 3.0})
        self.assertEqual(snapshot.pairs, 0)
        self.assertEqual(snapshot.weights, {"ensemble": 3.0})

    def test_refresh_without_an_event_loop_does_nothing(self) -> None:
        calibration.refresh_soon("2026-01-01T00", ("ensemble",), {"ensemble": 3.0})
        self.assertFalse(calibration.current({"ensemble": 3.0}).learned)


class StoredSnapshotTests(unittest.TestCase):
    """The snapshot kept in app_state, so the log is read once a day and not per process."""

    DEFAULTS = {"ensemble": 3.0, "models": 2.0}
    NOW = 1_800_000_000.0

    def _snapshot(self, age_s: float = 3600.0) -> calibration.Snapshot:
        return calibration.Snapshot(
            weights={"ensemble": 3.4, "models": 1.6},
            reliability=((0.1, 0.05), (0.5, 0.38)),
            pairs=7306,
            computed_at=self.NOW - age_s,
        )

    def test_what_was_stored_comes_back_the_same(self) -> None:
        original = self._snapshot()
        text = calibration.to_stored(original, self.DEFAULTS)
        back = calibration.from_stored(text, self.DEFAULTS, self.NOW)
        self.assertIsNotNone(back)
        self.assertEqual(back.as_meta(), original.as_meta())
        self.assertEqual(back.computed_at, original.computed_at)

    def test_a_day_old_snapshot_is_read_again_from_the_log(self) -> None:
        text = calibration.to_stored(self._snapshot(age_s=calibration.REFRESH_S + 1), self.DEFAULTS)
        self.assertIsNone(calibration.from_stored(text, self.DEFAULTS, self.NOW))

    def test_a_deploy_that_changes_the_hand_set_weights_does_not_reuse_the_old_answer(self) -> None:
        text = calibration.to_stored(self._snapshot(), self.DEFAULTS)
        self.assertIsNone(calibration.from_stored(text, {"ensemble": 2.5, "models": 2.0}, self.NOW))

    def test_nothing_stored_or_nonsense_stored_means_read_the_log(self) -> None:
        for text in (None, "", "not json", "[]", '{"computed_at": "soon"}', '{"settings": 1}'):
            self.assertIsNone(calibration.from_stored(text, self.DEFAULTS, self.NOW), text)


class RefreshTests(unittest.TestCase):
    """Which database work a refresh does. The database itself is faked."""

    DEFAULTS = {"ensemble": 3.0, "models": 2.0}
    SOURCES = ("ensemble", "models")
    ROWS = [
        {"precip_prob": 90, "prob_ensemble": 0.9, "prob_models": 0.5, "raining": 1},
        {"precip_prob": 10, "prob_ensemble": 0.1, "prob_models": 0.5, "raining": 0},
    ] * 60

    def setUp(self) -> None:
        calibration.reset_for_tests()
        self.addCleanup(calibration.reset_for_tests)
        self.stored: list[str] = []
        self.read = mock.Mock(return_value={"rain": list(self.ROWS), "observations": [], "forecasts": []})
        self.prune = mock.Mock()
        self.in_state: str | None = None
        for name, fake in (
            ("_load_stored", lambda defaults: calibration.from_stored(self.in_state, defaults, time.time())),
            ("_read", self.read),
            ("_store", lambda snapshot, defaults: self.stored.append(calibration.to_stored(snapshot, defaults))),
            ("_prune_log", self.prune),
        ):
            patcher = mock.patch.object(calibration, name, fake)
            patcher.start()
            self.addCleanup(patcher.stop)

    def _refresh(self) -> None:
        calibration._refresh("2026-10-05T00", self.SOURCES, self.DEFAULTS)

    def test_the_first_process_of_the_day_reads_the_log_and_leaves_the_answer(self) -> None:
        self._refresh()
        self.assertEqual(self.read.call_count, 1)
        self.assertEqual(len(self.stored), 1)
        self.prune.assert_called_once()
        self.assertEqual(calibration.current(self.DEFAULTS).pairs, 120)
        self.assertGreater(calibration._next_refresh, time.monotonic() + calibration.REFRESH_S - 60)

    def test_every_other_process_takes_the_stored_answer_and_leaves_the_log_alone(self) -> None:
        self._refresh()
        learned = calibration.current(self.DEFAULTS).as_meta()
        # A cold start: nothing in memory, the answer from an hour ago in app_state.
        self.in_state = self.stored[0]
        calibration.reset_for_tests()
        self.read.reset_mock()
        self.prune.reset_mock()

        self._refresh()

        self.read.assert_not_called()
        self.prune.assert_not_called()
        self.assertEqual(len(self.stored), 1)
        self.assertEqual(calibration.current(self.DEFAULTS).as_meta(), learned)

    def test_a_failed_write_does_not_cost_what_was_learned(self) -> None:
        with mock.patch.object(calibration, "_store", side_effect=ValueError("writes are blocked")):
            self._refresh()
        self.assertTrue(calibration.current(self.DEFAULTS).learned)
        # The prune is tried even though the store failed.
        self.prune.assert_called_once()

    def test_a_database_that_refuses_reads_leaves_the_defaults_and_backs_off(self) -> None:
        self.read.side_effect = ValueError("reads are blocked")
        self._refresh()
        self.assertFalse(calibration.current(self.DEFAULTS).learned)
        self.assertEqual(self.stored, [])
        wait = calibration._next_refresh - time.monotonic()
        self.assertLessEqual(wait, calibration.RETRY_AFTER_FAILURE_S)
        self.assertGreater(wait, calibration.RETRY_AFTER_FAILURE_S - 60)

    def test_before_the_migration_nothing_is_stored(self) -> None:
        self.read.return_value = None
        self._refresh()
        self.assertEqual(self.stored, [])
        self.prune.assert_not_called()
        self.assertFalse(calibration.current(self.DEFAULTS).learned)


if __name__ == "__main__":
    unittest.main(verbosity=2)
