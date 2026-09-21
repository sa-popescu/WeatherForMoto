"""
How well each source forecast what was then measured, from the verification log.

Run from backend/ with the database variables set (read only, changes nothing):

    python verify_report.py              # the last 30 days
    python verify_report.py --days 7

Rain: every source's chance of rain against whether a station or an airport
saw rain in that cell and hour, as a Brier score (0 is perfect, lower is
better) and a skill score against always forecasting the local base rate
(above 0 beats it, 1 is perfect). The suggested weights for rain_fusion.WEIGHTS
follow the skill scores, scaled so the ensemble keeps its current weight.

Temperature: the app's final value against Open-Meteo's raw one, both against
the official station, so the measured-gap and altitude corrections can be
judged. Hours without a matching measurement are left out.
"""

from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

import rain_fusion
import verification

# Lead hours are reported in these groups.
LEAD_GROUPS: tuple[tuple[str, tuple[int, ...]], ...] = (
    ("0-3 h", (1, 3)),
    ("6-12 h", (6, 12)),
    ("24-48 h", (24, 48)),
)
# Fewer pairs than this make a score too noisy to act on.
MIN_PAIRS = 30


def _rain_rows(conn: Any, since: str) -> list[dict[str, Any]]:
    prob_columns = ", ".join(f"f.prob_{name.replace('-', '_')}" for name in verification.RAIN_SOURCES)
    return conn.execute(
        f"""SELECT f.lead_h, f.precip_prob, {prob_columns}, o.raining
            FROM forecast_log f
            JOIN (SELECT cell, hour, MAX(raining) AS raining FROM observation_log
                  WHERE raining IS NOT NULL GROUP BY cell, hour) o
              ON o.cell = f.cell AND o.hour = f.valid_hour
            WHERE f.issued_hour >= ?""",
        (since,),
    ).fetchall()


def _temp_rows(conn: Any, since: str) -> list[dict[str, Any]]:
    return conn.execute(
        """SELECT f.lead_h, f.temp, f.om_temp, o.temp AS measured
           FROM forecast_log f
           JOIN observation_log o ON o.cell = f.cell AND o.hour = f.valid_hour AND o.source = 'official'
           WHERE f.issued_hour >= ? AND o.temp IS NOT NULL""",
        (since,),
    ).fetchall()


def _group_of(lead: int) -> str | None:
    return next((name for name, leads in LEAD_GROUPS if lead in leads), None)


def rain_scores(rows: list[dict[str, Any]]) -> dict[str, dict[str, tuple[float, float, int]]]:
    """Per source and lead group: (Brier score, skill against the base rate, pairs)."""
    pairs: dict[tuple[str, str], list[tuple[float, int]]] = defaultdict(list)
    for row in rows:
        group = _group_of(int(row["lead_h"]))
        if group is None or row["raining"] is None:
            continue
        observed = int(row["raining"])
        if row["precip_prob"] is not None:
            pairs[("final", group)].append((float(row["precip_prob"]) / 100, observed))
        for name in verification.RAIN_SOURCES:
            value = row.get(f"prob_{name.replace('-', '_')}")
            if value is not None:
                pairs[(name, group)].append((float(value), observed))
    out: dict[str, dict[str, tuple[float, float, int]]] = defaultdict(dict)
    for (source, group), items in pairs.items():
        base = sum(o for _, o in items) / len(items)
        brier = sum((p - o) ** 2 for p, o in items) / len(items)
        reference = sum((base - o) ** 2 for _, o in items) / len(items)
        skill = 1 - brier / reference if reference > 0 else 0.0
        out[source][group] = (brier, skill, len(items))
    return out


def suggested_weights(scores: dict[str, dict[str, tuple[float, float, int]]]) -> dict[str, float]:
    """Weights in proportion to each source's skill over all leads, ensemble kept at its weight."""
    skill: dict[str, float] = {}
    for name in rain_fusion.WEIGHTS:
        groups = [v for v in scores.get(name, {}).values() if v[2] >= MIN_PAIRS]
        if groups:
            total = sum(n for _, _, n in groups)
            skill[name] = max(0.0, sum(s * n for _, s, n in groups) / total)
    anchor = skill.get("ensemble")
    if not anchor:
        return {}
    scale = rain_fusion.WEIGHTS["ensemble"] / anchor
    return {name: round(value * scale, 2) for name, value in skill.items()}


def temp_scores(rows: list[dict[str, Any]]) -> dict[str, dict[str, tuple[float, float, int]]]:
    """Per lead group, for the final and the raw value: (mean absolute error, mean bias, pairs)."""
    errors: dict[tuple[str, str], list[float]] = defaultdict(list)
    for row in rows:
        group = _group_of(int(row["lead_h"]))
        if group is None:
            continue
        for label, column in (("final", "temp"), ("open-meteo raw", "om_temp")):
            if row[column] is not None:
                errors[(label, group)].append(float(row[column]) - float(row["measured"]))
    out: dict[str, dict[str, tuple[float, float, int]]] = defaultdict(dict)
    for (label, group), items in errors.items():
        out[label][group] = (sum(abs(e) for e in items) / len(items), sum(items) / len(items), len(items))
    return out


def _print_table(title: str, scores: dict[str, dict[str, tuple[float, float, int]]], headers: tuple[str, str]) -> None:
    print(f"\n{title}")
    groups = [name for name, _ in LEAD_GROUPS]
    print(f"{'':<16}" + "".join(f"{g:>28}" for g in groups))
    print(f"{'':<16}" + "".join(f"{headers[0]:>10}{headers[1]:>10}{'pairs':>8}" for _ in groups))
    for source, by_group in sorted(scores.items()):
        cells = []
        for group in groups:
            value = by_group.get(group)
            cells.append(f"{value[0]:>10.3f}{value[1]:>10.3f}{value[2]:>8}" if value else f"{'-':>10}{'-':>10}{0:>8}")
        print(f"{source:<16}" + "".join(cells))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--days", type=int, default=30, help="how many days back to read (default 30)")
    args = parser.parse_args()
    since = (datetime.now(timezone.utc) - timedelta(days=max(1, args.days))).strftime("%Y-%m-%dT%H")

    from auth_alerts import _connect  # needs TURSO_DATABASE_URL and TURSO_AUTH_TOKEN

    try:
        conn = _connect()
    except Exception as exc:
        print(f"Cannot open the database: {exc}", file=sys.stderr)
        return 1
    try:
        rain = rain_scores(_rain_rows(conn, since))
        temp = temp_scores(_temp_rows(conn, since))
    except Exception as exc:
        print(f"Cannot read the verification log (has the migration run?): {exc}", file=sys.stderr)
        return 1
    finally:
        conn.close()

    if not rain and not temp:
        print(f"No forecast has a matching measurement since {since} UTC yet.")
        return 0
    _print_table("Rain: Brier score (lower is better) and skill against the base rate (higher is better)",
                 rain, ("brier", "skill"))
    _print_table("Temperature against the official station, °C", temp, ("mae", "bias"))
    weights = suggested_weights(rain)
    if weights:
        print("\nSuggested rain_fusion.WEIGHTS (current in brackets), from sources with "
              f"at least {MIN_PAIRS} pairs per lead group:")
        for name, value in weights.items():
            print(f"  {name:<16}{value:>6}  ({rain_fusion.WEIGHTS[name]})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
