"""Rain measured by Europe's weather radars (EUMETNET OPERA), for the road after rain.

OPERA joins the national radars, the Romanian ones included, into one 1 km
grid over Europe. Among its open products (CC BY 4.0, through EUMETNET's
MeteoGate) is the rain of the last hour, every 15 minutes. The road's state
after rain (wet_road.py) used to learn the rain of the past hours from the
model alone; where the radar saw it, measured rain now takes the model's
place for those hours, both ways: rain the model missed wets the road, and
rain it forecast but that never fell does not.

The files are the same for everyone, so the server keeps the last
HOURS hourly sums, cut down to Romania and its neighbours (a few MB in all),
and fetches a new one about once an hour. Reading a point costs nothing
then. A value is the median of the 3 x 3 cells around the point (2 km cells
for the hourly sums), leaving out cells the composite itself rates as poor,
so one bad pixel (sea clutter shows near Constanța) does not decide it.

The files are HDF5 (ODIM), read with h5py; the grid is in a Lambert
azimuthal equal-area projection, worked out here without a library.
"""

from __future__ import annotations

import asyncio
import io
import logging
import math
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

logger = logging.getLogger("weatherformoto.opera")

LIST_URL = "https://api.meteogate.eu/eu-eumetnet-weather-radar/collections/observations/locations/0-20010-0-OPERA"
PARAMETER = "ACRR:comp"
ATTRIBUTION = "EUMETNET OPERA, CC BY 4.0"
# A free MeteoGate key raises the gateway's limits; the radar answers without one too.
API_KEY: str = os.getenv("METEOGATE_API_KEY", "").strip()
API_KEY_HEADER = "apikey"
TIMEOUT_S = 30.0

# Full hours of measured rain kept, newest last.
HOURS = 6
# The list of files is asked for again after this long.
LIST_TTL_S = 30 * 60
# Half the side of the square read around a point, in grid cells.
HALF_WINDOW = 1
# Cells the composite rates below this quality (its qi_total, 0 to 1) are left out.
MIN_QUALITY = 0.2

# The part of the grid kept: Romania and its neighbours.
REGION_LAT = (42.0, 49.5)
REGION_LON = (19.0, 31.0)

# The projection of every OPERA composite (the files' "projdef"): its false
# origin puts the grid's top left corner at (0, 0). The cell size is the
# file's own (1 km for reflectivity, 2 km for the hourly sums).
_A = 6378137.0
_E2 = 0.0066943799901413165
_LAT0, _LON0 = 55.0, 10.0
_X0, _Y0 = 1950000.0, -2100000.0


def _q(phi: float) -> float:
    e = math.sqrt(_E2)
    s = math.sin(phi)
    return (1 - _E2) * (s / (1 - _E2 * s * s) - (1 / (2 * e)) * math.log((1 - e * s) / (1 + e * s)))


_QP = _q(math.pi / 2)
_RQ = _A * math.sqrt(_QP / 2)
_BETA1 = math.asin(_q(math.radians(_LAT0)) / _QP)
_D = _A * (math.cos(math.radians(_LAT0)) / math.sqrt(1 - _E2 * math.sin(math.radians(_LAT0)) ** 2)) / (_RQ * math.cos(_BETA1))


def grid_xy(lat: float, lon: float) -> tuple[float, float]:
    """The point in the composites' projection, metres from the grid's top left corner (y negative southward)."""
    beta = math.asin(_q(math.radians(lat)) / _QP)
    dlon = math.radians(lon - _LON0)
    b = _RQ * math.sqrt(2 / (1 + math.sin(_BETA1) * math.sin(beta) + math.cos(_BETA1) * math.cos(beta) * math.cos(dlon)))
    x = b * _D * math.cos(beta) * math.sin(dlon) + _X0
    y = (b / _D) * (math.cos(_BETA1) * math.sin(beta) - math.sin(_BETA1) * math.cos(beta) * math.cos(dlon)) + _Y0
    return x, y


def grid_cell(lat: float, lon: float, cell_m: float) -> tuple[int, int]:
    """The (row, column) of the cell holding the point, in a grid of ``cell_m`` metre cells (ellipsoidal LAEA, Snyder's formulas)."""
    x, y = grid_xy(lat, lon)
    return int(-y // cell_m), int(x // cell_m)


def region(cell_m: float, rows: int, cols: int) -> tuple[int, int, int, int]:
    """Rows and columns (start inclusive, end exclusive) covering REGION_LAT x REGION_LON."""
    cells = [
        grid_cell(lat, lon, cell_m)
        for lat in (REGION_LAT[0], (REGION_LAT[0] + REGION_LAT[1]) / 2, REGION_LAT[1])
        for lon in [REGION_LON[0] + i * (REGION_LON[1] - REGION_LON[0]) / 8 for i in range(9)]
    ]
    r = [cell[0] for cell in cells]
    c = [cell[1] for cell in cells]
    return max(0, min(r)), min(rows, max(r) + 1), max(0, min(c)), min(cols, max(c) + 1)


class Grid:
    """The region of one composite: values (float32, NaN where nothing is known), where it starts, its cell size."""

    def __init__(self, values: Any, row0: int, col0: int, cell_m: float) -> None:
        self.values = values
        self.row0 = row0
        self.col0 = col0
        self.cell_m = cell_m


def read_hourly_sum(data: bytes) -> Grid:
    """The region of one ACRR file, mm; NaN where no radar sees or the composite rates the cell poor. Raises on a bad file."""
    import h5py
    import numpy as np

    with h5py.File(io.BytesIO(data), "r") as f:
        cell_m = float(f["where"].attrs.get("yscale", 2000.0))
        dataset = f["dataset1/data1/data"]
        r0, r1, c0, c1 = region(cell_m, dataset.shape[0], dataset.shape[1])
        what = f["dataset1/data1/what"].attrs
        gain = float(what.get("gain", 1.0))
        offset = float(what.get("offset", 0.0))
        nodata = float(what.get("nodata", float("nan")))
        undetect = float(what.get("undetect", float("nan")))
        raw = dataset[r0:r1, c0:c1].astype(np.float64)
        quality = None
        if "dataset1/data1/quality1/data" in f:
            q_what = f["dataset1/data1/quality1/what"].attrs
            quality = (f["dataset1/data1/quality1/data"][r0:r1, c0:c1] * float(q_what.get("gain", 1.0))
                       + float(q_what.get("offset", 0.0)))
    out = raw * gain + offset
    out[raw == undetect] = 0.0  # seen by a radar, and nothing fell
    out[out < 0] = 0.0
    out[raw == nodata] = np.nan
    if quality is not None:
        out[(quality < MIN_QUALITY) & (raw != undetect)] = np.nan
    return Grid(out.astype(np.float32), r0, c0, cell_m)


def value_at(grid: Grid, lat: float, lon: float) -> float | None:
    """The median of the 3 x 3 cells around the point, mm; None outside the region or where no radar sees."""
    import numpy as np

    row, col = grid_cell(lat, lon, grid.cell_m)
    r, c = row - grid.row0, col - grid.col0
    rows, cols = grid.values.shape
    if not (HALF_WINDOW <= r < rows - HALF_WINDOW and HALF_WINDOW <= c < cols - HALF_WINDOW):
        return None
    window = grid.values[r - HALF_WINDOW:r + HALF_WINDOW + 1, c - HALF_WINDOW:c + HALF_WINDOW + 1]
    seen = window[~np.isnan(window)]
    return round(float(np.median(seen)), 2) if seen.size else None


def _end_time(href: str) -> datetime | None:
    """The end of the hour a file sums up, from its name: OPERA@20261123T2200@0@ACRR.h5 (UTC)."""
    try:
        stamp = href.rsplit("/", 1)[-1].split("@")[1]
        return datetime.strptime(stamp, "%Y%m%dT%H%M").replace(tzinfo=timezone.utc)
    except (IndexError, ValueError):
        return None


class HourlyRain:
    """The last HOURS hourly sums, by the end of their hour (UTC), shared by every request."""

    def __init__(self) -> None:
        self.grids: dict[datetime, Grid] = {}
        self.listed_at = 0.0
        self.links: dict[datetime, str] = {}
        self.lock = asyncio.Lock()

    async def _list(self, client: httpx.AsyncClient) -> None:
        headers = {API_KEY_HEADER: API_KEY} if API_KEY else {}
        params = {"parameter-name": PARAMETER, "level": "0.0", "format": "ODIM"}
        response = await client.get(LIST_URL, params=params, headers=headers, timeout=TIMEOUT_S)
        response.raise_for_status()
        links: dict[datetime, str] = {}
        for link in response.json().get("links") or []:
            href = link.get("href") if isinstance(link, dict) else None
            end = _end_time(href) if link.get("rel") == "items" and href else None
            # Only the sums that end on the hour, so the hours do not overlap.
            if end is not None and end.minute == 0:
                links[end] = href
        self.links = links
        self.listed_at = time.monotonic()

    async def refresh(self, client: httpx.AsyncClient) -> None:
        """Fetch the newest hours not kept yet and forget the old ones; one refresh at a time."""
        async with self.lock:
            if time.monotonic() - self.listed_at < LIST_TTL_S and self.grids:
                return
            await self._list(client)
            wanted = sorted(self.links)[-HOURS:]
            for end in wanted:
                if end in self.grids:
                    continue
                response = await client.get(self.links[end], timeout=TIMEOUT_S)
                response.raise_for_status()
                self.grids[end] = await asyncio.to_thread(read_hourly_sum, response.content)
            for end in [e for e in self.grids if e not in wanted]:
                del self.grids[end]
            logger.info("OPERA hourly rain: %d hours, newest ending %s",
                        len(self.grids), max(self.grids).isoformat() if self.grids else "-")

    def at(self, lat: float, lon: float) -> dict[str, float]:
        """Measured rain at the point by the end of each kept hour (UTC ISO, "YYYY-MM-DDTHH:00")."""
        out: dict[str, float] = {}
        for end, grid in sorted(self.grids.items()):
            value = value_at(grid, lat, lon)
            if value is not None:
                out[end.strftime("%Y-%m-%dT%H:%M")] = value
        return out


_hourly = HourlyRain()


async def measured_rain(lat: float, lon: float, client: httpx.AsyncClient) -> dict[str, float] | None:
    """
    The radar's rain at the point for each of the last full hours, mm, keyed
    by the hour's end in UTC; None when the radar cannot be read. A stale
    list is refreshed first (shared, so most requests only read).
    """
    try:
        await _hourly.refresh(client)
    except Exception as exc:  # the radar is a help, never a reason to fail the weather
        logger.warning("OPERA radar unavailable: %s", type(exc).__name__)
        if not _hourly.grids:
            return None
    return _hourly.at(lat, lon)


def local_hours(measured: dict[str, float] | None, utc_offset_seconds: int) -> dict[str, float]:
    """
    The same sums keyed by the hourly item they belong to: Open-Meteo's
    precipitation at local "HH:00" is the rain of the hour before, as is the
    radar's sum ending at that moment.
    """
    if not measured:
        return {}
    out: dict[str, float] = {}
    for end_utc, mm in measured.items():
        try:
            end = datetime.strptime(end_utc, "%Y-%m-%dT%H:%M")
        except ValueError:
            continue
        out[(end + timedelta(seconds=utc_offset_seconds)).strftime("%Y-%m-%dT%H:%M")] = mm
    return out
