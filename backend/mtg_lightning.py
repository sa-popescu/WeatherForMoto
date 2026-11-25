"""Lightning seen from space by Meteosat Third Generation (EUMETSAT MTG-I1, Lightning Imager).

The Lightning Imager watches Europe and Africa all the time and reports each
flash, cloud to ground or inside the cloud, with its place and time. Radar
and models say where it rains; this says where it strikes, which is what a
rider caught in the open needs to know first.

EUMETSAT publishes the flashes (LI-2-LFL) in its Data Store as one file per
10 minutes for the whole disc, a few hundred kB each, about 10 minutes after
the period ends. Downloading needs a free EUMETSAT account: its API key and
secret (EUMETSAT_CONSUMER_KEY / EUMETSAT_CONSUMER_SECRET) buy a token for an
hour. Without them the source is simply off.

The files are the same for everyone, so the server keeps the flashes of the
last WINDOW_MIN minutes over Romania and its neighbours (a few thousand points
on a stormy afternoon) and asks for new files at most every REFRESH_S. Reading
a point costs nothing then.

The files are NetCDF-4 (HDF5 underneath), read with h5py; latitude and
longitude are packed integers, unpacked here from their own attributes.
Credit: "© EUMETSAT", as the EUMETSAT data policy asks.
"""

from __future__ import annotations

import asyncio
import io
import logging
import math
import os
import time
import zipfile
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

logger = logging.getLogger("motonor.lightning")

COLLECTION = "EO:EUM:DAT:0691"  # LI Lightning Flashes - MTG - 0 degree
TOKEN_URL = "https://api.eumetsat.int/token"
SEARCH_URL = "https://api.eumetsat.int/data/search-products/1.0.0/os"
ATTRIBUTION = "© EUMETSAT"
TIMEOUT_S = 30.0

CONSUMER_KEY: str = os.getenv("EUMETSAT_CONSUMER_KEY", "").strip()
CONSUMER_SECRET: str = os.getenv("EUMETSAT_CONSUMER_SECRET", "").strip()

# Flashes kept and looked at, minutes back from now.
WINDOW_MIN = 40
# New files are asked for at most this often (one comes every 10 minutes).
REFRESH_S = 5 * 60
# After a failure the next try waits this long, so a bad key is not hammered.
BACKOFF_S = 10 * 60
# A file bigger than this is not a flash file.
MAX_FILE_BYTES = 40_000_000

# The part of the disc kept: Romania and its neighbours, with a margin for storms coming in.
REGION_LAT = (41.0, 50.5)
REGION_LON = (18.0, 32.0)

# What a place is told about: flashes this close and this recent.
RADIUS_KM = 30.0
NEAR_KM = 10.0
RECENT_MIN = 30

# flash_time counts seconds from this moment (the files' own "units").
EPOCH = datetime(2000, 1, 1, tzinfo=timezone.utc)


def enabled() -> bool:
    return bool(CONSUMER_KEY and CONSUMER_SECRET)


def _unpack(dataset: Any) -> Any:
    """The values of a NetCDF variable read through h5py, as float64 with NaN for the fill value."""
    import numpy as np

    raw = dataset[()]
    values = np.asarray(raw, dtype=np.float64)
    attrs = dataset.attrs
    fill = attrs.get("_FillValue")
    if fill is not None:
        values[np.asarray(raw) == np.asarray(fill).reshape(-1)[0]] = np.nan
    scale = attrs.get("scale_factor")
    offset = attrs.get("add_offset")
    if scale is not None:
        values = values * float(np.asarray(scale).reshape(-1)[0])
    if offset is not None:
        values = values + float(np.asarray(offset).reshape(-1)[0])
    return values


def _find(f: Any, name: str) -> Any | None:
    """The dataset called ``name`` at the root or in any group (the root in today's files)."""
    if name in f:
        return f[name]
    found: list[Any] = []

    def visit(path: str, obj: Any) -> None:
        if not found and path.rsplit("/", 1)[-1] == name and hasattr(obj, "shape"):
            found.append(obj)

    f.visititems(visit)
    return found[0] if found else None


class Flashes:
    """Flashes of one file inside the region: latitude, longitude (degrees) and time (UTC epoch seconds), as arrays."""

    def __init__(self, lat: Any, lon: Any, t: Any) -> None:
        self.lat = lat
        self.lon = lon
        self.t = t

    def __len__(self) -> int:
        return int(self.lat.size)


def read_flashes(data: bytes) -> Flashes:
    """The flashes of one LI-2-LFL body file (NetCDF bytes) inside REGION_LAT x REGION_LON. Raises on a bad file."""
    import h5py
    import numpy as np

    with h5py.File(io.BytesIO(data), "r") as f:
        lat_ds, lon_ds, time_ds = _find(f, "latitude"), _find(f, "longitude"), _find(f, "flash_time")
        if lat_ds is None or lon_ds is None or time_ds is None:
            raise ValueError("not a lightning flash file")
        lat, lon, seconds = _unpack(lat_ds), _unpack(lon_ds), _unpack(time_ds)
    if not (lat.shape == lon.shape == seconds.shape):
        raise ValueError("flash variables differ in length")
    keep = (np.isfinite(lat) & np.isfinite(lon) & np.isfinite(seconds)
            & (lat >= REGION_LAT[0]) & (lat <= REGION_LAT[1])
            & (lon >= REGION_LON[0]) & (lon <= REGION_LON[1]))
    t = seconds[keep] + EPOCH.timestamp()
    return Flashes(lat[keep].astype(np.float32), lon[keep].astype(np.float32), t)


def body_from_zip(data: bytes) -> bytes:
    """The flash file inside a product zip (the one whose name says BODY)."""
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        for info in z.infolist():
            if "-BODY-" in info.filename and info.filename.endswith(".nc"):
                if info.file_size > MAX_FILE_BYTES:
                    raise ValueError("flash file too big")
                return z.read(info)
    raise ValueError("no flash file in the product")


def _haversine_km(lat1: Any, lon1: Any, lat2: Any, lon2: Any) -> Any:
    import numpy as np

    p1, p2 = np.radians(lat1), np.radians(lat2)
    dp, dl = p2 - p1, np.radians(lon2 - lon1)
    a = np.sin(dp / 2) ** 2 + np.cos(p1) * np.cos(p2) * np.sin(dl / 2) ** 2
    return 6371.0 * 2 * np.arcsin(np.sqrt(np.minimum(1.0, a)))


def _bearing_deg(lat1: float, lon1: float, lat2: float, lon2: float) -> int:
    """Compass bearing from the first point to the second, 0 = north."""
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dl = math.radians(lon2 - lon1)
    x = math.sin(dl) * math.cos(p2)
    y = math.cos(p1) * math.sin(p2) - math.sin(p1) * math.cos(p2) * math.cos(dl)
    return int(round(math.degrees(math.atan2(x, y)))) % 360


def summary(files: list[Flashes], lat: float, lon: float, now: float, *,
            radius_km: float = RADIUS_KM, recent_min: int = RECENT_MIN, covered_until: float | None = None) -> dict[str, Any]:
    """
    What the flashes say about one place: how many struck within radius_km in
    the last recent_min minutes, how many of those within NEAR_KM, and the
    nearest one's distance, direction and age.
    """
    import numpy as np

    out: dict[str, Any] = {
        "count": 0, "near_count": 0, "nearest_km": None, "bearing_deg": None, "last_minutes_ago": None,
        "radius_km": radius_km, "window_min": recent_min,
        "observed_until": (datetime.fromtimestamp(covered_until, timezone.utc).isoformat(timespec="seconds")
                           if covered_until else None),
        "attribution": ATTRIBUTION,
    }
    since = now - recent_min * 60
    best: tuple[float, float, float, float] | None = None  # distance, lat, lon, time
    newest = None
    for chunk in files:
        if not len(chunk):
            continue
        recent = chunk.t >= since
        if not recent.any():
            continue
        la, lo, tt = chunk.lat[recent], chunk.lon[recent], chunk.t[recent]
        dist = _haversine_km(lat, lon, la.astype(np.float64), lo.astype(np.float64))
        inside = dist <= radius_km
        n = int(inside.sum())
        if not n:
            continue
        out["count"] += n
        out["near_count"] += int((dist <= NEAR_KM).sum())
        i = int(np.argmin(np.where(inside, dist, np.inf)))
        if best is None or dist[i] < best[0]:
            best = (float(dist[i]), float(la[i]), float(lo[i]), float(tt[i]))
        last = float(tt[inside].max())
        newest = last if newest is None else max(newest, last)
    if best is not None:
        out["nearest_km"] = round(best[0], 1)
        out["bearing_deg"] = _bearing_deg(lat, lon, best[1], best[2])
    if newest is not None:
        out["last_minutes_ago"] = max(0, int(round((now - newest) / 60)))
    return out


def _end_of(product_id: str) -> datetime | None:
    """The end of the period a product covers, from its name: ..._OPE_20261124211008_20261124212008_N__..."""
    try:
        stamp = product_id.split("_OPE_", 1)[1].split("_")[1]
        return datetime.strptime(stamp, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
    except (IndexError, ValueError):
        return None


class Lightning:
    """The flashes of the last WINDOW_MIN minutes, by product, shared by every request."""

    def __init__(self) -> None:
        self.files: dict[str, tuple[datetime, Flashes]] = {}
        self.token: str = ""
        self.token_until = 0.0
        # None until the first try: monotonic time can start near zero.
        self.checked_at: float | None = None
        self.failed_at: float | None = None
        self.lock = asyncio.Lock()

    async def _token(self, client: httpx.AsyncClient) -> str:
        if self.token and time.monotonic() < self.token_until:
            return self.token
        response = await client.post(TOKEN_URL, data={"grant_type": "client_credentials"},
                                     auth=(CONSUMER_KEY, CONSUMER_SECRET), timeout=TIMEOUT_S)
        response.raise_for_status()
        body = response.json()
        self.token = str(body["access_token"])
        self.token_until = time.monotonic() + max(60.0, float(body.get("expires_in") or 3600) - 120.0)
        return self.token

    async def _latest(self, client: httpx.AsyncClient, now: datetime) -> dict[str, tuple[datetime, str]]:
        """Products ending in the window: product id -> (end, download link of the zip)."""
        start = now - timedelta(minutes=WINDOW_MIN + 10)
        params = {"pi": COLLECTION, "format": "json", "si": "0", "c": "20", "sort": "start,time,0",
                  "dtstart": start.strftime("%Y-%m-%dT%H:%M:%SZ"), "dtend": now.strftime("%Y-%m-%dT%H:%M:%SZ")}
        response = await client.get(SEARCH_URL, params=params, timeout=TIMEOUT_S)
        response.raise_for_status()
        found: dict[str, tuple[datetime, str]] = {}
        for feature in response.json().get("features") or []:
            pid = feature.get("id") if isinstance(feature, dict) else None
            end = _end_of(pid) if pid else None
            links = (((feature.get("properties") or {}).get("links") or {}).get("data") or []) if pid else []
            href = links[0].get("href") if links and isinstance(links[0], dict) else None
            if end is not None and href and end >= now - timedelta(minutes=WINDOW_MIN):
                found[pid] = (end, href)
        return found

    async def refresh(self, client: httpx.AsyncClient) -> None:
        """Fetch products not kept yet and forget the old ones; one refresh at a time, at most every REFRESH_S."""
        async with self.lock:
            moment = time.monotonic()
            if self.checked_at is not None and moment - self.checked_at < REFRESH_S:
                return
            if self.failed_at is not None and moment - self.failed_at < BACKOFF_S:
                return
            try:
                now = datetime.now(timezone.utc)
                wanted = await self._latest(client, now)
                for pid, (end, href) in sorted(wanted.items(), key=lambda item: item[1][0]):
                    if pid in self.files:
                        continue
                    token = await self._token(client)
                    response = await client.get(href, headers={"Authorization": f"Bearer {token}"}, timeout=TIMEOUT_S)
                    if response.status_code == 401:
                        self.token = ""  # expired early: one fresh token, then give up for now
                        token = await self._token(client)
                        response = await client.get(href, headers={"Authorization": f"Bearer {token}"}, timeout=TIMEOUT_S)
                    response.raise_for_status()
                    if len(response.content) > MAX_FILE_BYTES:
                        raise ValueError("product too big")
                    flashes = await asyncio.to_thread(lambda data=response.content: read_flashes(body_from_zip(data)))
                    self.files[pid] = (end, flashes)
                cutoff = now - timedelta(minutes=WINDOW_MIN)
                for pid in [p for p, (end, _) in self.files.items() if end < cutoff]:
                    del self.files[pid]
                self.checked_at = moment
                self.failed_at = None
                logger.info("MTG lightning: %d files, %d flashes over the region",
                            len(self.files), sum(len(f) for _, f in self.files.values()))
            except Exception:
                self.failed_at = moment
                raise

    def covered_until(self) -> float | None:
        return max(end for end, _ in self.files.values()).timestamp() if self.files else None

    def at(self, lat: float, lon: float, now: float | None = None) -> dict[str, Any]:
        return summary([f for _, f in self.files.values()], lat, lon, time.time() if now is None else now,
                       covered_until=self.covered_until())


_lightning = Lightning()


async def near(lat: float, lon: float, client: httpx.AsyncClient) -> dict[str, Any] | None:
    """
    Lightning around the point in the last RECENT_MIN minutes, or None when the
    source is off or nothing could be read. Shared files, refreshed when stale.
    """
    if not enabled():
        return None
    try:
        await _lightning.refresh(client)
    except Exception as exc:  # lightning is a help, never a reason to fail the weather
        logger.warning("MTG lightning unavailable: %s", type(exc).__name__)
    # Files older than the window say nothing about now.
    covered = _lightning.covered_until()
    if covered is None or time.time() - covered > (WINDOW_MIN + 15) * 60:
        return None
    return _lightning.at(lat, lon)
