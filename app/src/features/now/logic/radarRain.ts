import { latToY, lonToX, TILE_SIZE, xToLon, yToLat, type FieldGeometry } from '../../map/mercator';
import { echoLevel, NOWCAST_STEP_S, NOWCAST_STEPS, velocityAt, type MotionField } from '../../map/nowcast';

// Rain at one point for the next ~90 minutes, from the radar: the last
// RainViewer pictures around the rider, their measured motion (the map's own
// extrapolation engine), and the echo that motion brings over the point. The
// forecast models cannot place a shower to the kilometre and the minute; the
// radar can, for as long as the rain keeps its course. Pure helpers only; the
// hook in useRadarRain.ts loads the pictures.

/** RainViewer's deepest native zoom: about 0.9 km per pixel over Romania. */
export const RADAR_ZOOM = 7;
/**
 * Canvas reach around the point. Rain at 60 km/h covers 90 km in the
 * 90 minutes looked at; a larger reach costs tiles and adds rain that the
 * extrapolation would not bring in time anyway.
 */
export const RADAR_REACH_KM = 110;
/** Around the point, the strongest echo within this many pixels counts (about 2 km). */
const SAMPLE_RADIUS_PX = 2;
/** Radar echo at or above this rate counts as rain on the road. */
export const RADAR_WET_MM_H = 0.1;
/** An extrapolated step older than this is behind us, not ahead. */
const PAST_TOLERANCE_S = 5 * 60;

const EARTH_KM = 40_075;

/** A square canvas of RADAR_REACH_KM around the point, aligned to the radar tiles. */
export function pointGeometry(lat: number, lon: number, reachKm: number = RADAR_REACH_KM, z: number = RADAR_ZOOM): FieldGeometry {
  const kmPerPx = (EARTH_KM * Math.cos((lat * Math.PI) / 180)) / (TILE_SIZE * 2 ** z);
  const half = Math.ceil(reachKm / kmPerPx);
  const cx = Math.round(lonToX(lon, z));
  const cy = Math.round(latToY(lat, z));
  const x0 = cx - half;
  const y0 = cy - half;
  const size = half * 2;
  return {
    z,
    x0,
    y0,
    width: size,
    height: size,
    bounds: { north: yToLat(y0, z), south: yToLat(y0 + size, z), west: xToLon(x0, z), east: xToLon(x0 + size, z) },
  };
}

/** Rain rate from radar reflectivity (Marshall-Palmer, Z = 200 R^1.6). */
export function dbzToMmH(dbz: number): number {
  return (10 ** (dbz / 10) / 200) ** (1 / 1.6);
}

/** Rain rate at a canvas pixel: the strongest echo in a small disc around it, 0 without rain. */
function rateAt(rgba: Uint8ClampedArray, width: number, height: number, x: number, y: number): number {
  let level = 0;
  const px = Math.round(x);
  const py = Math.round(y);
  for (let dy = -SAMPLE_RADIUS_PX; dy <= SAMPLE_RADIUS_PX; dy += 1) {
    for (let dx = -SAMPLE_RADIUS_PX; dx <= SAMPLE_RADIUS_PX; dx += 1) {
      if (dx * dx + dy * dy > SAMPLE_RADIUS_PX * SAMPLE_RADIUS_PX) continue;
      const sx = px + dx;
      const sy = py + dy;
      if (sx < 0 || sy < 0 || sx >= width || sy >= height) continue;
      const i = (sy * width + sx) * 4;
      level = Math.max(level, echoLevel(rgba[i], rgba[i + 1], rgba[i + 2], rgba[i + 3]));
    }
  }
  // echoLevel gives dBZ above 10, with 0 meaning no echo worth following.
  return level > 0 ? dbzToMmH(level + 10) : 0;
}

export interface RadarStep {
  /** Unix seconds this step describes. */
  timeSec: number;
  mmPerHour: number;
}

/**
 * Rain over the point at each radar interval from the newest picture on: the
 * point's trail is followed back through the motion one interval at a time,
 * and the echo found at the end of it is the rain that will be overhead.
 */
export function rainAlongTrail(source: Uint8ClampedArray, geometry: FieldGeometry, motion: MotionField, lat: number, lon: number, baseSec: number, steps: number = NOWCAST_STEPS): RadarStep[] {
  const x = lonToX(lon, geometry.z) - geometry.x0;
  const y = latToY(lat, geometry.z) - geometry.y0;
  const out: RadarStep[] = [];
  const velocity = { x: 0, y: 0 };
  let tx = x;
  let ty = y;
  for (let s = 0; s <= steps; s += 1) {
    if (s > 0) {
      velocityAt(motion, tx, ty, velocity);
      tx -= velocity.x;
      ty -= velocity.y;
    }
    out.push({ timeSec: baseSec + s * NOWCAST_STEP_S, mmPerHour: rateAt(source, geometry.width, geometry.height, tx, ty) });
  }
  return out;
}

export interface RadarRain {
  /** Rain over the point now (the step closest to the clock), mm/h. */
  nowMmPerHour: number;
  /** Minutes until rain arrives, when it is dry now; null when none comes within reach. */
  arrivesInMin: number | null;
  /** Minutes until the rain overhead stops; null when it is dry now or does not stop within reach. */
  stopsInMin: number | null;
  /** Strongest rate expected over the look-ahead, mm/h. */
  peakMmPerHour: number;
  /** How far ahead the radar looks from now, minutes. */
  reachMin: number;
  /** Unix seconds of the newest radar picture. */
  baseSec: number;
}

/** What the steps mean for someone about to ride, seen from `nowSec`. */
export function summarizeRadar(steps: readonly RadarStep[], nowSec: number): RadarRain | null {
  const ahead = steps.filter((s) => s.timeSec >= nowSec - PAST_TOLERANCE_S);
  if (ahead.length === 0) return null;
  const minutesTo = (s: RadarStep): number => Math.max(0, Math.round((s.timeSec - nowSec) / 60));
  const wet = (s: RadarStep): boolean => s.mmPerHour >= RADAR_WET_MM_H;
  const now = ahead[0];
  const change = ahead.find((s) => wet(s) !== wet(now));
  return {
    nowMmPerHour: now.mmPerHour,
    arrivesInMin: !wet(now) && change ? minutesTo(change) : null,
    stopsInMin: wet(now) && change ? minutesTo(change) : null,
    peakMmPerHour: Math.max(...ahead.map((s) => s.mmPerHour)),
    reachMin: minutesTo(ahead[ahead.length - 1]),
    baseSec: steps[0].timeSec,
  };
}
