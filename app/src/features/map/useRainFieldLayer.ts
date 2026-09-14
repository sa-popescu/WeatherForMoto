import * as L from 'leaflet';
import { useEffect, useRef } from 'react';
import { rainWeights, type RainGrid } from './iconEu';
import { geometryKey, type FieldGeometry } from './mercator';
import { extrapolate, modelWeight, NOWCAST_STEP_S } from './nowcast';
import { crossFade, mixValues, paintModelRain } from './rainPaint';
import type { TimelineEntry } from './timeline';
import type { IconEuRain } from './useIconEuRain';
import type { RadarNowcast } from './useRadarNowcast';

// Paints the rain ahead for the moment on the band: the extrapolated radar
// (fading into the model after the first 20 minutes) or a model hour. One
// canvas is placed on the map like an image; the few frames around the one on
// screen are kept, so scrubbing back and forth does not repaint them.

const ATTRIBUTION =
  '<a href="https://www.rainviewer.com/api.html" target="_blank" rel="noopener">RainViewer</a>, ' +
  '<a href="https://www.dwd.de/" target="_blank" rel="noopener">DWD ICON-EU</a>';
/** Painted frames kept; each is width x height x 4 bytes. */
const KEPT_FRAMES = 4;

interface Model {
  key: string;
  grid: RainGrid;
  values: Float32Array;
}

/** The model's rain rate at a moment, from the hourly totals around it. */
function modelAt(rain: IconEuRain, tSec: number): Model | null {
  const { before, after, weight } = rainWeights(tSec);
  const a = rain.grids.get(before);
  const b = rain.grids.get(after);
  if (a && b && a.values.length === b.values.length) return { key: `${before}+${after}@${weight.toFixed(3)}`, grid: a, values: mixValues(a.values, b.values, weight) };
  const only = a ?? b;
  return only ? { key: `${a ? before : after}`, grid: only, values: only.values } : null;
}

function frameKey(entry: TimelineEntry, geometry: FieldGeometry, nowcast: RadarNowcast, rain: IconEuRain): string {
  const { before, after } = rainWeights(entry.timeSec);
  const model = `${rain.grids.has(before) ? 1 : 0}${rain.grids.has(after) ? 1 : 0}`;
  return `${geometryKey(geometry)}|${entry.source}|${entry.timeSec}|${nowcast.key ?? '-'}|${model}`;
}

function paintFrame(entry: TimelineEntry, geometry: FieldGeometry, nowcast: RadarNowcast, rain: IconEuRain): Uint8ClampedArray | null {
  const model = (): Model | null => modelAt(rain, entry.timeSec);
  const extrapolated =
    entry.source === 'nowcast' &&
    nowcast.status === 'ready' &&
    nowcast.source &&
    nowcast.motion &&
    nowcast.baseSec !== null &&
    nowcast.geometry &&
    geometryKey(nowcast.geometry) === geometryKey(geometry);

  if (extrapolated && nowcast.source && nowcast.motion && nowcast.baseSec !== null) {
    const lead = entry.timeSec - nowcast.baseSec;
    const pixels = extrapolate(nowcast.source, geometry.width, geometry.height, nowcast.motion, Math.round(lead / NOWCAST_STEP_S));
    const weight = modelWeight(lead);
    const m = weight > 0 ? model() : null;
    if (m) crossFade(pixels, paintModelRain(m.grid, m.values, geometry), weight);
    return pixels;
  }
  const m = model();
  return m ? paintModelRain(m.grid, m.values, geometry) : null;
}

interface Options {
  /** A nowcast or forecast entry to paint, or null to take the layer off. */
  entry: TimelineEntry | null;
  geometry: FieldGeometry | null;
  nowcast: RadarNowcast;
  rain: IconEuRain;
  opacity: number;
}

export function useRainFieldLayer(map: L.Map | null, { entry, geometry, nowcast, rain, opacity }: Options): void {
  const overlay = useRef<L.SVGOverlay | null>(null);
  const canvas = useRef<HTMLCanvasElement | null>(null);
  const painted = useRef(new Map<string, Uint8ClampedArray>());
  const onCanvas = useRef<string | null>(null);

  useEffect(() => {
    if (!map || !entry || !geometry) {
      overlay.current?.remove();
      overlay.current = null;
      onCanvas.current = null;
      return;
    }

    const key = frameKey(entry, geometry, nowcast, rain);
    let pixels = painted.current.get(key) ?? null;
    if (!pixels) {
      pixels = paintFrame(entry, geometry, nowcast, rain);
      if (pixels) {
        painted.current.set(key, pixels);
        while (painted.current.size > KEPT_FRAMES) {
          const oldest = painted.current.keys().next().value;
          if (oldest === undefined) break;
          painted.current.delete(oldest);
        }
      }
    }

    const element = canvas.current ?? document.createElement('canvas');
    canvas.current = element;
    const bounds = L.latLngBounds([geometry.bounds.south, geometry.bounds.west], [geometry.bounds.north, geometry.bounds.east]);

    if (pixels && onCanvas.current !== key) {
      if (element.width !== geometry.width || element.height !== geometry.height) {
        element.width = geometry.width;
        element.height = geometry.height;
      }
      element.getContext('2d')?.putImageData(new ImageData(pixels, geometry.width, geometry.height), 0, 0);
      onCanvas.current = key;
    }

    // Nothing to show yet (the model hour is still loading): keep the layer, hide it.
    const shownOpacity = pixels ? opacity : 0;
    if (!overlay.current) {
      // SVGOverlay places any element it is given; Leaflet has no canvas overlay of its own.
      overlay.current = L.svgOverlay(element as unknown as SVGElement, bounds, {
        opacity: shownOpacity,
        interactive: false,
        className: 'map-forecast-overlay',
        attribution: ATTRIBUTION,
      }).addTo(map);
    } else {
      overlay.current.setBounds(bounds);
      overlay.current.setOpacity(shownOpacity);
    }
  }, [map, entry, geometry, nowcast, rain, opacity]);

  // Leaving the screen (or the map going away) takes the overlay with it.
  useEffect(
    () => () => {
      overlay.current?.remove();
      overlay.current = null;
      painted.current.clear();
    },
    [],
  );
}
