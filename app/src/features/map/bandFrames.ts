import { geometryKey, type FieldGeometry } from './mercator';
import type { ModelSource } from './modelField';
import { extrapolate, modelWeight, NOWCAST_STEP_S } from './nowcast';
import { crossFade, paintModelField } from './rainPaint';
import type { FieldNowcast } from './useFieldNowcast';
import type { FieldFrame, Pixels } from './useFieldLayer';

// What the canvas shows for a moment of the band: an extrapolated picture
// fading into the model, a model moment, or an observed picture. A moment
// whose data is still on its way is a pending frame (paint gives null), not
// no frame at all: the layer then keeps the last picture on screen until it
// comes (useFieldLayer.ts), instead of going blank.

/** A frame for a moment whose data is not there yet. */
export function pendingFrame(key: string): FieldFrame {
  return { key: `${key}|pending`, paint: () => null };
}

function modelPixels(model: ModelSource, tSec: number, geometry: FieldGeometry): { key: string; pixels: () => Pixels } | null {
  const sample = model.sample(tSec);
  if (!sample) return null;
  return { key: `${model.id}|${sample.key}`, pixels: () => paintModelField(sample.grid, sample.values, geometry, model.color) };
}

/** A model moment on its own (pending until the model has that hour). */
export function modelFrame(tSec: number, geometry: FieldGeometry, model: ModelSource | null): FieldFrame {
  const key = `${geometryKey(geometry)}|model|${tSec}`;
  const m = model ? modelPixels(model, tSec, geometry) : null;
  return m ? { key: `${key}|${m.key}`, paint: m.pixels } : pendingFrame(key);
}

/**
 * The newest picture moved on to tSec, with the model showing through more
 * and more (see modelWeight). Without a usable extrapolation it is the model.
 */
export function nowcastFrame(tSec: number, geometry: FieldGeometry, nowcast: FieldNowcast, model: ModelSource | null): FieldFrame {
  const { source, motion, baseSec } = nowcast;
  const usable =
    nowcast.status === 'ready' && source && motion && baseSec !== null && nowcast.geometry && geometryKey(nowcast.geometry) === geometryKey(geometry);
  if (!usable || !source || !motion || baseSec === null) return modelFrame(tSec, geometry, model);

  const lead = tSec - baseSec;
  const weight = modelWeight(lead);
  const m = weight > 0 && model ? modelPixels(model, tSec, geometry) : null;
  return {
    key: `${geometryKey(geometry)}|nowcast|${tSec}|${nowcast.key}|${m?.key ?? '-'}`,
    paint: () => {
      const pixels = extrapolate(source, geometry.width, geometry.height, motion, Math.round(lead / NOWCAST_STEP_S));
      if (m) crossFade(pixels, m.pixels(), weight);
      return pixels;
    },
  };
}

/** An observed picture as it is (pending while it downloads). */
export function pictureFrame(key: string, pixels: Pixels | undefined): FieldFrame {
  return pixels ? { key, paint: () => pixels } : pendingFrame(key);
}
