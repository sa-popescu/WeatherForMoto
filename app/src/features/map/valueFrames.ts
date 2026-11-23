import { coverField } from './cloudValues';
import { geometryKey, type FieldGeometry } from './mercator';
import type { ModelSource } from './modelField';
import { extrapolate, modelWeight, NOWCAST_STEP_S } from './nowcast';
import type { GridBox } from './rainPaint';
import { blendFields, blendLinear, CHANNELS, gridIntoField, modelField, type ValueField } from './rainValues';
import type { FieldNowcast } from './useFieldNowcast';

// The field for a moment of the band: an observed one (radar frame,
// satellite clouds, lightning), the newest observed one moved on with the
// model showing through more and more, or a model moment. A moment whose data
// is still on its way is pending (values gives null): the map keeps what it
// shows until it comes, instead of going blank (useValueLayer.ts).

export interface ValueFrame {
  /** Identifies what the values are; the same key always gives the same values. */
  key: string;
  geometry: FieldGeometry;
  /** The moment shown, unix seconds: two moments close together can be joined along the motion (useValueLayer.ts). */
  timeSec: number;
  /** Null while the data is not there yet. */
  values: () => ValueField | null;
}

/** What a kind of field takes from the model, and how the model shows through the extrapolation. */
export interface FieldKind {
  name: string;
  fromModel: (grid: GridBox, values: Float32Array, geometry: FieldGeometry) => ValueField;
  blend: (field: ValueField, model: Uint8ClampedArray, weight: number) => void;
}

/** Rain: ICON-EU or the grid's rain totals, blended as rain amounts. */
export const RAIN_FIELD: FieldKind = { name: 'rain', fromModel: modelField, blend: blendFields };
/** Clouds: the grid's cloud cover, blended straight. */
export const CLOUD_FIELD: FieldKind = { name: 'clouds', fromModel: coverField, blend: blendLinear };

function pending(key: string, geometry: FieldGeometry, timeSec: number): ValueFrame {
  return { key: `${key}|pending`, geometry, timeSec, values: () => null };
}

/** An observed field at `timeSec`, once it has been read; `name` and `id` say which (rain and a radar path, clouds and a time). */
export function observedFrame(name: string, geometry: FieldGeometry, id: string, timeSec: number, field: ValueField | undefined): ValueFrame {
  const key = `${geometryKey(geometry)}|${name}|${id}`;
  return field ? { key, geometry, timeSec, values: () => field } : pending(key, geometry, timeSec);
}

function modelValues(kind: FieldKind, model: ModelSource, tSec: number, geometry: FieldGeometry): { key: string; values: () => ValueField } | null {
  const sample = model.sample(tSec);
  if (!sample) return null;
  // The share of snow, where the model knows it, goes in the second byte like the radar's snow mark.
  const snow = model.snow?.(tSec) ?? null;
  return {
    key: `${model.id}|${sample.key}|${snow?.key ?? '-'}`,
    values: () => {
      const field = kind.fromModel(sample.grid, sample.values, geometry);
      if (snow) gridIntoField(field, 1, snow.grid, snow.values, geometry, (share) => share * 255);
      return field;
    },
  };
}

/** A model moment on its own (pending until the model has that hour). */
export function modelFrame(kind: FieldKind, tSec: number, geometry: FieldGeometry, model: ModelSource | null): ValueFrame {
  const key = `${geometryKey(geometry)}|${kind.name}|model|${tSec}`;
  const m = model ? modelValues(kind, model, tSec, geometry) : null;
  return m ? { key: `${key}|${m.key}`, geometry, timeSec: tSec, values: m.values } : pending(key, geometry, tSec);
}

/**
 * The newest observed field moved on to tSec, with the model showing through
 * more and more (see modelWeight). Without a usable extrapolation it is the model.
 */
export function nowcastFrame(kind: FieldKind, tSec: number, geometry: FieldGeometry, nowcast: FieldNowcast, model: ModelSource | null): ValueFrame {
  const { source, motion, baseSec } = nowcast;
  const usable =
    nowcast.status === 'ready' && source && motion && baseSec !== null && nowcast.geometry && geometryKey(nowcast.geometry) === geometryKey(geometry);
  if (!usable || !source || !motion || baseSec === null) return modelFrame(kind, tSec, geometry, model);

  const lead = tSec - baseSec;
  const weight = modelWeight(lead);
  const m = weight > 0 && model ? modelValues(kind, model, tSec, geometry) : null;
  return {
    key: `${geometryKey(geometry)}|${kind.name}|nowcast|${tSec}|${nowcast.key}|${m?.key ?? '-'}`,
    geometry,
    timeSec: tSec,
    values: () => {
      const field = extrapolate(source, geometry.width, geometry.height, motion, Math.round(lead / NOWCAST_STEP_S), CHANNELS);
      if (m) kind.blend(field, m.values(), weight);
      return field;
    },
  };
}
