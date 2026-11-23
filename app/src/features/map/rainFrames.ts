import { geometryKey, type FieldGeometry } from './mercator';
import type { ModelSource } from './modelField';
import { extrapolate, modelWeight, NOWCAST_STEP_S } from './nowcast';
import { blendFields, CHANNELS, modelField, type RainField } from './rainValues';
import type { FieldNowcast } from './useFieldNowcast';

// The rain field for a moment of the band: an observed radar frame, the
// newest one moved on with the model showing through more and more, or a
// model moment. A moment whose data is still on its way is pending (values
// gives null): the map keeps what it shows until it comes, instead of going
// blank (useRainLayer.ts).

export interface RainFrame {
  /** Identifies what the values are; the same key always gives the same values. */
  key: string;
  geometry: FieldGeometry;
  /** Null while the data is not there yet. */
  values: () => RainField | null;
}

function pending(key: string, geometry: FieldGeometry): RainFrame {
  return { key: `${key}|pending`, geometry, values: () => null };
}

/** An observed radar frame, once it has been read (radarValues.ts). */
export function observedRainFrame(geometry: FieldGeometry, path: string, field: RainField | undefined): RainFrame {
  const key = `${geometryKey(geometry)}|radar|${path}`;
  return field ? { key, geometry, values: () => field } : pending(key, geometry);
}

function modelValues(model: ModelSource, tSec: number, geometry: FieldGeometry): { key: string; values: () => RainField } | null {
  const sample = model.sample(tSec);
  if (!sample) return null;
  return { key: `${model.id}|${sample.key}`, values: () => modelField(sample.grid, sample.values, geometry) };
}

/** A model moment on its own (pending until the model has that hour). */
export function modelRainFrame(tSec: number, geometry: FieldGeometry, model: ModelSource | null): RainFrame {
  const key = `${geometryKey(geometry)}|model|${tSec}`;
  const m = model ? modelValues(model, tSec, geometry) : null;
  return m ? { key: `${key}|${m.key}`, geometry, values: m.values } : pending(key, geometry);
}

/**
 * The newest radar frame moved on to tSec, with the model showing through
 * more and more (see modelWeight). Without a usable extrapolation it is the model.
 */
export function nowcastRainFrame(tSec: number, geometry: FieldGeometry, nowcast: FieldNowcast, model: ModelSource | null): RainFrame {
  const { source, motion, baseSec } = nowcast;
  const usable =
    nowcast.status === 'ready' && source && motion && baseSec !== null && nowcast.geometry && geometryKey(nowcast.geometry) === geometryKey(geometry);
  if (!usable || !source || !motion || baseSec === null) return modelRainFrame(tSec, geometry, model);

  const lead = tSec - baseSec;
  const weight = modelWeight(lead);
  const m = weight > 0 && model ? modelValues(model, tSec, geometry) : null;
  return {
    key: `${geometryKey(geometry)}|nowcast|${tSec}|${nowcast.key}|${m?.key ?? '-'}`,
    geometry,
    values: () => {
      const field = extrapolate(source, geometry.width, geometry.height, motion, Math.round(lead / NOWCAST_STEP_S), CHANNELS);
      if (m) blendFields(field, m.values(), weight);
      return field;
    },
  };
}
