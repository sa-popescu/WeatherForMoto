import { amountLevel } from './cloudValues';
import { echoLevel, estimateMotion, type LevelOf, type MotionField, type MotionOptions, type RadarPicture } from './nowcast';
import { codeLevel } from './rainValues';

// Measures motion off the main thread, so the map keeps panning and the band
// keeps playing while new pictures are being analysed.

/** Radar tiles as pictures ("Acum"), or rain and cloud values sent in RGBA form (rainValues.fieldToRgba). */
export type PictureKind = 'radar' | 'rain' | 'cloud';

export interface MotionRequest {
  id: number;
  kind: PictureKind;
  pictures: RadarPicture[];
  width: number;
  height: number;
  options: MotionOptions;
}

export type MotionReply = { id: number; motion: MotionField } | { id: number; error: string };

interface WorkerScope {
  onmessage: ((event: MessageEvent<MotionRequest>) => void) | null;
  postMessage: (message: MotionReply, transfer: Transferable[]) => void;
}

const scope = self as unknown as WorkerScope;

const LEVELS: Record<PictureKind, LevelOf> = { radar: echoLevel, rain: codeLevel, cloud: amountLevel };

scope.onmessage = (event) => {
  const { id, kind, pictures, width, height, options } = event.data;
  try {
    const motion = estimateMotion(pictures, width, height, options, LEVELS[kind]);
    scope.postMessage({ id, motion }, [motion.dx.buffer, motion.dy.buffer]);
  } catch (err) {
    scope.postMessage({ id, error: err instanceof Error ? err.message : String(err) }, []);
  }
};
