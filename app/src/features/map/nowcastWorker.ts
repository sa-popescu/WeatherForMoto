import { alphaLevel, echoLevel, estimateMotion, type MotionField, type MotionOptions, type RadarPicture } from './nowcast';

// Measures motion off the main thread, so the map keeps panning and the band
// keeps playing while new pictures are being analysed.

export type PictureKind = 'radar' | 'cloud';

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

scope.onmessage = (event) => {
  const { id, kind, pictures, width, height, options } = event.data;
  try {
    const motion = estimateMotion(pictures, width, height, options, kind === 'cloud' ? alphaLevel : echoLevel);
    scope.postMessage({ id, motion }, [motion.dx.buffer, motion.dy.buffer]);
  } catch (err) {
    scope.postMessage({ id, error: err instanceof Error ? err.message : String(err) }, []);
  }
};
