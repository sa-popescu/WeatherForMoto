import type { LngLat, MapLibreMap, MapTouchEvent, Point } from 'maplibre-gl';

// A long press on a touch screen, which MapLibre does not report by itself
// (Leaflet used to turn it into a context menu event). One finger, held still.

const LONG_PRESS_MS = 600;
/** A finger that moves further than this is panning, not pressing. */
const MOVE_TOLERANCE_PX = 10;

/** Calls `handler` with the pressed point; returns the function that stops listening. */
export function onLongPress(map: MapLibreMap, handler: (at: LngLat) => void): () => void {
  let timer = 0;
  let start: Point | null = null;
  const cancel = (): void => {
    window.clearTimeout(timer);
    start = null;
  };
  const onStart = (event: MapTouchEvent): void => {
    cancel();
    if (event.points.length !== 1) return;
    start = event.point;
    const at = event.lngLat;
    timer = window.setTimeout(() => {
      start = null;
      handler(at);
    }, LONG_PRESS_MS);
  };
  const onMove = (event: MapTouchEvent): void => {
    if (start && (event.points.length !== 1 || event.point.dist(start) > MOVE_TOLERANCE_PX)) cancel();
  };
  map.on('touchstart', onStart);
  map.on('touchmove', onMove);
  map.on('touchend', cancel);
  map.on('touchcancel', cancel);
  return () => {
    cancel();
    map.off('touchstart', onStart);
    map.off('touchmove', onMove);
    map.off('touchend', cancel);
    map.off('touchcancel', cancel);
  };
}
