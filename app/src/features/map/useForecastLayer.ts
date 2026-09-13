import * as L from 'leaflet';
import { useEffect, useRef } from 'react';
import { fieldImageData, type ForecastData, type ForecastKind, type GridBounds } from './forecast';

// Paints one forecast hour as a single image over the fetched box. Every pixel
// is computed from the interpolated field, so the bands have real edges; the
// browser is never asked to stretch a coloured grid, which would blend the
// colours and turn light rain into a halo.

/** Output pixels per grid cell. Enough for a smooth edge, small enough to redraw fast. */
const SCALE = 24;

function paint(
  canvas: HTMLCanvasElement,
  kind: ForecastKind,
  values: ReadonlyArray<number | null>,
  cols: number,
  rows: number,
): string | null {
  const ctx = canvas.getContext('2d');
  if (!ctx) return null;
  const image = ctx.createImageData(canvas.width, canvas.height);
  image.data.set(fieldImageData(kind, values, cols, rows, canvas.width, canvas.height));
  ctx.putImageData(image, 0, 0);
  return canvas.toDataURL('image/png');
}

interface Options {
  kind: ForecastKind | null;
  data: ForecastData | null;
  bounds: GridBounds | null;
  index: number;
  opacity: number;
}

export function useForecastLayer(map: L.Map | null, { kind, data, bounds, index, opacity }: Options): void {
  const overlayRef = useRef<L.ImageOverlay | null>(null);
  const canvasRef = useRef<HTMLCanvasElement | null>(null);

  if (!canvasRef.current && typeof document !== 'undefined') {
    canvasRef.current = document.createElement('canvas');
  }

  useEffect(() => {
    const canvas = canvasRef.current;
    const frame = kind && data ? (kind === 'cloud' ? data.cloud : data.rain)[index] : null;
    if (!map || !kind || !data || !bounds || !frame || !canvas) {
      overlayRef.current?.remove();
      overlayRef.current = null;
      return undefined;
    }

    // The output is sized from the grid that was actually fetched.
    canvas.width = data.cols * SCALE;
    canvas.height = data.rows * SCALE;
    const url = paint(canvas, kind, frame, data.cols, data.rows);
    if (!url) return undefined;
    const box = L.latLngBounds([bounds.south, bounds.west], [bounds.north, bounds.east]);

    if (!overlayRef.current) {
      overlayRef.current = L.imageOverlay(url, box, {
        opacity,
        interactive: false,
        className: 'map-forecast-overlay',
      }).addTo(map);
    } else {
      overlayRef.current.setBounds(box);
      overlayRef.current.setUrl(url);
      overlayRef.current.setOpacity(opacity);
    }
    return undefined;
  }, [map, kind, data, bounds, index, opacity]);

  // Leaving the screen (or the map going away) takes the overlay with it.
  useEffect(
    () => () => {
      overlayRef.current?.remove();
      overlayRef.current = null;
    },
    [],
  );
}
