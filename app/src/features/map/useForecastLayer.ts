import * as L from 'leaflet';
import { useEffect, useRef } from 'react';
import { frameImageData, GRID_COLS, GRID_ROWS, type ForecastData, type ForecastKind, type GridBounds } from './forecast';

// Paints one forecast hour as a single image stretched over the fetched box.
// The grid is drawn at one pixel per cell and scaled up by the browser, which
// interpolates it into a smooth field instead of a chequerboard.

/** How much the tiny grid is enlarged before it becomes the overlay image. */
const SCALE = 24;

function paint(canvas: HTMLCanvasElement, kind: ForecastKind, values: ReadonlyArray<number | null>): string | null {
  const cells = document.createElement('canvas');
  cells.width = GRID_COLS;
  cells.height = GRID_ROWS;
  const cellCtx = cells.getContext('2d');
  const ctx = canvas.getContext('2d');
  if (!cellCtx || !ctx) return null;
  const image = cellCtx.createImageData(GRID_COLS, GRID_ROWS);
  image.data.set(frameImageData(kind, values));
  cellCtx.putImageData(image, 0, 0);
  ctx.clearRect(0, 0, canvas.width, canvas.height);
  ctx.imageSmoothingEnabled = true;
  ctx.imageSmoothingQuality = 'high';
  ctx.drawImage(cells, 0, 0, canvas.width, canvas.height);
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
    const canvas = document.createElement('canvas');
    canvas.width = GRID_COLS * SCALE;
    canvas.height = GRID_ROWS * SCALE;
    canvasRef.current = canvas;
  }

  useEffect(() => {
    const canvas = canvasRef.current;
    const frame = kind && data ? (kind === 'cloud' ? data.cloud : data.rain)[index] : null;
    if (!map || !kind || !data || !bounds || !frame || !canvas) {
      overlayRef.current?.remove();
      overlayRef.current = null;
      return undefined;
    }

    const url = paint(canvas, kind, frame);
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
