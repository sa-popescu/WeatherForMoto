// Map panes for the weather layers, so they stack the same way whatever order
// they are added in: satellite clouds under the radar, lightning on top of it.
// Leaflet's tiles sit at 200, overlays (the rain and clouds ahead) at 400 and
// markers at 600.

export const PANES = {
  clouds: 'mm-clouds',
  radar: 'mm-radar',
  lightning: 'mm-lightning',
  ahead: 'overlayPane',
} as const;

export const CUSTOM_PANES: ReadonlyArray<readonly [name: string, zIndex: number]> = [
  [PANES.clouds, 300],
  [PANES.radar, 350],
  [PANES.lightning, 380],
];
