import qrcode from 'qrcode-generator';
import { useMemo } from 'react';

// A QR code drawn as SVG: one square per dark module, on a white quiet zone.
// Always black on white, whatever the theme: phone cameras read the inverted
// colours of the dark theme poorly. Its own chunk, out of the start-up bundle:
// only the install guide on a computer loads it.

/** The white border scanners need around the code, in modules. */
const QUIET_ZONE = 2;

interface Props {
  value: string;
  label: string;
  size?: number;
}

function modulesPath(value: string): { count: number; path: string } | null {
  try {
    const qr = qrcode(0, 'M');
    qr.addData(value);
    qr.make();
    const count = qr.getModuleCount();
    let path = '';
    for (let row = 0; row < count; row += 1) {
      for (let col = 0; col < count; col += 1) {
        if (qr.isDark(row, col)) path += `M${col} ${row}h1v1h-1z`;
      }
    }
    return { count, path };
  } catch (err) {
    console.warn('[install] the QR code could not be made', err);
    return null;
  }
}

export default function QrCode({ value, label, size = 168 }: Props) {
  const code = useMemo(() => modulesPath(value), [value]);
  if (!code) return null;
  const box = code.count + QUIET_ZONE * 2;
  return (
    <svg
      className="install-qr"
      viewBox={`${-QUIET_ZONE} ${-QUIET_ZONE} ${box} ${box}`}
      width={size}
      height={size}
      role="img"
      aria-label={label}
      shapeRendering="crispEdges"
    >
      <rect x={-QUIET_ZONE} y={-QUIET_ZONE} width={box} height={box} fill="#fff" />
      <path d={code.path} fill="#000" />
    </svg>
  );
}
