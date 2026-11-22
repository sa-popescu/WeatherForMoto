import type { ReactNode } from 'react';
import type { TurnShape } from './turnText';

// The arrow of the next turn: line drawings in the style of the app's icons
// (ui/icons.tsx), stroked with the current colour.

const PATHS: Record<TurnShape, ReactNode> = {
  right: (
    <>
      <path d="M7 20v-7a4 4 0 0 1 4-4h8" />
      <path d="m15 5 4 4-4 4" />
    </>
  ),
  left: (
    <>
      <path d="M17 20v-7a4 4 0 0 0-4-4H5" />
      <path d="M9 5 5 9l4 4" />
    </>
  ),
  'bear-right': (
    <>
      <path d="M8 20v-6a4 4 0 0 1 1.2-2.8L17 4" />
      <path d="M11 4h6v6" />
    </>
  ),
  'bear-left': (
    <>
      <path d="M16 20v-6a4 4 0 0 0-1.2-2.8L7 4" />
      <path d="M13 4H7v6" />
    </>
  ),
  'sharp-right': (
    <>
      <path d="M8 3v10a3 3 0 0 0 5 2.2L19 10" />
      <path d="M19 16v-6h-6" />
    </>
  ),
  'sharp-left': (
    <>
      <path d="M16 3v10a3 3 0 0 1-5 2.2L5 10" />
      <path d="M5 16v-6h6" />
    </>
  ),
  straight: (
    <>
      <path d="M12 20V4" />
      <path d="m7 9 5-5 5 5" />
    </>
  ),
  uturn: (
    <>
      <path d="M16 20V10a4 4 0 0 0-8 0v7" />
      <path d="m4 13 4 4 4-4" />
    </>
  ),
  roundabout: (
    <>
      <circle cx="12" cy="10" r="3.5" />
      <path d="M12 13.5V21" />
      <path d="M14.5 7.5 19 3" />
      <path d="M15 3h4v4" />
    </>
  ),
  merge: (
    <>
      <path d="M8 21v-3c0-4 8-5 8-10V4" />
      <path d="m12 8 4-4 4 4" />
    </>
  ),
  arrive: <path d="M6 21V4h11l-2 4 2 4H6" />,
  ferry: (
    <>
      <path d="M3 15h18l-3 5H6Z" />
      <path d="M12 4v11" />
      <path d="M12 5l6 6h-6" />
    </>
  ),
};

export function TurnIcon({ shape, size = 40 }: { shape: TurnShape; size?: number }) {
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth={2.4}
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
      focusable="false"
    >
      {PATHS[shape]}
    </svg>
  );
}
