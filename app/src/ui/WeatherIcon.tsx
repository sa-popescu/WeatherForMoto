import type { ReactNode } from 'react';
import { iconKeyFor, type WeatherIconKey } from '../lib/weatherCodes';

// Weather glyphs in the same stroke style as the UI icons; sun, rain and
// lightning get their semantic colours from theme tokens.

const CLOUD = 'M7 18.5h10.5a4 4 0 0 0 .6-7.95A6 6 0 0 0 6.6 9.1 4.75 4.75 0 0 0 7 18.5Z';
const CLOUD_HIGH = 'M7 14.5h10.5a4 4 0 0 0 .6-7.95A6 6 0 0 0 6.6 5.1 4.75 4.75 0 0 0 7 14.5Z';
const SUN_RAYS = 'M12 2.5v2M12 19.5v2M4.2 4.2l1.4 1.4M18.4 18.4l1.4 1.4M2.5 12h2M19.5 12h2M4.2 19.8l1.4-1.4M18.4 5.6l1.4-1.4';

const GLYPHS: Record<WeatherIconKey, ReactNode> = {
  sun: (
    <g stroke="var(--t-ok)">
      <circle cx="12" cy="12" r="4" />
      <path d={SUN_RAYS} />
    </g>
  ),
  moon: <path d="M20 14.5A8 8 0 0 1 9.5 4a8 8 0 1 0 10.5 10.5Z" />,
  cloudsun: (
    <>
      <g stroke="var(--t-ok)">
        <path d="M8.5 3v1.2M3.6 5.1l.9.9M2 10h1.2M13.4 5.1l-.9.9" />
        <path d="M5.2 11.6a3.5 3.5 0 0 1 6.3-3.2" />
      </g>
      <path d="M9 20h8.5a3.5 3.5 0 0 0 .4-6.98 5 5 0 0 0-9.6-.6A3.8 3.8 0 0 0 9 20Z" />
    </>
  ),
  cloudmoon: (
    <>
      <path d="M11 4.5A4.5 4.5 0 0 0 5.2 10.6" />
      <path d="M9 20h8.5a3.5 3.5 0 0 0 .4-6.98 5 5 0 0 0-9.6-.6A3.8 3.8 0 0 0 9 20Z" />
    </>
  ),
  cloud: <path d={CLOUD} />,
  fog: <path d="M4 9h16M3 13h18M5 17h14" />,
  drizzle: (
    <>
      <path d={CLOUD_HIGH} />
      <path d="M9 18v1M13 18v1" stroke="var(--rain)" />
    </>
  ),
  rain: (
    <>
      <path d={CLOUD_HIGH} />
      <path d="M9 17.5l-1 2.5M13 17.5l-1 2.5" stroke="var(--rain)" />
    </>
  ),
  heavyrain: (
    <>
      <path d={CLOUD_HIGH} />
      <path d="M8 17.5l-1 2.5M12 17.5l-1 2.5M16 17.5l-1 2.5" stroke="var(--rain)" />
    </>
  ),
  sleet: (
    <>
      <path d={CLOUD_HIGH} />
      <path d="M9 17.5l-1 2.5" stroke="var(--rain)" />
      <path d="M14 18.5h.01M16 20.5h.01" />
    </>
  ),
  snow: (
    <>
      <path d={CLOUD_HIGH} />
      <path d="M8 18h.01M12 18h.01M16 18h.01M10 21h.01M14 21h.01" strokeWidth={2.4} />
    </>
  ),
  storm: (
    <>
      <path d={CLOUD_HIGH} />
      <path d="m12.5 14-2.5 4h3.5l-2 4" stroke="var(--t-evita)" />
    </>
  ),
};

interface WeatherIconProps {
  code: number | null | undefined;
  isDay?: boolean | null;
  size?: number;
  title?: string;
}

export function WeatherIcon({ code, isDay, size = 28, title }: WeatherIconProps) {
  const key = iconKeyFor(code, isDay);
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth={1.7}
      strokeLinecap="round"
      strokeLinejoin="round"
      role={title ? 'img' : undefined}
      aria-hidden={title ? undefined : true}
      focusable="false"
    >
      {title ? <title>{title}</title> : null}
      {GLYPHS[key]}
    </svg>
  );
}
