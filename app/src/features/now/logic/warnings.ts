import { codeIsStale } from '../../../lib/directWeather';
import { fmt, pick, type Lang } from '../../../lib/i18n';
import { getScoringMeta } from '../../../lib/scoring';
import type { CurrentWeather, HourlyWeather } from '../../../lib/types';
import { fmtVisibility, timeLabel } from './formatting';
import { ALERT_TEXTS } from './texts';

// At most two compact hazard banners for now and the next hours, most
// dangerous first (the legacy app could stack eight).

export type WarningKind = 'storm' | 'ice' | 'frost' | 'snow' | 'wind' | 'fog';

export interface NowWarning {
  kind: WarningKind;
  /** Local ISO of the first affected hour; null means now. */
  at: string | null;
  /** Gust km/h or visibility in metres, when relevant. */
  value: number | null;
}

const ORDER: readonly WarningKind[] = ['storm', 'ice', 'frost', 'snow', 'wind', 'fog'];
export const LOOKAHEAD_HOURS = 6;
export const MAX_WARNINGS = 2;

interface Sample {
  at: string | null;
  code: number | null;
  mm: number | null;
  probability: number | null;
  frost: boolean;
  gust: number | null;
  visibilityM: number | null;
}

function limits(): { strongGust: number; fogBelowM: number } {
  const m = getScoringMeta() as ReturnType<typeof getScoringMeta> & { hazards: Record<string, { visibility_tiers?: { below_m: number }[] }> };
  // Strong gusts = the second published gust tier (50 km/h at the time of writing).
  const tiers = [...m.wind_gust_tiers].sort((a, b) => b.above_kmh - a.above_kmh);
  const fogTiers = m.hazards.fog?.visibility_tiers ?? [];
  return { strongGust: tiers[1]?.above_kmh ?? tiers[0]?.above_kmh ?? 50, fogBelowM: fogTiers.length ? Math.max(...fogTiers.map((t) => t.below_m)) : 1000 };
}

function kindsOf(s: Sample): WarningKind[] {
  const hazards = getScoringMeta().hazards;
  const { strongGust, fogBelowM } = limits();
  const kinds: WarningKind[] = [];
  const activeCode = s.code != null && !codeIsStale(s.code, s.mm, s.probability) ? s.code : null;
  if (activeCode != null && hazards.storm?.codes.includes(activeCode)) kinds.push('storm');
  if (activeCode != null && hazards.ice?.codes.includes(activeCode)) kinds.push('ice');
  if (s.frost) kinds.push('frost');
  if (activeCode != null && hazards.snow?.codes.includes(activeCode)) kinds.push('snow');
  if ((s.gust ?? 0) > strongGust) kinds.push('wind');
  if ((s.code != null && hazards.fog?.codes.includes(s.code)) || (s.visibilityM ?? Infinity) < fogBelowM) kinds.push('fog');
  return kinds;
}

export function nowWarnings(current: CurrentWeather, hourly: ReadonlyArray<HourlyWeather>, startIndex: number, lookahead: number = LOOKAHEAD_HOURS): NowWarning[] {
  const samples: Sample[] = [
    {
      at: null,
      code: current.weather_code,
      mm: current.precipitation_mm,
      probability: current.precipitation_probability,
      frost: current.frost_risk === true,
      gust: current.wind_gusts_kmh,
      visibilityM: current.visibility_km == null ? null : current.visibility_km * 1000,
    },
    ...hourly.slice(startIndex + 1, startIndex + 1 + lookahead).map((h) => ({
      at: h.time,
      code: h.weather_code,
      mm: h.precipitation_mm,
      probability: h.precipitation_probability,
      frost: h.frost_risk === true,
      gust: h.wind_gusts_kmh,
      visibilityM: h.visibility,
    })),
  ];
  const found = new Map<WarningKind, NowWarning>();
  for (const s of samples) {
    for (const kind of kindsOf(s)) {
      const existing = found.get(kind);
      if (!existing) found.set(kind, { kind, at: s.at, value: kind === 'wind' ? s.gust : kind === 'fog' ? s.visibilityM : null });
      else if (kind === 'wind' && (s.gust ?? 0) > (existing.value ?? 0)) existing.value = s.gust;
      else if (kind === 'fog' && s.visibilityM != null && s.visibilityM < (existing.value ?? Infinity)) existing.value = s.visibilityM;
    }
  }
  return ORDER.filter((k) => found.has(k)).map((k) => found.get(k) as NowWarning).slice(0, MAX_WARNINGS);
}

export function warningText(w: NowWarning, nowIso: string, lang: Lang): string {
  const t = pick(ALERT_TEXTS, lang);
  const hour = w.at ? timeLabel(w.at, nowIso, lang) : '';
  const vis = w.value != null ? fmtVisibility(w.value, lang) : '–';
  const gust = Math.round(w.value ?? 0);
  const now = w.at === null;
  switch (w.kind) {
    case 'storm':
      return now ? t.warnStormNow : fmt(t.warnStormAt, { hour });
    case 'ice':
      return now ? t.warnIceNow : fmt(t.warnIceAt, { hour });
    case 'frost':
      return now ? t.warnFrostNow : fmt(t.warnFrostAt, { hour });
    case 'snow':
      return now ? t.warnSnowNow : fmt(t.warnSnowAt, { hour });
    case 'wind':
      return now ? fmt(t.warnWindNow, { gust }) : fmt(t.warnWindAt, { gust, hour });
    case 'fog':
      return now ? fmt(t.warnFogNow, { vis }) : fmt(t.warnFogAt, { vis, hour });
  }
}
