import { scoreBreakdown, scoreHour } from '../../../lib/directWeather';
import { fmtTemp } from '../../../lib/format';
import { fmt, pick, type Lang } from '../../../lib/i18n';
import type { CurrentWeather, HourlyWeather, ScoreFactor } from '../../../lib/types';
import { capitalize, fmtVisibility, maxOf, minOf, rainPhrase, timeLabel } from './formatting';
import { TEXTS } from './texts';

// The main reason a score is low, in plain language. For "now" it uses the
// backend's score_breakdown; for future hours it re-derives each hour's
// factors with the same published rules (scoreHour).

export type CauseFactor = 'storm' | 'ice' | 'snow' | 'frost' | 'rain' | 'fog' | 'wind' | 'cold_wet' | 'cold' | 'heat';

/** Tie-break order: the more dangerous factor wins when two cost the same. */
const PRIORITY: readonly CauseFactor[] = ['storm', 'ice', 'snow', 'frost', 'rain', 'fog', 'wind', 'cold_wet', 'cold', 'heat'];

export interface Cause {
  factor: CauseFactor;
  /** Local ISO of the first affected hour; null when it describes "now". */
  from: string | null;
  /** Points the factor takes off (penalty, or 100 minus its cap). */
  reduction: number;
  probability: number | null;
  mm: number | null;
  gust: number | null;
  feels: number | null;
  visibilityM: number | null;
  road: number | null;
}

export function reductionOf(f: ScoreFactor): number {
  return Math.max(f.penalty, f.cap == null ? 0 : 100 - f.cap);
}

export function isCauseFactor(name: string): name is CauseFactor {
  return (PRIORITY as readonly string[]).includes(name);
}

function strongest(factors: ReadonlyArray<ScoreFactor>): { factor: CauseFactor; reduction: number } | null {
  let best: { factor: CauseFactor; reduction: number } | null = null;
  for (const f of factors) {
    if (!isCauseFactor(f.factor)) continue;
    const reduction = reductionOf(f);
    if (reduction <= 0) continue;
    const better = !best || reduction > best.reduction || (reduction === best.reduction && PRIORITY.indexOf(f.factor) < PRIORITY.indexOf(best.factor));
    if (better) best = { factor: f.factor, reduction };
  }
  return best;
}

/** Strongest factor over a span of hours, with its extremes over the hours it affects. */
export function causeFromHours(hours: ReadonlyArray<HourlyWeather>): Cause | null {
  const perHour = hours.map((h) => ({ h, factors: scoreHour(h).factors }));
  const top = strongest(perHour.flatMap((x) => x.factors));
  if (!top) return null;
  const hit = perHour.filter((x) => x.factors.some((f) => f.factor === top.factor && reductionOf(f) > 0)).map((x) => x.h);
  const feels = hit.map((h) => h.feels_like);
  return {
    factor: top.factor,
    from: hit[0]?.time ?? null,
    reduction: top.reduction,
    probability: maxOf(hit.map((h) => h.precipitation_probability)),
    mm: maxOf(hit.map((h) => h.precipitation_mm)),
    gust: maxOf(hit.map((h) => h.wind_gusts_kmh)),
    feels: top.factor === 'heat' ? maxOf(feels) : minOf(feels),
    visibilityM: minOf(hit.map((h) => h.visibility)),
    road: minOf(hit.map((h) => h.road_surface_temp)),
  };
}

/** Strongest factor right now, from the backend breakdown (or the local rules as a fallback). */
export function causeFromCurrent(c: CurrentWeather): Cause | null {
  const visibilityM = c.visibility_km == null ? null : c.visibility_km * 1000;
  const factors =
    c.score_breakdown ??
    scoreBreakdown({
      feelsLike: c.feels_like,
      gustsKmh: c.wind_gusts_kmh,
      precipitationMm: c.precipitation_mm,
      weatherCode: c.weather_code,
      probability: c.precipitation_probability,
      visibilityM,
      frostRisk: c.frost_risk === true,
    }).factors;
  const top = strongest(factors);
  if (!top) return null;
  return {
    factor: top.factor,
    from: null,
    reduction: top.reduction,
    probability: c.precipitation_probability,
    mm: c.precipitation_mm,
    gust: c.wind_gusts_kmh,
    feels: c.feels_like,
    visibilityM,
    road: c.road_surface_temp,
  };
}

export function causePhrase(c: Cause, lang: Lang): string {
  const t = pick(TEXTS, lang);
  const rain = rainPhrase({ probability: c.probability, mm: c.mm }, lang);
  const feels = fmtTemp(c.feels, lang);
  switch (c.factor) {
    case 'rain':
      return rain;
    case 'storm':
      return fmt(t.causeStorm, { rain });
    case 'snow':
      return fmt(t.causeSnow, { feels });
    case 'ice':
      return t.causeIce;
    case 'fog':
      return c.visibilityM != null && c.visibilityM < 5000 ? fmt(t.causeFog, { vis: fmtVisibility(c.visibilityM, lang) }) : t.causeFogPlain;
    case 'wind':
      return fmt(t.causeWind, { gust: Math.round(c.gust ?? 0) });
    case 'cold':
      return fmt(t.causeCold, { feels });
    case 'heat':
      return fmt(t.causeHeat, { feels });
    case 'cold_wet':
      return fmt(t.causeColdWet, { feels, rain });
    case 'frost':
      return fmt(t.causeFrost, { road: fmtTemp(c.road, lang) });
  }
}

/** Full sentence; with `nowIso` and a later `from`, prefixed "De la 16:00: ...". */
export function causeSentence(c: Cause, lang: Lang, nowIso?: string): string {
  const phrase = causePhrase(c, lang);
  if (nowIso && c.from && c.from.slice(0, 13) > nowIso.slice(0, 13)) {
    return `${fmt(pick(TEXTS, lang).fromHour, { hour: timeLabel(c.from, nowIso, lang), text: phrase })}.`;
  }
  return `${capitalize(phrase)}.`;
}
