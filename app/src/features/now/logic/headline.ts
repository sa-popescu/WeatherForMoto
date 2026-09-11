import { hourOf } from '../../../lib/format';
import { fmt, pick, type Lang } from '../../../lib/i18n';
import { rainBandOf } from '../../../lib/scoring';
import type { CurrentWeather, DailyWeather, HourlyWeather } from '../../../lib/types';
import { causeFromCurrent, causeFromHours, causeSentence, type Cause } from './cause';
import { isDaylight, nextSunrise, sunTimes } from './daylight';
import { capitalize, maxOf, rainPhrase } from './formatting';
import { countedMm, isWetHour } from './rainOutlook';
import { TEXTS } from './texts';
import { computeVerdict, daylightAhead, type Verdict } from './verdict';

// Hero text: the verdict sentence and a sub-line naming the main cause.

export interface Headline {
  verdict: Verdict;
  title: string;
  sub: string;
  /** The hour the verdict hinges on (drop or recovery), for the timeline callout. */
  keyTime: string | null;
}

export interface HeadlineInput {
  current: CurrentWeather;
  hourly: ReadonlyArray<HourlyWeather>;
  startIndex: number;
  daily: ReadonlyArray<DailyWeather>;
  lang: Lang;
}

function goTitle(cause: Cause | null, verdict: Extract<Verdict, { kind: 'go' }>, lang: Lang): string {
  const t = pick(TEXTS, lang);
  if (verdict.night) return t.goNight;
  if (verdict.tier === 'ideal' || !cause) return verdict.tier === 'ideal' ? t.goIdeal : t.goOk;
  switch (cause.factor) {
    case 'rain':
      return ['none', 'urme'].includes(rainBandOf(cause.mm)) ? t.goRain : t.goRainLight;
    case 'wind':
      return t.goWind;
    case 'cold':
    case 'cold_wet':
      return t.goCold;
    case 'heat':
      return t.goHeat;
    case 'fog':
      return t.goFog;
    default:
      return t.goOk;
  }
}

/** Sub-line when nothing bad is ahead: rain chances if any, else a dry summary. */
function calmSub(rest: ReadonlyArray<HourlyWeather>, nowIso: string, daily: ReadonlyArray<DailyWeather>, lang: Lang): string {
  const t = pick(TEXTS, lang);
  const wet = rest.filter(isWetHour);
  if (wet.length) {
    const phrase = rainPhrase({ probability: maxOf(wet.map((h) => h.precipitation_probability)), mm: maxOf(wet.map(countedMm)) }, lang);
    return wet[0].time.slice(0, 13) > nowIso.slice(0, 13) ? `${fmt(t.fromHour, { hour: hourOf(wet[0].time), text: phrase })}.` : `${capitalize(phrase)}.`;
  }
  const gust = Math.round(maxOf(rest.map((h) => h.wind_gusts_kmh)) ?? 0);
  const { sunset } = sunTimes(nowIso.slice(0, 10), daily);
  return sunset && sunset > nowIso ? fmt(t.dryUntilSunset, { sunset: hourOf(sunset), gust }) : fmt(t.dryAhead, { gust });
}

export function buildHeadline({ current, hourly, startIndex, daily, lang }: HeadlineInput): Headline {
  const t = pick(TEXTS, lang);
  const now = hourly[startIndex];
  const nowScore = current.moto_score ?? now?.moto_score ?? null;
  const verdict = computeVerdict({ nowScore, hourly, startIndex, daily });
  if (!now || verdict.kind === 'unknown') return { verdict, title: t.unknown, sub: '', keyTime: null };

  const nowIso = now.time;
  const ahead = daylightAhead(hourly, startIndex, daily);
  const today = hourly.slice(startIndex).filter((h) => h.time.startsWith(nowIso.slice(0, 10)));
  const nowCause = causeFromCurrent(current);

  switch (verdict.kind) {
    case 'go': {
      const title = goTitle(nowCause, verdict, lang);
      if (verdict.night) {
        const sunrise = nextSunrise(nowIso, daily);
        const night = sunrise ? fmt(t.night, { sunrise: hourOf(sunrise) }) : t.nightNoSunrise;
        return { verdict, title, sub: verdict.tier === 'ok' && nowCause ? `${causeSentence(nowCause, lang)} ${night}` : night, keyTime: null };
      }
      const sub = verdict.tier === 'ok' && nowCause ? causeSentence(nowCause, lang) : calmSub([now, ...ahead], nowIso, daily, lang);
      return { verdict, title, sub, keyTime: null };
    }
    case 'goUntil': {
      const bad = ahead.filter((h) => h.time >= verdict.until);
      const cause = causeFromHours(bad);
      return { verdict, title: fmt(t.goUntil, { hour: hourOf(verdict.until) }), sub: cause ? causeSentence(cause, lang, nowIso) : '', keyTime: verdict.until };
    }
    case 'notNow': {
      const stretch = hourly.slice(startIndex).filter((h) => h.time < verdict.from);
      const cause = nowCause ?? causeFromHours(stretch);
      const title = fmt(verdict.tomorrow ? t.notNowTomorrow : t.notNowLater, { hour: hourOf(verdict.from) });
      return { verdict, title, sub: cause ? causeSentence(cause, lang) : '', keyTime: verdict.from };
    }
    case 'notToday': {
      const riding = today.filter((h) => isDaylight(h, daily));
      const cause = causeFromHours(riding.length ? riding : today) ?? nowCause;
      const tomorrow = verdict.tomorrow ? fmt(t.tomorrowGood, { hour: hourOf(verdict.tomorrow.from), score: verdict.tomorrow.avg }) : t.tomorrowBad;
      const sub = [cause ? causeSentence(cause, lang, nowIso) : '', tomorrow].filter(Boolean).join(' ');
      return { verdict, title: t.notToday, sub, keyTime: verdict.tomorrow?.from ?? null };
    }
  }
}
