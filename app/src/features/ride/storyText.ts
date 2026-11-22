import { fmtNumber } from '../../lib/format';
import { fmt, type Lang } from '../../lib/i18n';
import type { RainBand } from '../../lib/types';
import type { IconName } from '../../ui/icons';
import type { RideStory } from './logic/story';
import { fmtClockTime } from './logic/rideFormat';
import { STORY_S } from './storyStrings';

// A ride's story in lines: the rain, the cold (and how it felt at speed),
// the gusts, the dark and the highest point, each only when it says
// something. The same lines go on the screen and on the picture to share.

export interface StoryLine {
  key: string;
  icon: IconName;
  text: string;
}

/** Gusts weaker than this are not worth a line. */
const GUST_WORTH_KMH = 30;
/** Riding in the dark this long is worth a line. */
const DARK_WORTH_MIN = 5;
/** A ride that went this high is worth saying so. */
const HIGH_WORTH_M = 800;
/** "Felt like" is said only when it is this much colder than the air. */
const FEELS_WORTH_C = 2;

function rainWhat(band: RainBand, lang: Lang): string {
  const s = STORY_S[lang];
  if (band === 'puternica') return s.rainPuternica;
  if (band === 'moderata') return s.rainModerata;
  if (band === 'slaba') return s.rainSlaba;
  return s.rainUrme;
}

const degrees = (c: number, lang: Lang): string => fmtNumber(Math.round(c), lang);

export function storyLines(story: RideStory, lang: Lang): StoryLine[] {
  const s = STORY_S[lang];
  const at = (ms: number): string => fmtClockTime(ms, lang);
  const lines: StoryLine[] = [];

  if (story.rainMin >= 1 && story.rainFrom !== null && story.rainTo !== null) {
    const min = fmt(s.minutes, { n: story.rainMin });
    lines.push({ key: 'rain', icon: 'drop', text: fmt(s.rain, { what: rainWhat(story.rainBand, lang), min, from: at(story.rainFrom), to: at(story.rainTo) }) });
  } else {
    lines.push({ key: 'rain', icon: 'sun', text: s.dryAll });
  }

  if (story.minTempC !== null && story.warmestC !== null) {
    const low = degrees(story.minTempC, lang);
    const high = degrees(story.warmestC, lang);
    let text = low === high ? fmt(s.tempOne, { t: low }) : fmt(s.tempRange, { min: low, max: high });
    const cold = story.coldest;
    if (cold && cold.speedKmh !== null && cold.feelsC <= story.minTempC - FEELS_WORTH_C) {
      text = fmt(s.feels, { temp: text, feels: degrees(cold.feelsC, lang), speed: Math.round(cold.speedKmh), at: at(cold.at) });
    }
    lines.push({ key: 'temp', icon: 'thermo', text });
  }

  if (story.gust && story.gust.kmh >= GUST_WORTH_KMH) {
    lines.push({ key: 'gust', icon: 'wind', text: fmt(s.gust, { kmh: Math.round(story.gust.kmh), at: at(story.gust.at) }) });
  }
  if (story.darkMin >= DARK_WORTH_MIN) lines.push({ key: 'dark', icon: 'moon', text: fmt(s.dark, { min: fmt(s.minutes, { n: story.darkMin }) }) });
  if (story.highestM !== null && story.highestM >= HIGH_WORTH_M) {
    lines.push({ key: 'high', icon: 'road', text: fmt(s.highest, { m: String(story.highestM) }) });
  }
  return lines;
}
