import { describe, expect, it } from 'vitest';
import { readStoryWeather, storyUrl } from '../storyWeather';
import { storyLines } from '../storyText';
import { fmtClockTime } from './rideFormat';
import { periodStart, periodTotals, rideRecords, type StatRide } from './rideStats';
import { buildStory, highestAltitude, storySamples, windChill, type SampleWeather } from './story';
import type { RidePoint } from './track';

/** 09:00 UTC. */
const T0 = Date.UTC(2026, 10, 22, 9, 0);

function point(minute: number, over: Partial<RidePoint> = {}): RidePoint {
  return { t: T0 + minute * 60_000, lat: 44.4 + minute / 1000, lon: 26.1, acc: 5, speed: 20, alt: null, seg: 0, ...over };
}

/** A ride of `minutes`, one point a minute. */
const ride = (minutes: number): RidePoint[] => Array.from({ length: minutes + 1 }, (_, i) => point(i));

describe('storySamples', () => {
  it('takes a sample every five minutes on a short ride, and the last point', () => {
    const samples = storySamples(ride(23));
    expect(samples.map((s) => (s.t - T0) / 60_000)).toEqual([0, 5, 10, 15, 20, 23]);
    expect(samples[0].minutes).toBe(5);
    expect(samples[4].minutes).toBe(3);
    expect(samples[5].minutes).toBe(0);
    expect(samples[0].speedKmh).toBe(72);
  });

  it('keeps to 48 samples on a long ride', () => {
    expect(storySamples(ride(600)).length).toBeLessThanOrEqual(49);
  });
});

describe('the weather met', () => {
  const dry: SampleWeather = { tempC: 12, precipMm: 0, gustKmh: 20, isDay: true };

  it('adds up the rain, the cold at speed, the gusts and the dark', () => {
    const samples = storySamples(ride(60));
    const weather = samples.map((_, i): SampleWeather => {
      if (i === 2 || i === 3) return { ...dry, precipMm: 2.5, tempC: 4 };
      if (i === 5) return { ...dry, gustKmh: 55 };
      if (i >= 10) return { ...dry, isDay: false };
      return dry;
    });
    const story = buildStory(samples, weather, 1250);
    expect(story.rainMin).toBe(10);
    expect(story.rainFrom).toBe(T0 + 10 * 60_000);
    expect(story.rainTo).toBe(T0 + 20 * 60_000);
    expect(story.rainBand).toBe('moderata');
    expect(story.minTempC).toBe(4);
    expect(story.warmestC).toBe(12);
    // 4° at 72 km/h feels like about -2°.
    expect(story.coldest?.feelsC).toBeCloseTo(windChill(4, 72), 6);
    expect(story.coldest?.feelsC).toBeLessThan(-1);
    expect(story.gust).toEqual({ kmh: 55, at: T0 + 25 * 60_000 });
    expect(story.darkMin).toBe(10);
    expect(story.samples).toBe(samples.length);

    // Times are on the phone's clock, whatever zone it is in.
    const at = (minute: number): string => fmtClockTime(T0 + minute * 60_000, 'ro');
    const lines = storyLines(story, 'ro').map((l) => l.text);
    expect(lines[0]).toBe(`Ploaie moderată 10 min, între ${at(10)} și ${at(20)}`);
    expect(lines[1]).toBe(`Între 4° și 12°; cel mai frig resimțit ${Math.round(windChill(4, 72))}° la 72 km/h, la ${at(10)}`);
    expect(lines).toContain(`Rafale de până la 55 km/h, la ${at(25)}`);
    expect(lines).toContain('10 min pe întuneric');
    expect(lines).toContain('Cel mai sus: 1250 m');
  });

  it('says dry, and skips what is not worth a line', () => {
    const samples = storySamples(ride(20));
    const story = buildStory(samples, samples.map(() => dry), null);
    expect(storyLines(story, 'ro').map((l) => l.text)).toEqual(['Uscat tot drumul', '12°']);
    expect(buildStory(samples, samples.map(() => null), null).samples).toBe(0);
  });

  it('leaves the air temperature alone when it is warm or the bike stands', () => {
    expect(windChill(20, 100)).toBe(20);
    expect(windChill(5, 2)).toBe(5);
    expect(windChill(5, 90)).toBeLessThan(0);
  });

  it('finds the highest point without a GPS spike', () => {
    const alts = [800, 820, 2900, 840, 860, 850].map((alt, i) => point(i, { alt }));
    // The 2900 m spike is never the middle of three; 860 m is.
    expect(highestAltitude(alts)).toBe(860);
    expect(highestAltitude([point(0)])).toBeNull();
  });
});

describe('the weather request', () => {
  it('asks once for every sample, in UTC, and reads the hour of each', () => {
    const samples = storySamples(ride(70));
    const url = new URL(storyUrl(samples));
    expect(url.searchParams.get('latitude')?.split(',')).toHaveLength(samples.length);
    expect(url.searchParams.get('timezone')).toBe('GMT');
    expect(url.searchParams.get('start_date')).toBe('2026-11-22');
    const location = (temp: number) => ({ hourly: { time: ['2026-11-22T09:00', '2026-11-22T10:00'], temperature_2m: [temp, temp + 1], precipitation: [0, 1.2], wind_gusts_10m: [10, 30], is_day: [1, 1] } });
    const body = samples.map((_, i) => location(i));
    const weather = readStoryWeather(body, samples);
    expect(weather[0]).toEqual({ tempC: 0, precipMm: 0, gustKmh: 10, isDay: true });
    // 70 minutes in: the 10:00 hour.
    expect(weather[samples.length - 1]).toMatchObject({ precipMm: 1.2, gustKmh: 30 });
    // One location comes back as an object.
    expect(readStoryWeather(location(5), samples.slice(0, 1))[0]?.tempC).toBe(5);
  });
});

describe('statistics', () => {
  const day = (d: number, h = 10): number => new Date(2026, 10, d, h).getTime();
  const rides: StatRide[] = [
    { id: 'a', startedAt: day(3), summary: { distanceM: 120_000, movingDistanceM: 118_000, movingMs: 7_200_000, elapsedMs: 8_000_000, topSpeedMs: 40, maxLeanDeg: 31 } },
    { id: 'b', startedAt: day(17), summary: { distanceM: 40_000, movingDistanceM: 39_000, movingMs: 3_000_000, elapsedMs: 3_200_000, topSpeedMs: 45 } },
    { id: 'c', startedAt: day(21), summary: { distanceM: 60_000, movingDistanceM: 59_000, movingMs: 4_000_000, elapsedMs: 4_100_000, topSpeedMs: 30, maxLeanDeg: 22 } },
  ];
  // Sunday 22 November 2026, noon.
  const now = day(22, 12);

  it('starts the week on Monday', () => {
    expect(new Date(periodStart('week', now)).getDate()).toBe(16);
    expect(new Date(periodStart('month', now)).getDate()).toBe(1);
  });

  it('adds up a period and finds the records', () => {
    expect(periodTotals(rides, 'week', now)).toEqual({ rides: 2, distanceM: 100_000, movingMs: 7_000_000 });
    expect(periodTotals(rides, 'month', now).rides).toBe(3);
    const records = rideRecords(rides);
    expect(records.longest?.id).toBe('a');
    expect(records.fastest).toMatchObject({ id: 'b', value: 45 });
    expect(records.leanest).toMatchObject({ id: 'a', value: 31 });
    expect(rideRecords([]).longest).toBeNull();
  });
});
