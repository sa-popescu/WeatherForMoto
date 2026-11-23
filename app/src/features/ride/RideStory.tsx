import { useEffect, useState } from 'react';
import { fmt, useLang, useStrings, type Lang } from '../../lib/i18n';
import { noteUse } from '../../lib/usage';
import { useToast } from '../../state/toast';
import { Icon } from '../../ui/icons';
import { Button, Spinner } from '../../ui/primitives';
import { averageKmh } from './logic/clock';
import { hasLean } from './logic/lean';
import { fmtClockTime, fmtDuration, fmtKm, fmtKmh, msToKmh } from './logic/rideFormat';
import type { RideStory as Story } from './logic/story';
import { rideRecorder, type RideSnapshot } from './recorder';
import { drawStoryCard } from './storyCard';
import { STORY_S } from './storyStrings';
import { storyLines } from './storyText';
import { RIDE_S } from './strings';
import { useRideStory } from './useRideStory';

// "Povestea turei" in a finished ride's summary: the weather it actually met,
// in a few lines, and a picture of the ride to share. The picture is made as
// soon as the story is there, so sharing it is one tap (Safari only shares
// straight from a tap).

function whenText(startedAt: number, lang: Lang): string {
  const day = new Date(startedAt).toLocaleDateString(lang === 'en' ? 'en-GB' : 'ro-RO', { weekday: 'long', day: 'numeric', month: 'short' }).replace('.', '');
  return `${day.charAt(0).toLocaleUpperCase()}${day.slice(1)} · ${fmtClockTime(startedAt, lang)}`;
}

async function storyFile(ride: RideSnapshot, story: Story, lang: Lang): Promise<File> {
  const s = STORY_S[lang];
  const r = RIDE_S[lang];
  const { totals } = ride;
  const stats = [
    { label: r.distance, value: `${fmtKm(totals.distanceM, lang)} km` },
    { label: r.movingTime, value: fmtDuration(totals.movingMs) },
    // The unit goes with the label, so the numbers keep their size.
    { label: `${r.avgMoving} ${r.kmh}`, value: fmtKmh(averageKmh(totals.movingDistanceM, totals.movingMs), lang) },
    { label: `${r.topSpeed} ${r.kmh}`, value: fmtKmh(totals.topSpeedMs > 0 ? msToKmh(totals.topSpeedMs) : null, lang) },
  ];
  if (hasLean(ride.lean)) stats.push({ label: r.leanMax, value: `${Math.round(Math.max(ride.lean.maxLeftDeg, ride.lean.maxRightDeg))}°` });
  const blob = await drawStoryCard({
    title: ride.route ? fmt(s.cardTowards, { name: ride.route.name }) : s.cardFree,
    when: whenText(ride.clock.startedAt ?? Date.now(), lang),
    points: rideRecorder.getPoints(),
    stats,
    lines: storyLines(story, lang).map((l) => l.text),
    brand: 'Motonor',
  });
  return new File([blob], 'motonor-tura.png', { type: 'image/png' });
}

function save(file: File): void {
  const url = URL.createObjectURL(file);
  const link = document.createElement('a');
  link.href = url;
  link.download = file.name;
  document.body.appendChild(link);
  link.click();
  link.remove();
  window.setTimeout(() => URL.revokeObjectURL(url), 1000);
}

export function RideStory({ ride }: { ride: RideSnapshot }) {
  const s = useStrings(STORY_S);
  const lang = useLang();
  const toast = useToast();
  const { status, story, retry } = useRideStory(ride);
  const [file, setFile] = useState<File | null>(null);

  useEffect(() => {
    setFile(null);
    if (!story) return undefined;
    let current = true;
    storyFile(ride, story, lang)
      .then((made) => {
        if (current) setFile(made);
      })
      .catch((err: unknown) => console.warn('[ride] the ride’s picture could not be made', err));
    return () => {
      current = false;
    };
    // The picture follows the story and the language; the ride's numbers do not change once it is finished.
  }, [story, lang]);

  const share = (): void => {
    if (!file) return;
    const data = { files: [file], title: file.name };
    noteUse('ride:story');
    if (typeof navigator.canShare === 'function' && navigator.canShare(data)) {
      navigator.share(data).catch((err: unknown) => {
        if (err instanceof DOMException && err.name === 'AbortError') return;
        console.warn('[ride] sharing the picture failed', err);
        toast(s.shareErr, { tone: 'error' });
      });
      return;
    }
    save(file);
    toast(s.shareReady, { tone: 'success' });
  };

  if (status === 'none') return null;
  return (
    <section className="ride-story" aria-labelledby="ride-story-title">
      <h3 id="ride-story-title" className="eyebrow">
        {s.storyTitle}
      </h3>
      {status === 'loading' && (
        <p className="ride-story__status" role="status">
          <Spinner size={16} /> {s.storyLoading}
        </p>
      )}
      {status === 'offline' && <p className="ride-story__status">{s.storyOffline}</p>}
      {status === 'error' && (
        <div className="ride-story__status">
          <p>{s.storyFailed}</p>
          <Button icon="refresh" onClick={retry}>
            {s.storyRetry}
          </Button>
        </div>
      )}
      {story && (
        <>
          <ul className="ride-story__lines">
            {storyLines(story, lang).map((line) => (
              <li key={line.key}>
                <Icon name={line.icon} size={18} />
                <span>{line.text}</span>
              </li>
            ))}
          </ul>
          <p className="ride-story__note">{s.storyNote}</p>
          <Button size="lg" full icon="share" disabled={!file} onClick={share}>
            {s.share}
          </Button>
        </>
      )}
    </section>
  );
}
