import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useLang } from '../../lib/i18n';
import type { Place } from '../../lib/types';
import { useWideLayout } from '../../lib/useMediaQuery';
import { samePlace, usePlace } from '../../state/place';
import { useWeather } from '../../state/weather';
import { PlaceHeader } from '../place/PlaceHeader';
import type { ScreenProps } from '../types';
import { FavoritesCompare } from './components/FavoritesCompare';
import { NowContent } from './components/NowContent';
import { NowSheets, type NowSheetState } from './components/NowSheets';
import { NowSkeleton } from './components/NowSkeleton';
import { ErrorCard, StatusBanners } from './components/StatusBanners';
import { WeatherSky } from './components/WeatherSky';
import { skyOf } from './logic/sky';
import { useClock } from './useClock';
import { useNowModel } from './useNowModel';
import { useRadarRain } from './useRadarRain';
import './now.css';
import './now-desktop.css';

// "Acum": can I ride now? Verdict, 24 h timeline, rain, readouts, days.
// The previous data stays on screen during refreshes (no skeleton flashes).
// Behind it, the weather right now (WeatherSky), hidden with the screen.

export default function NowScreen({ active }: ScreenProps) {
  const weather = useWeather();
  const lang = useLang();
  const { place, setPlace } = usePlace();
  const nowMs = useClock(active);
  const radarPlace = weather.data ? { lat: weather.data.latitude, lon: weather.data.longitude } : null;
  const radar = useRadarRain(radarPlace, active, nowMs);
  const model = useNowModel(weather.data, nowMs, lang, radar);
  const [sheet, setSheet] = useState<NowSheetState>(null);
  const [selectedTime, setSelectedTime] = useState<string | null>(null);
  const [tableDate, setTableDate] = useState<string | null>(null);
  const wide = useWideLayout();
  const screenRef = useRef<HTMLDivElement>(null);
  // A favourite picked in the comparison, while its forecast loads: the day
  // to show in the hour table, and where to scroll once it is on screen.
  const pendingDate = useRef<string | null>(null);
  const pendingScroll = useRef<ScrollTarget | null>(null);
  const data = weather.data;
  const sky = useMemo(() => (data ? skyOf(data.current) : null), [data]);

  // A new place starts from the default timeline hour and today's hours (or
  // the day picked in the comparison), with no sheet open.
  useEffect(() => {
    setSelectedTime(null);
    setTableDate(pendingDate.current);
    pendingDate.current = null;
    setSheet(null);
  }, [place]);

  // A new place shows a skeleton until its forecast arrives, so the scroll
  // waits for the forecast rather than aiming at the skeleton.
  useEffect(() => {
    if (!data || !pendingScroll.current) return;
    scrollWithin(screenRef.current, pendingScroll.current);
    pendingScroll.current = null;
  }, [data]);

  // A favourite clicked in the comparison: show it from the top, or from the
  // day cards when a day was clicked, since that day's hours sit under them.
  const showFavorite = useCallback(
    (fav: Place, date: string | null) => {
      const where: ScrollTarget = date ? 'days' : 'top';
      if (samePlace(fav, place)) {
        setTableDate(date);
        scrollWithin(screenRef.current, where);
        return;
      }
      pendingDate.current = date;
      pendingScroll.current = where;
      setPlace(fav, 'favorite');
    },
    [place, setPlace],
  );

  return (
    <div className="screen now" hidden={!active} ref={screenRef}>
      {sky && <WeatherSky sky={sky} />}
      <PlaceHeader
        utcOffsetSeconds={data?.utc_offset_seconds ?? null}
        sources={data?.current.sources?.length ?? null}
        onSources={data && model ? () => setSheet({ kind: 'sources' }) : undefined}
        fetchedAt={weather.fetchedAt}
        refreshing={weather.refreshing && data !== null}
        loading={weather.status === 'loading'}
        nowMs={nowMs}
      />
      <StatusBanners source={weather.source} offlineAgeMin={weather.offlineAgeMin} staleReason={weather.staleReason} failedRefresh={weather.error !== null && data !== null} errorDetail={weather.errorDetail} onRetry={weather.refresh} />
      {data && model ? (
        <>
          <NowContent
            data={data}
            model={model}
            selectedTime={selectedTime}
            onSelectTime={setSelectedTime}
            tableDate={tableDate}
            onPickDay={setTableDate}
            onOpen={setSheet}
          />
          <NowSheets sheet={sheet} data={data} model={model} nowMs={nowMs} onClose={() => setSheet(null)} />
        </>
      ) : weather.status === 'error' ? (
        <ErrorCard kind={weather.error} detail={weather.errorDetail} busy={weather.refreshing} onRetry={weather.refresh} />
      ) : (
        <NowSkeleton />
      )}
      {/* Outside the forecast above, so its results outlive a change of place. */}
      {wide && <FavoritesCompare onShow={showFavorite} />}
    </div>
  );
}

type ScrollTarget = 'top' | 'days';

function scrollWithin(screen: HTMLElement | null, where: ScrollTarget): void {
  // The day cards' section, so its title stays in view above the cards.
  const target = where === 'days' ? screen?.querySelector('.now-days')?.closest('section') : screen;
  target?.scrollIntoView({ block: 'start', behavior: 'smooth' });
}
