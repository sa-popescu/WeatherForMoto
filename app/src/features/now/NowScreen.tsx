import { useEffect, useState } from 'react';
import { useLang } from '../../lib/i18n';
import { usePlace } from '../../state/place';
import { useWeather } from '../../state/weather';
import { PlaceHeader } from '../place/PlaceHeader';
import type { ScreenProps } from '../types';
import { NowContent } from './components/NowContent';
import { NowSheets, type NowSheetState } from './components/NowSheets';
import { NowSkeleton } from './components/NowSkeleton';
import { ErrorCard, StatusBanners } from './components/StatusBanners';
import { useClock } from './useClock';
import { useNowModel } from './useNowModel';
import './now.css';

// "Acum": can I ride now? Verdict, 24 h timeline, rain, readouts, gear, days.
// The previous data stays on screen during refreshes (no skeleton flashes).

export default function NowScreen({ active }: ScreenProps) {
  const weather = useWeather();
  const lang = useLang();
  const { place } = usePlace();
  const nowMs = useClock(active);
  const model = useNowModel(weather.data, nowMs, lang);
  const [sheet, setSheet] = useState<NowSheetState>(null);
  const [selectedTime, setSelectedTime] = useState<string | null>(null);

  // A new place starts from the default timeline hour, with no sheet open.
  useEffect(() => {
    setSelectedTime(null);
    setSheet(null);
  }, [place]);

  const data = weather.data;
  return (
    <div className="screen now" hidden={!active}>
      <PlaceHeader
        utcOffsetSeconds={data?.utc_offset_seconds ?? null}
        sources={data?.current.sources?.length ?? null}
        fetchedAt={weather.fetchedAt}
        refreshing={weather.refreshing && data !== null}
        loading={weather.status === 'loading'}
        nowMs={nowMs}
      />
      <StatusBanners source={weather.source} offlineAgeMin={weather.offlineAgeMin} failedRefresh={weather.error !== null && data !== null} onRetry={weather.refresh} />
      {data && model ? (
        <>
          <NowContent data={data} model={model} selectedTime={selectedTime} onSelectTime={setSelectedTime} onOpen={setSheet} />
          <NowSheets sheet={sheet} data={data} model={model} onClose={() => setSheet(null)} />
        </>
      ) : weather.status === 'error' ? (
        <ErrorCard kind={weather.error} busy={weather.refreshing} onRetry={weather.refresh} />
      ) : (
        <NowSkeleton />
      )}
    </div>
  );
}
