import { useMemo } from 'react';
import type { WeatherResponse } from '../../../lib/types';
import { useWideLayout } from '../../../lib/useMediaQuery';
import { usePersonalScore } from '../../ride/personalStore';
import { hoursOfDay, tableDay } from '../logic/dayHours';
import type { NowModel } from '../useNowModel';
import { AlertBanners } from './AlertBanners';
import { DayHours } from './DayHours';
import { DayList } from './DayList';
import { Hero } from './Hero';
import type { NowSheetState } from './NowSheets';
import { Timeline } from './Timeline';

interface NowContentProps {
  data: WeatherResponse;
  model: NowModel;
  /** Hour picked on the timeline (kept by time so it survives refreshes). */
  selectedTime: string | null;
  onSelectTime: (time: string) => void;
  /** Desktop: the day picked for the hour table (null until one is picked). */
  tableDate: string | null;
  onPickDay: (date: string) => void;
  onOpen: (sheet: NowSheetState) => void;
}

export function NowContent({ data, model, selectedTime, onSelectTime, tableDate, onPickDay, onOpen }: NowContentProps) {
  const wide = useWideLayout();
  // The timeline opens on the current hour: the first thing a rider wants from
  // it is what the weather is doing right now. The hour the verdict hinges on
  // is already named in the headline above. An hour picked by hand is kept
  // until it drops off the left edge.
  const selected = useMemo(() => {
    const picked = selectedTime ? model.bars.findIndex((b) => b.time === selectedTime) : -1;
    return picked >= 0 ? picked : 0;
  }, [model.bars, selectedTime]);

  const score = data.current.moto_score ?? model.bars[0]?.score ?? null;
  const personal = usePersonalScore(data.current);
  const today = model.nowIso.slice(0, 10);
  // A desktop has room for the hours of a day under the day cards, so a card
  // picks the day shown there; a phone opens the day's sheet instead.
  const shownDay = wide ? tableDay(data.daily, tableDate, today) : null;

  return (
    <>
      <AlertBanners alerts={data.alerts ?? []} />
      <Hero score={score} personal={personal} model={model} onScore={() => onOpen({ kind: 'score' })} />
      <Timeline
        bars={model.bars}
        outlook={model.rain}
        nowIso={model.nowIso}
        selected={selected}
        onSelect={(i) => onSelectTime(model.bars[i].time)}
        onOpenHour={(i) => onOpen({ kind: 'hour', time: model.bars[i].time })}
        onRainInfo={() => onOpen({ kind: 'rain' })}
      />
      <DayList
        daily={data.daily}
        today={today}
        picked={shownDay?.date}
        onOpen={(date) => (shownDay ? onPickDay(date) : onOpen({ kind: 'day', date }))}
      />
      {shownDay && (
        <DayHours
          day={shownDay}
          hours={hoursOfDay(data.hourly, shownDay.date, model.nowIso)}
          today={today}
          nowIso={model.nowIso}
          onOpenHour={(time) => onOpen({ kind: 'hour', time })}
        />
      )}
    </>
  );
}
