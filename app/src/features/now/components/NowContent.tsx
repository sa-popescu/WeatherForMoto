import { useMemo } from 'react';
import type { WeatherResponse } from '../../../lib/types';
import type { NowModel } from '../useNowModel';
import { AlertBanners } from './AlertBanners';
import { DayList } from './DayList';
import { Hero } from './Hero';
import { MoreGrid } from './MoreGrid';
import type { NowSheetState } from './NowSheets';
import { RainCard } from './RainCard';
import { Readouts } from './Readouts';
import { Timeline } from './Timeline';

interface NowContentProps {
  data: WeatherResponse;
  model: NowModel;
  /** Hour picked on the timeline (kept by time so it survives refreshes). */
  selectedTime: string | null;
  onSelectTime: (time: string) => void;
  onOpen: (sheet: NowSheetState) => void;
}

export function NowContent({ data, model, selectedTime, onSelectTime, onOpen }: NowContentProps) {
  // The timeline opens on the current hour: the first thing a rider wants from
  // it is what the weather is doing right now. The hour the verdict hinges on
  // is already named in the headline above. An hour picked by hand is kept
  // until it drops off the left edge.
  const selected = useMemo(() => {
    const picked = selectedTime ? model.bars.findIndex((b) => b.time === selectedTime) : -1;
    return picked >= 0 ? picked : 0;
  }, [model.bars, selectedTime]);

  const score = data.current.moto_score ?? model.bars[0]?.score ?? null;

  return (
    <>
      <AlertBanners alerts={data.alerts ?? []} />
      <Hero score={score} model={model} onScore={() => onOpen({ kind: 'score' })} />
      <Timeline
        bars={model.bars}
        nowIso={model.nowIso}
        selected={selected}
        onSelect={(i) => onSelectTime(model.bars[i].time)}
        onOpenHour={(i) => onOpen({ kind: 'hour', time: model.bars[i].time })}
      />
      <RainCard outlook={model.rain} nowIso={model.nowIso} onInfo={() => onOpen({ kind: 'rain' })} />
      <Readouts current={data.current} />
      <MoreGrid current={data.current} />
      <DayList daily={data.daily} today={model.nowIso.slice(0, 10)} onOpen={(date) => onOpen({ kind: 'day', date })} />
    </>
  );
}
