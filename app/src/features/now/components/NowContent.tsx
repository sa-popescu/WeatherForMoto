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
  // Default callout: the hour the verdict hinges on, else the rain peak, else now.
  const selected = useMemo(() => {
    const find = (time: string | null): number => (time ? model.bars.findIndex((b) => b.time === time) : -1);
    const rainPeak = model.rain.kind === 'episode' ? (model.rain.peakMmTime ?? model.rain.start) : null;
    const candidates = [selectedTime, model.headline.keyTime, rainPeak];
    for (const time of candidates) {
      const i = find(time);
      if (i >= 0) return i;
    }
    return 0;
  }, [model, selectedTime]);

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
