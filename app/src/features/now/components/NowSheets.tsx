import type { WeatherResponse } from '../../../lib/types';
import { hoursOfDay } from '../logic/dayHours';
import type { NowModel } from '../useNowModel';
import { DaySheet } from './DaySheet';
import { HourSheet } from './HourSheet';
import { RainSheet } from './RainSheet';
import { ScoreSheet } from './ScoreSheet';
import { SourcesSheet } from './SourcesSheet';
import '../now-sheets.css';

export type NowSheetState =
  | { kind: 'score' }
  | { kind: 'rain' }
  | { kind: 'sources' }
  | { kind: 'hour'; time: string }
  | { kind: 'day'; date: string }
  | null;

interface NowSheetsProps {
  sheet: NowSheetState;
  data: WeatherResponse;
  model: NowModel;
  nowMs: number;
  onClose: () => void;
}

/** At most one sheet at a time; closed sheets are not rendered at all. */
export function NowSheets({ sheet, data, model, nowMs, onClose }: NowSheetsProps) {
  if (!sheet) return null;
  switch (sheet.kind) {
    case 'score':
      return <ScoreSheet current={data.current} onClose={onClose} />;
    case 'sources':
      return <SourcesSheet current={data.current} nowMs={nowMs} onClose={onClose} />;
    case 'rain':
      return <RainSheet outlook={model.rain} onClose={onClose} />;
    case 'hour': {
      const hour = data.hourly.find((h) => h.time === sheet.time);
      // Air quality and pollen exist only for the present, so only the current hour shows them.
      const current = sheet.time === model.bars[0]?.time ? data.current : null;
      return hour ? <HourSheet hour={hour} current={current} nowIso={model.nowIso} onClose={onClose} /> : null;
    }
    case 'day': {
      const day = data.daily.find((d) => d.date === sheet.date);
      if (!day) return null;
      return <DaySheet day={day} hours={hoursOfDay(data.hourly, day.date, model.nowIso)} onClose={onClose} />;
    }
  }
}
