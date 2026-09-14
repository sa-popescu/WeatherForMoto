import type { WeatherResponse } from '../../../lib/types';
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
      return hour ? <HourSheet hour={hour} nowIso={model.nowIso} onClose={onClose} /> : null;
    }
    case 'day': {
      const day = data.daily.find((d) => d.date === sheet.date);
      if (!day) return null;
      // Today starts at the current hour; other days show all their hours.
      const hours = data.hourly.filter((h) => h.time.startsWith(day.date) && h.time >= model.nowIso);
      return <DaySheet day={day} hours={hours} onClose={onClose} />;
    }
  }
}
