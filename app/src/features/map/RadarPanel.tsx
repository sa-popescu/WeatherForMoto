import { CORE } from '../../i18n/core';
import { useLang, useStrings } from '../../lib/i18n';
import { Icon } from '../../ui/icons';
import { Button, cx, Spinner } from '../../ui/primitives';
import { frameClock, frameOffsetMinutes, latestObservedIndex } from './radar';
import { RadarLegend } from './RadarLegend';
import { RadarScrubber } from './RadarScrubber';
import { MAP_STRINGS } from './strings';
import { radarOffsetText } from './texts';
import type { RadarState } from './useRadar';

// Bottom panel for the rain radar: play/pause, the time on screen, the
// scrubber, the colour legend and a pointer to "Acum" for chances and amounts.

export function RadarPanel({ radar }: { radar: RadarState }) {
  const s = useStrings(MAP_STRINGS);
  const core = useStrings(CORE);
  const lang = useLang();
  const frame = radar.index >= 0 && radar.index < radar.frames.length ? radar.frames[radar.index] : null;

  if (radar.status === 'error' || radar.tilesFailed) {
    return (
      <div className="map-panel map-radar-msg" role="status">
        <Icon name="radar" size={22} />
        <span className="map-radar-msg__text">{s.radarUnavailable}</span>
        <Button onClick={radar.retry}>{core.retry}</Button>
      </div>
    );
  }

  if (!frame) {
    return (
      <div className="map-panel map-radar-msg" role="status">
        <Spinner size={20} />
        <span className="map-radar-msg__text">{s.radarLoading}</span>
      </div>
    );
  }

  const relative = radarOffsetText(s, frameOffsetMinutes(frame, Date.now() / 1000));
  const isLatest = radar.index === latestObservedIndex(radar.frames);

  return (
    <section className="map-panel map-radar" aria-label={s.layerRadar}>
      <div className="map-radar__head">
        <button
          type="button"
          className="map-radar__play"
          aria-label={radar.playing ? s.radarPause : s.radarPlay}
          title={radar.playing ? s.radarPause : s.radarPlay}
          onClick={radar.togglePlay}
        >
          <Icon name={radar.playing ? 'pause' : 'play'} size={22} strokeWidth={2.4} />
        </button>
        <div className="map-radar__readout">
          <span className="map-radar__time num">{frameClock(frame.time, lang)}</span>
          <span className="map-radar__rel">{isLatest ? `${relative} · ${s.radarLatest}` : relative}</span>
        </div>
        {radar.tilesLoading && <Spinner size={18} />}
        <span className={cx('map-radar__tag', frame.nowcast && 'map-radar__tag--forecast')}>
          {frame.nowcast ? s.radarForecast : s.radarObserved}
        </span>
      </div>
      <RadarScrubber frames={radar.frames} index={radar.index} onSelect={radar.select} />
      <RadarLegend />
      <p className="map-radar__note">
        {s.radarNote} <a href="#/acum">{s.radarNoteLink}</a>.
      </p>
    </section>
  );
}
