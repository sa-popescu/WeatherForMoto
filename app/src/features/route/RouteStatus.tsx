import { fmt, useStrings } from '../../lib/i18n';
import { CORE } from '../../i18n/core';
import { Icon } from '../../ui/icons';
import { Banner, Button, Card, Spinner } from '../../ui/primitives';
import { RS } from './strings';
import type { PlanState } from './useRoutePlan';

// Progress per step, errors with retry, the "stops changed" hint and the
// empty state with a ready-made example.

interface RouteStatusProps {
  state: PlanState;
  settled: number;
  failed: number;
  stale: boolean;
  onRetry: () => void;
  onRetryWeather: () => void;
  onExample: () => void;
}

export function RouteStatus({ state, settled, failed, stale, onRetry, onRetryWeather, onExample }: RouteStatusProps) {
  const s = useStrings(RS);
  const core = useStrings(CORE);
  const { step, error, points } = state;

  if (step === 'geocoding' || step === 'routing') {
    return (
      <div className="route-progress" role="status">
        <Spinner size={20} />
        <span>{step === 'geocoding' ? s.stepGeocoding : s.stepRouting}</span>
      </div>
    );
  }

  if (step === 'error' && error) {
    const message = error.kind === 'no-route' ? s.errNoRoute : error.kind === 'geocode' ? fmt(s.errGeocode, { name: error.name ?? '' }) : s.errNetwork;
    return (
      <Banner tone="error" icon="alert" action={<Button onClick={onRetry}>{core.retry}</Button>}>
        {message}
      </Banner>
    );
  }

  if (step === 'idle') {
    return (
      <Card className="route-empty">
        <span className="route-empty__icon" aria-hidden="true">
          <Icon name="route" size={28} />
        </span>
        <h2 className="route-empty__title num">{s.emptyTitle}</h2>
        <p className="muted">{s.emptyBody}</p>
        <p className="route-empty__example">{s.emptyExample}</p>
        <Button icon="play" onClick={onExample}>
          {s.tryExample}
        </Button>
      </Card>
    );
  }

  const total = points.length;
  return (
    <>
      {step === 'weather' && total > 0 && (
        <div className="route-progress route-progress--bar" role="status">
          <span className="route-progress__text">
            <Spinner size={18} /> {fmt(s.stepWeather, { done: settled, total })}
          </span>
          <span className="route-progress__track" role="progressbar" aria-valuemin={0} aria-valuemax={total} aria-valuenow={settled}>
            <span className="route-progress__fill" style={{ width: `${Math.round((settled / total) * 100)}%` }} />
          </span>
        </div>
      )}
      {step === 'ready' && failed > 0 && (
        <Banner tone="warn" icon="alert" action={<Button onClick={onRetryWeather}>{core.retry}</Button>}>
          {fmt(s.errWeather, { n: failed })}
        </Banner>
      )}
      {stale && (
        <Banner tone="warn" icon="info">
          {s.stale}
        </Banner>
      )}
    </>
  );
}
