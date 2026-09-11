import { useCallback } from 'react';
import { fmtNumber } from '../../lib/format';
import { fmt, useLang, useStrings } from '../../lib/i18n';
import { Icon } from '../../ui/icons';
import { SectionLabel, Spinner } from '../../ui/primitives';
import { AS } from './actionStrings';
import { isKnownHazardType, serverTimeMs, typeKey, type RouteHazard } from './hazards';
import { ageText, fmtKm } from './routeFormat';
import type { HazardsState } from './useRouteHazards';

// Reports from other riders near the route. The description is user text and
// is only ever rendered as text.

const SEVERITY_COLOR = ['var(--t-ok)', 'var(--t-ok)', 'var(--t-atentie)', 'var(--t-evita)', 'var(--t-evita)'];

/** One line describing a hazard, shared by the list and the map tooltips. */
export function useHazardText() {
  const as = useStrings(AS);
  const lang = useLang();
  const typeWord = useCallback((type: string): string => (isKnownHazardType(type) ? as[typeKey(type)] : as.type_other), [as]);
  const where = useCallback(
    (h: RouteHazard): string =>
      h.offRouteKm < 1
        ? fmt(as.hzOnRoad, { km: fmtKm(h.routeKm, lang) })
        : fmt(as.hzOffRoad, { km: fmtKm(h.routeKm, lang), d: fmtNumber(h.offRouteKm, lang, h.offRouteKm < 10 ? 1 : 0) }),
    [as, lang],
  );
  const summary = useCallback((h: RouteHazard): string => `${typeWord(h.hazard_type)} · ${where(h)} · ${h.description}`, [typeWord, where]);
  return { typeWord, where, summary };
}

export function HazardList({ state }: { state: HazardsState }) {
  const as = useStrings(AS);
  const { typeWord, where } = useHazardText();
  const now = Date.now();
  const sevWords = [as.sev1, as.sev2, as.sev3, as.sev4, as.sev5];

  return (
    <section className="route-hz" aria-labelledby="route-hz-title" aria-busy={state.status === 'loading' || undefined}>
      <SectionLabel id="route-hz-title">{as.hzTitle}</SectionLabel>
      {state.status === 'loading' && state.list.length === 0 && (
        <p className="route-note">
          <Spinner size={16} /> {as.hzLoading}
        </p>
      )}
      {state.status === 'error' && <p className="route-note">{as.hzError}</p>}
      {state.status === 'ready' && state.list.length === 0 && <p className="route-note">{as.hzNone}</p>}
      {state.list.length > 0 && (
        <ul className="route-hz__list">
          {state.list.map((h) => {
            const sev = Math.min(5, Math.max(1, Math.round(h.severity)));
            return (
              <li key={h.id} className="route-hz__item">
                <span className="route-hz__icon" style={{ color: SEVERITY_COLOR[sev - 1] }}>
                  <Icon name="alert" size={26} />
                </span>
                <span className="route-hz__text">
                  <span className="route-hz__title">{typeWord(h.hazard_type)}</span>
                  <span className="route-hz__meta">
                    {where(h)} · {ageText(serverTimeMs(h.created_at), now, as)}
                  </span>
                  <span className="route-hz__meta">{fmt(as.hzSev, { n: sev, word: sevWords[sev - 1] })}</span>
                  {h.description && <span className="route-hz__desc">{h.description}</span>}
                </span>
              </li>
            );
          })}
        </ul>
      )}
    </section>
  );
}
