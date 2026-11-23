import { useEffect, useRef, useState, type KeyboardEvent } from 'react';
import { fmt, useStrings } from '../../lib/i18n';
import { Icon } from '../../ui/icons';
import { Button, cx, Segmented, Toggle } from '../../ui/primitives';
import { Sheet } from '../../ui/Sheet';
import {
  AVOIDABLE,
  DEFAULT_ROUTE_OPTIONS,
  LEVELS,
  queryKey,
  ROUTE_TYPES,
  setRouteOptions,
  TOP_SPEEDS,
  useRouteOptions,
  type RouteOptions,
  type RouteType,
  type TopSpeed,
} from './routeOptions';
import { RO_S } from './routeOptionsStrings';
import { avoidName, curvesName, hillsName, optionsSummary, typeDescription, typeName } from './routeOptionsText';
import './route-options.css';

// The sheet that sets the route options, and the button that opens it (Drum,
// the planner) or the row in "Eu". The choices are a draft while the sheet is
// open and apply when it closes, so trying three of them asks TomTom once,
// not three times.

interface SheetProps {
  open: boolean;
  /** `changed` is true when the options differ from the ones the sheet opened with. */
  onClose: (changed: boolean) => void;
}

export function RouteOptionsSheet({ open, onClose }: SheetProps) {
  const s = useStrings(RO_S);
  const saved = useRouteOptions();
  const [draft, setDraft] = useState<RouteOptions>(saved);
  const typeRefs = useRef<Array<HTMLButtonElement | null>>([]);

  // Each opening starts from what is saved.
  useEffect(() => {
    if (open) setDraft(saved);
  }, [open, saved]);

  const close = (): void => {
    const changed = queryKey(draft) !== queryKey(saved);
    if (changed) setRouteOptions(draft);
    onClose(changed);
  };
  const patch = (next: Partial<RouteOptions>): void => setDraft((d) => ({ ...d, ...next }));
  const toggleAvoid = (what: (typeof AVOIDABLE)[number], on: boolean): void =>
    setDraft((d) => ({ ...d, avoid: AVOIDABLE.filter((a) => (a === what ? on : d.avoid.includes(a))) }));

  // A radio group: Tab enters once, the arrow keys move and pick.
  const onTypeKey = (event: KeyboardEvent<HTMLButtonElement>, index: number): void => {
    const step = event.key === 'ArrowDown' || event.key === 'ArrowRight' ? 1 : event.key === 'ArrowUp' || event.key === 'ArrowLeft' ? -1 : 0;
    if (step === 0) return;
    event.preventDefault();
    const next = (index + step + ROUTE_TYPES.length) % ROUTE_TYPES.length;
    patch({ type: ROUTE_TYPES[next] });
    typeRefs.current[next]?.focus();
  };

  const footer = (
    <div className="ropt-footer">
      <Button onClick={() => setDraft(DEFAULT_ROUTE_OPTIONS)}>{s.reset}</Button>
      <Button variant="primary" onClick={close}>
        {s.done}
      </Button>
    </div>
  );

  return (
    <Sheet open={open} onClose={close} title={s.title} footer={footer}>
      <div className="ropt">
        <div className="ropt-types" role="radiogroup" aria-label={s.typeLabel}>
          {ROUTE_TYPES.map((type: RouteType, i) => {
            const on = draft.type === type;
            return (
              <button
                key={type}
                ref={(el) => {
                  typeRefs.current[i] = el;
                }}
                type="button"
                role="radio"
                aria-checked={on}
                tabIndex={on ? 0 : -1}
                className={cx('ropt-type', on && 'ropt-type--on')}
                onClick={() => patch({ type })}
                onKeyDown={(e) => onTypeKey(e, i)}
              >
                <span className="ropt-type__dot" aria-hidden="true" />
                <span className="ropt-type__text">
                  <span className="ropt-type__label">{typeName(type, s)}</span>
                  <span className="ropt-type__desc">{typeDescription(type, s)}</span>
                </span>
              </button>
            );
          })}
        </div>

        {draft.type === 'thrilling' && (
          <div className="ropt-levels">
            <span className="ropt-caption">{s.curvesLabel}</span>
            <Segmented label={s.curvesLabel} options={LEVELS.map((l) => ({ value: l, label: curvesName(l, s) }))} value={draft.windingness} onChange={(windingness) => patch({ windingness })} />
            <span className="ropt-caption">{s.hillsLabel}</span>
            <Segmented label={s.hillsLabel} options={LEVELS.map((l) => ({ value: l, label: hillsName(l, s) }))} value={draft.hilliness} onChange={(hilliness) => patch({ hilliness })} />
          </div>
        )}

        <h3 className="ropt-title eyebrow">{s.avoidLabel}</h3>
        <div className="ropt-avoid">
          {AVOIDABLE.map((what) => (
            <Toggle
              key={what}
              label={avoidName(what, s)}
              description={what === 'motorways' ? s.avoidMotorwaysDesc : undefined}
              checked={draft.avoid.includes(what)}
              onChange={(on) => toggleAvoid(what, on)}
            />
          ))}
        </div>

        <h3 className="ropt-title eyebrow">{s.speedLabel}</h3>
        <Segmented
          label={s.speedLabel}
          options={TOP_SPEEDS.map((v) => ({ value: String(v), label: v === null ? s.speedNone : fmt(s.speedValue, { v }) }))}
          value={String(draft.topSpeedKmh)}
          onChange={(v) => patch({ topSpeedKmh: (v === 'null' ? null : Number(v)) as TopSpeed })}
        />
        <p className="ropt-note">{s.speedHint}</p>
        <p className="ropt-note">
          <Icon name="info" size={16} />
          {s.note}
        </p>
      </div>
    </Sheet>
  );
}

/** The button that shows the options in one line and opens the sheet. */
export function RouteOptionsButton({ onChanged }: { onChanged?: () => void }) {
  const s = useStrings(RO_S);
  const options = useRouteOptions();
  const [open, setOpen] = useState(false);
  const summary = optionsSummary(options, s);
  return (
    <>
      <button type="button" className="ropt-button" aria-label={fmt(s.openAria, { summary })} onClick={() => setOpen(true)}>
        <Icon name="settings" size={18} />
        <span className="ropt-button__label">{s.rowLabel}</span>
        {/* The inner span is the text; the outer one cuts it with an ellipsis. */}
        <span className="ropt-button__value">
          <span>{summary}</span>
        </span>
      </button>
      <RouteOptionsSheet
        open={open}
        onClose={(changed) => {
          setOpen(false);
          if (changed) onChanged?.();
        }}
      />
    </>
  );
}
