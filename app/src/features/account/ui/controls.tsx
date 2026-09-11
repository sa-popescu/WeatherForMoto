import { useId, useRef, type CSSProperties, type KeyboardEvent, type ReactNode } from 'react';
import { Icon, type IconName } from '../../../ui/icons';
import { cx, SectionLabel, Segmented } from '../../../ui/primitives';

// Small controls shared by the account sections.

/** Titled block of cards; the eyebrow heading names the region for screen readers. */
export function Group({ title, children }: { title: string; children: ReactNode }) {
  const id = useId();
  return (
    <section className="acct-group" aria-labelledby={id}>
      <SectionLabel id={id}>{title}</SectionLabel>
      {children}
    </section>
  );
}

interface RangeFieldProps {
  label: string;
  value: number;
  min: number;
  max: number;
  step: number;
  /** Text shown next to the label and read by screen readers. */
  display: string;
  onChange: (value: number) => void;
  disabled?: boolean;
  /** Keeps the label for screen readers only (when a toggle above already names it). */
  hideLabel?: boolean;
}

export function RangeField({ label, value, min, max, step, display, onChange, disabled, hideLabel }: RangeFieldProps) {
  const id = useId();
  const clamped = Math.min(max, Math.max(min, value));
  const fill = ((clamped - min) / (max - min)) * 100;
  const style = { '--acct-fill': `${fill}%` } as CSSProperties;
  return (
    <div className={cx('acct-range', disabled && 'acct-range--disabled')}>
      <div className="acct-range__head">
        <label htmlFor={id} className={cx('acct-range__label', hideLabel && 'visually-hidden')}>
          {label}
        </label>
        <output htmlFor={id} className="acct-range__value num">
          {display}
        </output>
      </div>
      <input
        id={id}
        type="range"
        className="acct-range__input"
        min={min}
        max={max}
        step={step}
        value={clamped}
        disabled={disabled}
        aria-valuetext={display}
        style={style}
        onChange={(e) => onChange(Number(e.target.value))}
      />
    </div>
  );
}

interface Choice<T extends string> {
  value: T;
  label: string;
  description?: string;
}

interface ChoicesProps<T extends string> {
  label: string;
  options: ReadonlyArray<Choice<T>>;
  value: T;
  onChange: (value: T) => void;
  variant?: 'rows' | 'chips';
}

/** Radio group with roving focus: Tab enters once, arrow keys move and select. */
export function Choices<T extends string>({ label, options, value, onChange, variant = 'rows' }: ChoicesProps<T>) {
  const refs = useRef<Array<HTMLButtonElement | null>>([]);
  const onKey = (event: KeyboardEvent<HTMLButtonElement>, index: number): void => {
    const forward = event.key === 'ArrowDown' || event.key === 'ArrowRight';
    const back = event.key === 'ArrowUp' || event.key === 'ArrowLeft';
    if (!forward && !back) return;
    event.preventDefault();
    const next = (index + (forward ? 1 : -1) + options.length) % options.length;
    onChange(options[next].value);
    refs.current[next]?.focus();
  };
  return (
    <div className={cx('acct-choices', `acct-choices--${variant}`)} role="radiogroup" aria-label={label}>
      {options.map((o, i) => {
        const on = o.value === value;
        return (
          <button
            key={o.value}
            ref={(el) => {
              refs.current[i] = el;
            }}
            type="button"
            role="radio"
            aria-checked={on}
            tabIndex={on ? 0 : -1}
            className={cx('acct-choice', on && 'acct-choice--on')}
            onClick={() => onChange(o.value)}
            onKeyDown={(e) => onKey(e, i)}
          >
            {variant === 'rows' && <span className="acct-choice__dot" aria-hidden="true" />}
            <span className="acct-choice__text">
              <span className="acct-choice__label">{o.label}</span>
              {o.description && <span className="acct-choice__desc">{o.description}</span>}
            </span>
          </button>
        );
      })}
    </div>
  );
}

interface LabeledSegmentedProps<T extends string> {
  label: string;
  options: ReadonlyArray<{ value: T; label: string }>;
  value: T;
  onChange: (value: T) => void;
}

/** Segmented control with a visible caption (the group itself carries the same aria-label). */
export function LabeledSegmented<T extends string>(props: LabeledSegmentedProps<T>) {
  return (
    <div className="acct-seg-field">
      <span className="acct-seg-field__label" aria-hidden="true">
        {props.label}
      </span>
      <Segmented {...props} />
    </div>
  );
}

interface RowButtonProps {
  label: string;
  value?: string;
  icon?: IconName;
  onClick: () => void;
  tone?: 'default' | 'danger';
  busy?: boolean;
}

/** 56px list row that opens something. */
export function RowButton({ label, value, icon, onClick, tone = 'default', busy }: RowButtonProps) {
  return (
    <button
      type="button"
      className={cx('acct-row', tone === 'danger' && 'acct-row--danger')}
      onClick={onClick}
      disabled={busy}
      aria-busy={busy || undefined}
    >
      {icon && <Icon name={icon} size={22} />}
      <span className="acct-row__label">{label}</span>
      {value && <span className="acct-row__value">{value}</span>}
      <Icon name="chevronRight" size={20} className="acct-row__chevron" />
    </button>
  );
}

export function Note({ icon = 'info', children }: { icon?: IconName; children: ReactNode }) {
  return (
    <p className="acct-note">
      <Icon name={icon} size={18} />
      <span>{children}</span>
    </p>
  );
}
