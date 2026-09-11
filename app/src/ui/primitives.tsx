import { useId, type ButtonHTMLAttributes, type HTMLAttributes, type InputHTMLAttributes, type ReactNode } from 'react';
import { Icon, type IconName } from './icons';

// Shared building blocks. Every tappable thing is at least 48px (56px for
// primary actions) so it works with riding gloves.

export function cx(...classes: Array<string | false | null | undefined>): string {
  return classes.filter(Boolean).join(' ');
}

type ButtonVariant = 'primary' | 'secondary' | 'ghost' | 'danger';

interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: ButtonVariant;
  size?: 'md' | 'lg';
  icon?: IconName;
  full?: boolean;
  busy?: boolean;
}

export function Button({ variant = 'secondary', size = 'md', icon, full, busy, children, className, disabled, type = 'button', ...rest }: ButtonProps) {
  return (
    <button
      type={type}
      className={cx('btn', `btn--${variant}`, size === 'lg' && 'btn--lg', full && 'btn--full', className)}
      disabled={disabled || busy}
      aria-busy={busy || undefined}
      {...rest}
    >
      {busy ? <Spinner size={18} /> : icon ? <Icon name={icon} size={20} /> : null}
      {children != null && <span>{children}</span>}
    </button>
  );
}

interface IconButtonProps extends Omit<ButtonHTMLAttributes<HTMLButtonElement>, 'children'> {
  icon: IconName;
  label: string;
  tone?: 'default' | 'accent';
}

export function IconButton({ icon, label, tone = 'default', className, type = 'button', ...rest }: IconButtonProps) {
  return (
    <button type={type} className={cx('icon-btn', tone === 'accent' && 'icon-btn--accent', className)} aria-label={label} title={label} {...rest}>
      <Icon name={icon} size={22} />
    </button>
  );
}

export function Card({ className, children, ...rest }: HTMLAttributes<HTMLElement>) {
  return (
    <section className={cx('card', className)} {...rest}>
      {children}
    </section>
  );
}

export function SectionLabel({ children, action, id }: { children: ReactNode; action?: ReactNode; id?: string }) {
  return (
    <div className="section-label">
      <h2 className="eyebrow" id={id}>
        {children}
      </h2>
      {action}
    </div>
  );
}

interface ChipProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  selected?: boolean;
}

export function Chip({ selected, className, children, type = 'button', ...rest }: ChipProps) {
  return (
    <button type={type} className={cx('chip', selected && 'chip--selected', className)} aria-pressed={selected} {...rest}>
      {children}
    </button>
  );
}

interface ToggleProps {
  checked: boolean;
  onChange: (checked: boolean) => void;
  label: ReactNode;
  description?: ReactNode;
  disabled?: boolean;
  id?: string;
}

export function Toggle({ checked, onChange, label, description, disabled, id }: ToggleProps) {
  const autoId = useId();
  const labelId = `${id ?? autoId}-label`;
  return (
    <div className={cx('toggle-row', disabled && 'toggle-row--disabled')}>
      <div className="toggle-row__text">
        <span id={labelId} className="toggle-row__label">
          {label}
        </span>
        {description && <span className="toggle-row__desc">{description}</span>}
      </div>
      <button
        type="button"
        id={id ?? autoId}
        role="switch"
        aria-checked={checked}
        aria-labelledby={labelId}
        className="switch"
        disabled={disabled}
        onClick={() => onChange(!checked)}
      >
        <span className="switch__thumb" />
      </button>
    </div>
  );
}

interface SegmentedProps<T extends string> {
  label: string;
  options: ReadonlyArray<{ value: T; label: string }>;
  value: T;
  onChange: (value: T) => void;
}

export function Segmented<T extends string>({ label, options, value, onChange }: SegmentedProps<T>) {
  return (
    <div className="segmented" role="radiogroup" aria-label={label}>
      {options.map((o) => (
        <button
          key={o.value}
          type="button"
          role="radio"
          aria-checked={o.value === value}
          className={cx('segmented__item', o.value === value && 'segmented__item--on')}
          onClick={() => onChange(o.value)}
        >
          {o.label}
        </button>
      ))}
    </div>
  );
}

interface TextFieldProps extends Omit<InputHTMLAttributes<HTMLInputElement>, 'onChange'> {
  label: string;
  value: string;
  onChange: (value: string) => void;
  hint?: string;
  error?: string | null;
}

export function TextField({ label, value, onChange, hint, error, id, className, ...rest }: TextFieldProps) {
  const autoId = useId();
  const inputId = id ?? autoId;
  const hintId = `${inputId}-hint`;
  return (
    <div className={cx('field', error && 'field--error', className)}>
      <label className="field__label" htmlFor={inputId}>
        {label}
      </label>
      <input
        id={inputId}
        className="field__input"
        value={value}
        onChange={(e) => onChange(e.target.value)}
        aria-invalid={error ? true : undefined}
        aria-describedby={hint || error ? hintId : undefined}
        {...rest}
      />
      {(error || hint) && (
        <span id={hintId} className={cx('field__hint', error && 'field__hint--error')}>
          {error ?? hint}
        </span>
      )}
    </div>
  );
}

export function Banner({ tone = 'info', icon, children, action }: { tone?: 'info' | 'warn' | 'error'; icon?: IconName; children: ReactNode; action?: ReactNode }) {
  return (
    <div className={cx('banner', `banner--${tone}`)} role={tone === 'error' ? 'alert' : undefined}>
      {icon && <Icon name={icon} size={22} />}
      <div className="banner__text">{children}</div>
      {action}
    </div>
  );
}

export function Spinner({ size = 22 }: { size?: number }) {
  return <span className="spinner" style={{ width: size, height: size }} aria-hidden="true" />;
}

export function Skeleton({ height = 16, width = '100%', radius = 10 }: { height?: number | string; width?: number | string; radius?: number }) {
  return <span className="skeleton" style={{ height, width, borderRadius: radius }} aria-hidden="true" />;
}
