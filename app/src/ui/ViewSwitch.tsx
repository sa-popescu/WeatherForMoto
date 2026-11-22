// Two or more views of one tab, as links (so each view keeps its own URL and
// works as a deep link), drawn as a segmented control. Used by "Vreme" for
// the forecast and the radar map.

export interface ViewSwitchItem {
  href: string;
  label: string;
  current: boolean;
}

export function ViewSwitch({ label, items }: { label: string; items: readonly ViewSwitchItem[] }) {
  return (
    <nav className="view-switch" aria-label={label}>
      {items.map((item) => (
        <a key={item.href} href={item.href} className="view-switch__item" aria-current={item.current ? 'page' : undefined}>
          {item.label}
        </a>
      ))}
    </nav>
  );
}
