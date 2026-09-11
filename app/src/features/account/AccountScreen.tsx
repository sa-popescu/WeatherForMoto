import { CORE } from '../../i18n/core';
import { useStrings } from '../../lib/i18n';
import { Card } from '../../ui/primitives';
import type { ScreenProps } from '../types';

// Placeholder, replaced by the "Eu" feature implementation.
export default function AccountScreen({ active }: ScreenProps) {
  const s = useStrings(CORE);
  return (
    <div className="screen" hidden={!active}>
      <Card>
        <h1 className="num">{s.tabMe}</h1>
        <p className="muted">{s.comingSoon}</p>
      </Card>
    </div>
  );
}
