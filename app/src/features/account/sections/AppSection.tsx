import { lazy, Suspense, useState } from 'react';
import { PRIVACY_POLICY_URL } from '../../../lib/config';
import { useStrings, type Lang } from '../../../lib/i18n';
import { useSettings, type Theme } from '../../../state/settings';
import { Icon } from '../../../ui/icons';
import { Button, Card, Toggle } from '../../../ui/primitives';
import { useInstallState } from '../installPrompt';
import { APP } from '../strings/app';
import { Group, LabeledSegmented, Note } from '../ui/controls';
import { OfflineMapsRow } from './OfflineMapsRow';
import { APP_VERSION } from '../../../lib/appVersion';

const InstallGuide = lazy(() => import('../../install/InstallGuide'));

const LANGUAGES: ReadonlyArray<{ value: Lang; label: string }> = [
  { value: 'ro', label: 'Română' },
  { value: 'en', label: 'English' },
];

export function AppSection() {
  const s = useStrings(APP);
  const { lang, setLang, theme, setTheme, followMe, setFollowMe } = useSettings();
  const install = useInstallState();
  const [guide, setGuide] = useState(false);

  const themes: ReadonlyArray<{ value: Theme; label: string }> = [
    { value: 'dark', label: s.themeDark },
    { value: 'day', label: s.themeDay },
  ];

  return (
    <Group title={s.appSection}>
      <Card className="acct-stack">
        <LabeledSegmented label={s.language} options={LANGUAGES} value={lang} onChange={setLang} />
        <LabeledSegmented label={s.theme} options={themes} value={theme} onChange={setTheme} />
        <p className="acct-hint">{s.themeNote}</p>
        <div className="acct-divider" />
        <Toggle checked={followMe} onChange={setFollowMe} label={s.followMe} description={s.followMeNote} />
        <div className="acct-divider" />
        {install === 'installed' ? (
          <Note icon="check">{s.installed}</Note>
        ) : (
          <>
            {/* The same guide as the first-visit invitation (features/install). */}
            <Button full icon="download" onClick={() => setGuide(true)}>
              {s.install}
            </Button>
            <p className="acct-hint">{s.installNote}</p>
          </>
        )}
      </Card>
      {guide && (
        <Suspense fallback={null}>
          <InstallGuide open onClose={() => setGuide(false)} />
        </Suspense>
      )}
      <Card className="acct-rows">
        {/* Same window, not a new one: the installed app on an iPhone has no
            windows, and the policy page links back here instead. */}
        <a className="acct-row" href={PRIVACY_POLICY_URL[lang]}>
          <Icon name="lock" size={22} />
          <span className="acct-row__label">{s.privacy}</span>
          <Icon name="chevronRight" size={20} className="acct-row__chevron" />
        </a>
        <OfflineMapsRow />
        <div className="acct-row acct-row--static">
          <Icon name="info" size={22} />
          <span className="acct-row__label">{s.version}</span>
          <span className="acct-row__value num">{APP_VERSION}</span>
        </div>
      </Card>
    </Group>
  );
}
