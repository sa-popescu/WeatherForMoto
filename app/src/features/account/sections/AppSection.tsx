import { CORE } from '../../../i18n/core';
import { PRIVACY_POLICY_URL } from '../../../lib/config';
import { useStrings, type Lang } from '../../../lib/i18n';
import { useSettings, type Theme } from '../../../state/settings';
import { useToast } from '../../../state/toast';
import { Icon } from '../../../ui/icons';
import { Button, Card } from '../../../ui/primitives';
import { promptInstall, useInstallState } from '../installPrompt';
import { currentIsIos } from '../lib/platform';
import { APP } from '../strings/app';
import { Group, LabeledSegmented, Note } from '../ui/controls';
import { version as APP_VERSION } from '../../../../package.json';

const LANGUAGES: ReadonlyArray<{ value: Lang; label: string }> = [
  { value: 'ro', label: 'Română' },
  { value: 'en', label: 'English' },
];

export function AppSection() {
  const s = useStrings(APP);
  const core = useStrings(CORE);
  const { lang, setLang, theme, setTheme } = useSettings();
  const install = useInstallState();
  const toast = useToast();

  const themes: ReadonlyArray<{ value: Theme; label: string }> = [
    { value: 'dark', label: s.themeDark },
    { value: 'day', label: s.themeDay },
  ];

  const onInstall = async (): Promise<void> => {
    try {
      if ((await promptInstall()) === 'accepted') toast(s.installDone, { tone: 'success' });
    } catch (err) {
      console.warn('[account] install prompt failed', err);
      toast(core.genericError, { tone: 'error' });
    }
  };

  return (
    <Group title={s.appSection}>
      <Card className="acct-stack">
        <LabeledSegmented label={s.language} options={LANGUAGES} value={lang} onChange={setLang} />
        <LabeledSegmented label={s.theme} options={themes} value={theme} onChange={setTheme} />
        <p className="acct-hint">{s.themeNote}</p>
        <div className="acct-divider" />
        {install === 'installed' && <Note icon="check">{s.installed}</Note>}
        {install === 'available' && (
          <>
            <Button full icon="download" onClick={() => void onInstall()}>
              {s.install}
            </Button>
            <p className="acct-hint">{s.installNote}</p>
          </>
        )}
        {install === 'unavailable' && <Note icon="download">{currentIsIos() ? s.installIos : s.installOther}</Note>}
      </Card>
      <Card className="acct-rows">
        <a className="acct-row" href={PRIVACY_POLICY_URL} target="_blank" rel="noopener noreferrer">
          <Icon name="lock" size={22} />
          <span className="acct-row__label">{s.privacy}</span>
          <Icon name="external" size={20} className="acct-row__chevron" />
        </a>
        <div className="acct-row acct-row--static">
          <Icon name="info" size={22} />
          <span className="acct-row__label">{s.version}</span>
          <span className="acct-row__value num">{APP_VERSION}</span>
        </div>
      </Card>
    </Group>
  );
}
