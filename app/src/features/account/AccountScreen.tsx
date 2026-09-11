import './account.css';
import { CORE } from '../../i18n/core';
import { useStrings } from '../../lib/i18n';
import { useAuth } from '../../state/auth';
import { Skeleton } from '../../ui/primitives';
import type { ScreenProps } from '../types';
import { AuthCard } from './auth/AuthCard';
import { PrefsEditorProvider } from './hooks/prefsEditor';
import { AccountSection } from './sections/AccountSection';
import { AlertsSection } from './sections/AlertsSection';
import { AppSection } from './sections/AppSection';
import { FavoritesSection } from './sections/FavoritesSection';
import { MotoSection } from './sections/MotoSection';
import { NotificationsSection } from './sections/NotificationsSection';
import { LoadErrorCard, ProfileHeader } from './sections/ProfileHeader';
import { ScoreInfo } from './sections/ScoreInfo';
import { AUTH } from './strings/auth';

// "Eu" tab: account, alerts and app settings. Device-level sections (moto,
// favourites, app, score) work signed out too.

export default function AccountScreen({ active }: ScreenProps) {
  const s = useStrings(AUTH);
  const { status } = useAuth();
  return (
    <div className="screen acct" hidden={!active}>
      <header className="acct-head">
        <h1 className="acct-head__title num">{s.title}</h1>
        <p className="acct-head__sub">{s.subtitle}</p>
      </header>
      <PrefsEditorProvider>
        {status === 'anonymous' && <AuthCard />}
        {status === 'loading' && <ProfileSkeleton />}
        {status === 'signed-in' && <SignedIn active={active} />}
        <MotoSection />
        <FavoritesSection />
        <AppSection />
        <ScoreInfo />
        {status === 'signed-in' && <AccountSection />}
      </PrefsEditorProvider>
    </div>
  );
}

function SignedIn({ active }: { active: boolean }) {
  const { me, loadError } = useAuth();
  if (!me) return loadError ? <LoadErrorCard /> : <ProfileSkeleton />;
  return (
    <>
      {loadError && <LoadErrorCard />}
      <ProfileHeader me={me} />
      <NotificationsSection me={me} active={active} />
      <AlertsSection verified={me.email_verified} />
    </>
  );
}

function ProfileSkeleton() {
  const core = useStrings(CORE);
  return (
    <div className="card acct-profile" aria-busy="true">
      <span className="visually-hidden">{core.loading}</span>
      <div className="acct-profile__row">
        <Skeleton width={56} height={56} radius={28} />
        <div className="acct-profile__text acct-grow">
          <Skeleton height={20} width="60%" />
          <Skeleton height={14} width="85%" />
        </div>
      </div>
    </div>
  );
}
