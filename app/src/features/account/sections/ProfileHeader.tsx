import { useState } from 'react';
import { CORE } from '../../../i18n/core';
import { api } from '../../../lib/api';
import { fmt, useLang, useStrings, type Lang } from '../../../lib/i18n';
import type { MeResponse } from '../../../lib/types';
import { useAuth } from '../../../state/auth';
import { useToast } from '../../../state/toast';
import { Icon } from '../../../ui/icons';
import { Banner, Button, Card, cx } from '../../../ui/primitives';
import { useErrorText } from '../hooks/useErrorText';
import { isSessionError } from '../lib/errors';
import { PROFILE } from '../strings/profile';

function memberSince(createdAt: string | null, lang: Lang): string | null {
  if (!createdAt) return null;
  const date = new Date(createdAt);
  if (Number.isNaN(date.getTime())) return null;
  return date.toLocaleDateString(lang === 'en' ? 'en-GB' : 'ro-RO', { month: 'long', year: 'numeric' });
}

function reportsVerified(res: unknown): boolean {
  return typeof res === 'object' && res !== null && (res as { email_verified?: unknown }).email_verified === true;
}

export function ProfileHeader({ me }: { me: MeResponse }) {
  const s = useStrings(PROFILE);
  const lang = useLang();
  const { token, refreshMe, handleAuthError } = useAuth();
  const toast = useToast();
  const errorText = useErrorText();
  const [busy, setBusy] = useState(false);

  const name = me.display_name.trim() || me.email.split('@')[0];
  const since = memberSince(me.created_at, lang);

  const resend = async (): Promise<void> => {
    if (!token) return;
    setBusy(true);
    try {
      const res = await api.resendVerification(token);
      if (reportsVerified(res)) {
        toast(s.resendAlready);
        await refreshMe();
      } else {
        toast(fmt(s.resendDone, { email: me.email }), { tone: 'success' });
      }
    } catch (err) {
      console.warn('[account] resend verification failed', err);
      toast(errorText(err, 'other'), { tone: 'error' });
      if (isSessionError(err, 'other')) handleAuthError(err);
    } finally {
      setBusy(false);
    }
  };

  return (
    <Card className="acct-profile">
      <div className="acct-profile__row">
        <span className="acct-avatar num" aria-hidden="true">
          {name.charAt(0).toLocaleUpperCase()}
        </span>
        <div className="acct-profile__text">
          <h2 className="acct-profile__name num">{name}</h2>
          <span className="acct-profile__email">{me.email}</span>
          <span className={cx('acct-badge', me.email_verified ? 'acct-badge--ok' : 'acct-badge--warn')}>
            <Icon name={me.email_verified ? 'check' : 'alert'} size={15} strokeWidth={2.2} />
            {me.email_verified ? s.verified : s.unverified}
          </span>
        </div>
      </div>
      {since && <p className="acct-profile__since">{fmt(s.memberSince, { date: since })}</p>}
      {!me.email_verified && (
        <>
          <Banner tone="warn" icon="mail">
            {s.unverifiedNote}
          </Banner>
          <Button full icon="refresh" busy={busy} onClick={() => void resend()}>
            {s.resend}
          </Button>
        </>
      )}
    </Card>
  );
}

export function LoadErrorCard() {
  const s = useStrings(PROFILE);
  const core = useStrings(CORE);
  const { refreshMe } = useAuth();
  const [busy, setBusy] = useState(false);
  const retry = async (): Promise<void> => {
    setBusy(true);
    try {
      await refreshMe();
    } finally {
      setBusy(false);
    }
  };
  return (
    <Banner
      tone="error"
      icon="alert"
      action={
        <Button busy={busy} onClick={() => void retry()}>
          {core.retry}
        </Button>
      }
    >
      {s.loadError}
    </Banner>
  );
}
