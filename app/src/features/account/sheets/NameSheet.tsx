import { useEffect, useId, useState, type FormEvent } from 'react';
import { CORE } from '../../../i18n/core';
import { api } from '../../../lib/api';
import { useStrings } from '../../../lib/i18n';
import type { MeResponse } from '../../../lib/types';
import { useAuth } from '../../../state/auth';
import { useToast } from '../../../state/toast';
import { Banner, Button, TextField } from '../../../ui/primitives';
import { Sheet } from '../../../ui/Sheet';
import { useKindText } from '../hooks/useErrorText';
import { useSubmit } from '../hooks/useSubmit';
import { cleanDisplayName, NAME_MAX } from '../lib/validation';
import { PROFILE } from '../strings/profile';

export interface AccountSheetProps {
  open: boolean;
  me: MeResponse;
  onClose: () => void;
}

export function NameSheet({ open, me, onClose }: AccountSheetProps) {
  const s = useStrings(PROFILE);
  const core = useStrings(CORE);
  const { token, refreshMe } = useAuth();
  const toast = useToast();
  const kindText = useKindText();
  const { busy, error, setError, run } = useSubmit('other');
  const formId = useId();
  const [name, setName] = useState(me.display_name);
  const [checked, setChecked] = useState(false);

  useEffect(() => {
    if (!open) return;
    setName(me.display_name);
    setChecked(false);
    setError(null);
  }, [open, me.display_name, setError]);

  const clean = cleanDisplayName(name);

  const submit = (event: FormEvent): void => {
    event.preventDefault();
    setChecked(true);
    if (!clean || !token) return;
    void run(async () => {
      await api.updateProfile(token, clean);
      await refreshMe();
      toast(s.nameSaved, { tone: 'success' });
      onClose();
    });
  };

  const footer = (
    <>
      <Button onClick={onClose}>{core.cancel}</Button>
      <Button type="submit" form={formId} variant="primary" busy={busy}>
        {core.save}
      </Button>
    </>
  );

  return (
    <Sheet open={open} onClose={onClose} title={s.nameTitle} footer={footer}>
      <form id={formId} className="acct-form" onSubmit={submit} noValidate>
        <TextField
          label={s.nameRow}
          value={name}
          onChange={setName}
          maxLength={NAME_MAX}
          autoComplete="nickname"
          error={checked && !clean ? s.nameEmpty : null}
        />
        {error && (
          <Banner tone="error" icon="alert">
            {kindText(error)}
          </Banner>
        )}
      </form>
    </Sheet>
  );
}
