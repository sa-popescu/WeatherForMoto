import { useEffect, useId, useState, type FormEvent } from 'react';
import { CORE } from '../../../i18n/core';
import { fmt, useLang, useStrings } from '../../../lib/i18n';
import { useAuth } from '../../../state/auth';
import { useToast } from '../../../state/toast';
import { Banner, Button, TextField } from '../../../ui/primitives';
import { Sheet } from '../../../ui/Sheet';
import { useKindText } from '../hooks/useErrorText';
import { useSubmit } from '../hooks/useSubmit';
import { DELETE_WORD, deleteConfirmMatches } from '../lib/validation';
import { PROFILE } from '../strings/profile';

/** Typing the word is the confirmation; no confirm() dialog. */
export function DeleteSheet({ open, onClose }: { open: boolean; onClose: () => void }) {
  const s = useStrings(PROFILE);
  const core = useStrings(CORE);
  const lang = useLang();
  const { deleteAccount } = useAuth();
  const toast = useToast();
  const kindText = useKindText();
  const { busy, error, setError, run } = useSubmit('other');
  const formId = useId();
  const [typed, setTyped] = useState('');

  useEffect(() => {
    if (!open) return;
    setTyped('');
    setError(null);
  }, [open, setError]);

  const word = DELETE_WORD[lang];
  const confirmed = deleteConfirmMatches(typed, lang);

  const submit = (event: FormEvent): void => {
    event.preventDefault();
    if (!confirmed) return;
    // On success the signed-in view (and this sheet) unmounts.
    void run(async () => {
      await deleteAccount();
      toast(s.deleteDone, { tone: 'success' });
    });
  };

  const footer = (
    <>
      <Button onClick={onClose}>{core.cancel}</Button>
      <Button type="submit" form={formId} variant="danger" icon="trash" disabled={!confirmed} busy={busy}>
        {s.deleteConfirm}
      </Button>
    </>
  );

  return (
    <Sheet open={open} onClose={onClose} title={s.deleteTitle} footer={footer}>
      <form id={formId} className="acct-form" onSubmit={submit} noValidate>
        <Banner tone="warn" icon="alert">
          {s.deleteLead}
        </Banner>
        <p className="acct-hint">{s.deleteLocal}</p>
        <TextField
          label={fmt(s.deleteType, { word })}
          value={typed}
          onChange={setTyped}
          autoComplete="off"
          autoCapitalize="characters"
          autoCorrect="off"
          spellCheck={false}
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
