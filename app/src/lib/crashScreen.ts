// What is left on screen when React gives up on the whole app (an error
// while drawing it). Plain DOM, since React has just unmounted everything:
// a short note and a reload button, instead of an empty screen.

const TEXT = {
  ro: {
    title: 'Ceva n-a mers',
    body: 'Aplicația a dat de o eroare. Reîncarcă și încearcă din nou.',
    reload: 'Reîncarcă',
  },
  en: {
    title: 'Something went wrong',
    body: 'The app ran into an error. Reload and try again.',
    reload: 'Reload',
  },
} as const;

export function showCrashScreen(): void {
  if (document.querySelector('.crash')) return;
  const text = document.documentElement.lang === 'en' ? TEXT.en : TEXT.ro;

  const screen = document.createElement('div');
  screen.className = 'crash';
  screen.setAttribute('role', 'alert');

  const title = document.createElement('h1');
  title.className = 'crash__title';
  title.textContent = text.title;

  const body = document.createElement('p');
  body.className = 'crash__body';
  body.textContent = text.body;

  const reload = document.createElement('button');
  reload.type = 'button';
  reload.className = 'btn btn--primary btn--lg';
  reload.textContent = text.reload;
  reload.addEventListener('click', () => window.location.reload());

  screen.append(title, body, reload);
  document.body.append(screen);
  reload.focus();
}
