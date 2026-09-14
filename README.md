# WeatherForMoto

WeatherForMoto este o aplicație meteo pentru motocicliști, cu scoring de risc, rutare pre-ride și alerte personalizate (notificări push).

Stack-ul actual este:

- Frontend: `app/` — Vite + React + TypeScript, PWA (service worker Workbox), pregătit pentru Capacitor
- Backend: FastAPI + Turso, pe Google Cloud Run (doar API)
- Deploy frontend: Cloudflare (static, din `scripts/build-pages.sh`)
- URL: https://weatherformoto.bluemouse.cc (adresa run.app redirecționează aici)

Vechiul frontend dintr-un singur fișier a fost retras: `index.html` și `sw.js` din rădăcină sunt acum doar o pagină de redirecționare și un service worker care îl dezinstalează pe cel vechi (GitHub Pages publică rădăcina repo-ului). Codul vechi rămâne în istoricul git.

## Ce oferă soluția în forma curentă

### Core weather

- Agregare multi-sursă (Open-Meteo + OpenWeatherMap + MET Norway + Pirate Weather + WeatherXM)
- Ansamblu de modele: aceleași ore cerute separat de la ECMWF, ICON (DWD), GFS (NOAA), ARPEGE (Météo-France) și UKMO, fără cheie suplimentară. Valorile orare sunt trase spre media modelelor (blend-ul Open-Meteo păstrează greutate dublă), iar dispersia dintre ele dă încrederea afișată: „sunt de acord”, „diferă puțin”, „nu sunt de acord”
- Condiții curente + forecast daily + hourly
- Moto score (0-100), calculat pe server pentru fiecare oră, cu etichete IDEAL (≥85), OK (60–84), ATENȚIE (40–59), EVITĂ (<40); constantele sunt publicate la `GET /meta/scoring`
- Ploaia se punctează după șanse × intensitate (mm/h: urme, slabă, moderată, puternică), niciodată doar după procent: 70% cu 0,1 mm nu e tratat ca ploaie adevărată
- Căutare de locuri: orașe, sate, cartiere, străzi și locuri cunoscute (Photon, date OpenStreetMap, cu rezultatele din jurul locului curent primele; Open-Meteo ca rezervă), plus GPS
- Linkurile de partajare poartă și coordonatele (`?q=Nume&ll=lat,lon`), ca un loc cu nume comun să se deschidă exact unde a fost trimis
- Fereastră optimă de mers (azi/mâine)
- Date extinse: UV, presiune, vizibilitate, frost risk, temperatură estimată carosabil
- Avertizări oficiale de la Meteoalarm (ce emite ANM), afișate ca atare deasupra scorului. Se potrivesc pe poligonul sau cercul din avertizare, iar când feedul dă doar nume de zone, după numele localității

### Phase A (cont + alerting + PWA)

- Cont clasic: signup/login/logout + profil, cu verificarea adresei de email (link de confirmare)
- Preferințe avansate de alertă:
	- prag scor minim
	- rafale maxime
	- ploaie după matricea șanse × intensitate (fără praguri manuale de procent sau mm)
	- praguri min/max temperatură
	- frost risk on/off (nu mai cere precipitații)
	- quiet hours aplicate la livrare, în ora locală a locației
	- severitate: low (doar EVITĂ), medium (ATENȚIE și EVITĂ), high (tot)
- Alertele pleacă doar ca push notifications (VAPID), pe toate dispozitivele abonate; alertele pe email au fost retrase
- Emailul rămâne doar pentru cont: cod de autentificare, confirmarea adresei, resetarea parolei
- Verificare alertă manuală (`/alerts/check-now`) și dispatch batch (`/alerts/dispatch-all`)
- PWA install prompt + service worker cu acțiuni notificare (open/snooze)

### Phase 3 (route intelligence)

- Route planner cu 2-10 opriri, fiecare cu opțiunea „locația mea”
- Route weather snapshots pe waypoint-uri estimate
- Harta traseu (Leaflet)
- Hartă cu o singură bandă de timp, pe același scrubber, în două moduri:
	- **Ploaie**: radarul observat (RainViewer, ultimele 2 ore) cu fulgerele văzute de satelit (EUMETSAT MTG Lightning Imager, la 5 minute), apoi radarul extrapolat 90 de minute (ultima imagine mutată pe direcția și viteza măsurate din ultimele cadre, block matching într-un Web Worker), care după 20 de minute se estompează treptat în ploaia din modelul ICON-EU al DWD (celule de ~7 km, la rezoluția modelului, citite ca valori prin WCS de pe `maps.dwd.de`), din 30 în 30 de minute pentru primele 6 ore, apoi orar;
	- **Nori**: norii din satelit (EUMETSAT: masca de nori Meteosat pentru unde e înnorat, infraroșul MTG pentru cât de sus), extrapolați la fel, apoi prognoza de nebulozitate Open-Meteo pe o grilă;
	- totul e desenat pe un canvas aliniat la dalele hărții, în culorile radarului; ploaia în afara ICON-EU sau când DWD nu răspunde vine din grila Open-Meteo
- Saved routes per user (`/me/routes`)
- Ride logs + stats (`/me/rides/log`, `/me/rides/stats`)
- Hazard reporting geolocalizat (`/hazards`)

## Structura proiectului

```text
WeatherForMoto/
├── app/                    # Frontend: Vite + React + TypeScript, PWA (vezi mai jos)
├── backend/                # API FastAPI (vezi backend/README.md)
│   ├── main.py, weather_service.py, auth_alerts.py
│   ├── tests.py, test_scoring.py, test_auth_alerts.py, test_frontend_routes.py
│   ├── requirements.in     # dependențe directe
│   ├── requirements.txt    # toate versiunile fixate (Linux, Python 3.11)
│   └── .env.example, entrypoint.sh, migrate_to_turso.py
├── scripts/build-pages.sh  # build-ul frontend-ului pentru Cloudflare
├── .github/                # CI, deploy API, redirect GitHub Pages, Dependabot
├── Dockerfile              # imaginea API-ului (Cloud Run)
├── wrangler.jsonc          # Cloudflare static assets (dist/)
├── capacitor.config.json   # aplicația nativă (webDir: app/dist)
├── index.html, sw.js       # doar redirecționare + retragerea vechiului service worker
├── icons/, manifest.json, privacy-policy.html
└── package.json            # CLI Capacitor
```

## Rulare locală

### 1. Backend

```bash
cd backend
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# sau .venv\Scripts\activate pe Windows
pip install -r requirements.txt
cp .env.example .env
# Editează .env cu API keys și Turso credentials
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

Backend-ul va fi la `http://localhost:8000`.

### 2. Frontend (`app/`)

```bash
cd app
npm ci
npm run dev        # http://localhost:5173, /api e proxy către API-ul live
npm test           # vitest
npm run build      # typecheck + build în app/dist
```

Pe rețeaua firmei (TLS interceptat), setează înainte `NODE_EXTRA_CA_CERTS` și `npm_config_cafile` către bundle-ul de certificate. Pentru un backend local: `API_PROXY_TARGET=http://localhost:8000 npm run dev`.

Organizare: `src/lib` (API tipizat, format, scor, geo), `src/state` (sesiune, loc, vreme, setări), `src/ui` (componente de bază, iconițe, gauge, sheet), `src/features/{now,route,map,account,place}` (ecranele), `src/sw.ts` (service worker). Textele sunt în RO și EN, lângă fiecare funcționalitate (`strings.ts`).

## Variabile de mediu backend

**Obligatorii:**

- `TURSO_DATABASE_URL` (Turso database URL)
- `TURSO_AUTH_TOKEN` (Turso auth token)
- `OPENWEATHERMAP_API_KEY` (recomandat pentru calitate mai bună a datelor)

**Opționale:**

- `DEFAULT_CITY` (implicit: `Bucharest`)
- `PORT` (implicit: `8000`)
- `PIRATE_WEATHER_API_KEY`
- `WEATHERXM_API_KEY` (stații fizice WeatherXM PRO — prioritate maximă când există stație în zonă)
- `APP_BASE_URL` (URL public al aplicației, folosit în emailurile de cont)
- `API_BASE_URL` (URL public al backend-ului, pentru linkurile de confirmare a adresei; implicit URL-ul run.app)
- `MET_NORWAY_USER_AGENT` (identificator cerut de MET Norway; are o valoare implicită reală)
- `METEOALARM_FEED_URL` (feedul CAP de avertizări; implicit cel pentru România)

**Pentru funcții avansate:**

- `AUTH_CODE_TTL_MIN`
- `SESSION_TTL_DAYS`
- `ALLOW_INSECURE_AUTH_CODE`
- `PBKDF2_ITERATIONS` (implicit 600000; hash-urile vechi se actualizează la următoarea logare)
- `AUTH_CODE_PEPPER` (opțional; codurile de login se hash-uiesc cu HMAC când e setat)

**Securitate și limite** (toate au valori implicite sigure, vezi `backend/.env.example`):

- `TRUSTED_PROXY_HOPS` (implicit 1: IP-ul clientului e ultima intrare din `X-Forwarded-For`, adăugată de Google)
- `TRUST_CF_CONNECTING_IP` (implicit false; activează doar dacă tot traficul trece prin Cloudflare)
- `CODE_MAX_FAILURES`, `CODE_FAILURES_PER_EMAIL_DAY`, `PASSWORD_FAILURE_ALERT_DAY`
- `HAZARD_RATE_MAX`, `CHECK_NOW_RATE_MAX`, `DISPATCH_CONCURRENCY`, `ALERT_COOLDOWN_HOURS`

**Email de cont** (cod de autentificare, confirmarea adresei, resetarea parolei):

- SMTP: `SMTP_HOST`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASS`, `SMTP_FROM`
- sau Brevo API: `BREVO_API_KEY`

**Push notifications:**

- `VAPID_PUBLIC_KEY`
- `VAPID_PRIVATE_KEY`
- `VAPID_SUBJECT`

**Batch dispatch securizat:**

- `ALERT_DISPATCH_SECRET`

## API principal

### Meta

- `GET /health` - Status server
- `GET /meta/scoring` - Pragurile scorului, benzile de intensitate a ploii, matricea șanse × intensitate și regula de încredere între modele
- `GET /` - Redirecționează către aplicație (`APP_BASE_URL`)
- `GET /sw.js` - Service worker care îl retrage pe cel vechi de pe acest domeniu
- `GET /privacy-policy` - Politică de confidențialitate

### Weather și geocoding

- `GET /geocode?city=Cluj-Napoca` - Geocoding oraș
- `GET /weather?city=Cluj-Napoca&days=14` - Vreme după oraș
- `GET /weather?lat=46.77&lon=23.59&days=14` - Vreme după coordonate
- `GET /route?origin=Cluj-Napoca&destination=Sibiu&departure=2026-04-13T09:00&avg_speed=80` - Rută simplă
- `GET /route/multi?stops=Cluj-Napoca;Alba-Iulia;Sibiu&departure=2026-04-13T09:00&avg_speed=80` - Rută multi-oprire (2-10 opriri)

### Auth + account

- `POST /auth/request-code` - Cere cod autentificare
- `POST /auth/verify-code` - Verifică cod
- `POST /auth/signup` - Înregistrare
- `POST /auth/login` - Autentificare
- `POST /auth/logout` - Deconectare
- `GET /auth/verify-email?token=...` - Pagina de confirmare a adresei (nu modifică nimic)
- `POST /auth/verify-email?token=...` - Confirmă adresa
- `POST /me/resend-verification` - Retrimite linkul de confirmare
- `GET /me` - Profil utilizator
- `PUT /me/profile` - Actualizează profil
- `PUT /me/prefs` - Actualizează preferințe
- `DELETE /me` - Șterge cont

### Alerts și push

- `GET /push/public-key` - Cheie publică VAPID
- `POST /me/push-subscriptions` - Abonare push
- `DELETE /me/push-subscriptions` - Dezabonare push
- `POST /alerts/check-now` - Verificare alertă manuală
- `POST /alerts/dispatch-all` - Dispatch batch (secret doar în header-ul `X-Dispatch-Secret`)

### Route & ride data

- `GET /me/routes` - Rute salvate
- `POST /me/routes` - Salvează rută
- `DELETE /me/routes/{route_id}` - Șterge rută
- `POST /me/rides/log` - Log călătorie
- `GET /me/rides/stats` - Statistici călătorii
- `POST /hazards` - Raportează hazard
- `GET /hazards?lat=...&lon=...&radius_km=120` - Lista hazarduri

## Testare

```bash
cd backend
python tests.py                       # agregare și logica meteo de bază
python -m unittest test_scoring       # scor v2, matricea de ploaie, cache, buget de timp
python -m unittest test_auth_alerts   # autentificare, limite, alerte (SQLite în locul libsql)
python -m unittest test_route_limits  # limitele endpointului /route/multi
python -m unittest test_meteoalarm    # parsarea CAP și potrivirea pe zonă
```

Testele nu depind de rețea. Pe Windows, `tests.py` are nevoie de `PYTHONIOENCODING=utf-8` ca să poată afișa simbolurile din output.

## Deploy

### CI

`.github/workflows/ci.yml` rulează la fiecare pull request și pe `main`: testele backend (Python 3.11), typecheck + build + testele frontend (`app/`) și build-ul imaginii Docker. Dependabot propune săptămânal actualizări (pip, npm, imaginea Docker, acțiunile GitHub), fiecare trecând prin același CI.

### Google Cloud Run (API)

Deploy-ul e automat: la fiecare push pe `main` care atinge `backend/`, `Dockerfile` sau `.dockerignore`, workflow-ul `.github/workflows/deploy-backend.yml` rulează CI-ul, construiește imaginea în GitHub Actions, o urcă în Artifact Registry (`europe-west1-docker.pkg.dev/weatherformoto/weatherformoto/api`) și o publică pe Cloud Run.

Autentificarea e fără chei: Workload Identity Federation (pool `github`, provider `github-actions`), permisă doar pentru ramura `main` a acestui repo, cu contul `github-deployer@weatherformoto.iam.gserviceaccount.com` (roluri minime: `run.developer`, scriere în Artifact Registry, act-as pe contul de rulare).

**Secrete:** valorile sensibile (token Turso, cheile furnizorilor meteo, parola SMTP, cheia Brevo, cheia privată VAPID, secretele Netatmo, secretul de dispatch) stau în Secret Manager ca `mm-*` și ajung în serviciu ca variabile de mediu; restul sunt variabile obișnuite. Un deploy nou le păstrează pe toate.

**Migrare schemă:** rulează manual workflow-ul „Deploy API” cu opțiunea `run_migrations`. Noua revizie pornește fără trafic, rulează migrarea la startup și abia apoi primește traficul. Codul funcționează și pe o bază nemigrată (vechiul comportament).

**Alerte:** job-ul Cloud Scheduler `mm-dispatch-alerts` apelează `POST /alerts/dispatch-all` în fiecare oră, la minutul 5 (ora României), cu header-ul `X-Dispatch-Secret`. Pornire / oprire:

```bash
gcloud scheduler jobs resume mm-dispatch-alerts --location europe-west1 --project weatherformoto
gcloud scheduler jobs pause mm-dispatch-alerts --location europe-west1 --project weatherformoto
```

Deploy manual, dacă GitHub Actions nu e disponibil (păstrează variabilele și secretele serviciului):

```bash
gcloud run deploy weatherformoto --source . --region europe-west1 --project weatherformoto
```

### Frontend static pe Cloudflare Pages (zero-cost)

Frontend-ul static e servit de la edge prin Cloudflare (instant, fără cold start), iar backend-ul FastAPI rămâne pe Cloud Run doar pentru API. Build-ul (`sh scripts/build-pages.sh`) rulează `npm ci` și `npm run build` în `app/` și copiază rezultatul în `dist/`. Aplicația cheamă API-ul la URL-ul Cloud Run, iar CORS-ul backend-ului permite originea aplicației. Pe Cloud Run, `/` redirecționează către `APP_BASE_URL`, iar `/sw.js` retrage service worker-ul vechi. GitHub Pages servește doar o pagină de redirecționare.

Setup în dashboard-ul Cloudflare Pages (Connect to Git):

- Build command: `sh scripts/build-pages.sh`
- Build output directory: `dist`
- Custom domain: `weatherformoto.bluemouse.cc` (mută CNAME-ul de pe vechiul Worker pe proiectul Pages)

Pe backend (Cloud Run) setează `ALLOWED_ORIGINS` ca să includă originea frontend-ului:

```bash
gcloud run services update weatherformoto --region europe-west1 \
  --set-env-vars ALLOWED_ORIGINS=https://weatherformoto.bluemouse.cc
```

Domeniile `*.weatherformoto.pages.dev` (preview-uri Pages) sunt permise automat de backend.

> Cheia OpenWeatherMap nu mai este expusă în client. Este folosită doar server-side de backend; calea browser-direct (fallback când backend-ul nu răspunde) rulează fără ea, pe Open-Meteo.

### Docker local

```bash
docker build -t weatherformoto .
docker run --rm -p 8000:8000 --env-file backend/.env weatherformoto
```

## Observații practice

- Pentru push notifications reale, trebuie configurat VAPID pe backend
- Emailurile de cont au nevoie de SMTP sau Brevo; dacă contul Brevo are restricție de IP („Authorized IPs”), trebuie să accepte ieșirea Cloud Run, altfel Brevo răspunde 401
- Directorul `www/` conține asset-uri statice pentru PWA, copiate automat de Capacitor
