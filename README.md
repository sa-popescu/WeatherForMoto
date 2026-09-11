# WeatherForMoto

WeatherForMoto este o aplicație meteo pentru motocicliști, cu scoring de risc, recomandări de echipament, rutare pre-ride și alerte personalizate (push/email).

Stack-ul actual este:

- Frontend: HTML + Tailwind CSS + JavaScript (PWA)
- Backend: FastAPI + Turso
- PWA: Service Worker + Web App Manifest
- Deploy: Docker/Google Cloud Run
- URL: https://weatherformoto.bluemouse.cc

## Ce oferă soluția în forma curentă

### Core weather

- Agregare multi-sursă (Open-Meteo + OpenWeatherMap + MET Norway + Pirate Weather + WeatherXM)
- Condiții curente + forecast daily + hourly
- Moto score (0-100), calculat pe server pentru fiecare oră, cu etichete IDEAL (≥85), OK (60–84), ATENȚIE (40–59), EVITĂ (<40); constantele sunt publicate la `GET /meta/scoring`
- Ploaia se punctează după șanse × intensitate (mm/h: urme, slabă, moderată, puternică), niciodată doar după procent: 70% cu 0,1 mm nu e tratat ca ploaie adevărată
- Geo lookup după oraș sau coordonate
- Fereastră optimă de mers (azi/mâine)
- Recomandări de echipament în funcție de ploaie/vânt/temperatură
- Date extinse: UV, presiune, vizibilitate, frost risk, temperatură estimată carosabil

### Phase A (cont + alerting + PWA)

- Cont clasic: signup/login/logout + profil, cu verificarea adresei de email (link de confirmare; alertele pe email pleacă doar către adrese confirmate)
- Preferințe avansate de alertă:
	- prag scor minim
	- rafale maxime
	- ploaie după matricea șanse × intensitate (fără praguri manuale de procent sau mm)
	- praguri min/max temperatură
	- frost risk on/off (nu mai cere precipitații)
	- quiet hours aplicate la livrare, în ora locală a locației
	- severitate: low (doar EVITĂ), medium (ATENȚIE și EVITĂ), high (tot)
- Dezabonare de la emailuri cu confirmare (RFC 8058, `List-Unsubscribe`)
- Push notifications (VAPID) cu fallback email
- Verificare alertă manuală (`/alerts/check-now`) și dispatch batch (`/alerts/dispatch-all`)
- PWA install prompt + service worker cu acțiuni notificare (open/snooze)

### Phase 3 (route intelligence)

- Route planner cu 2-5 opriri
- Route weather snapshots pe waypoint-uri estimate
- Harta traseu (Leaflet)
- Saved routes per user (`/me/routes`)
- Ride logs + stats (`/me/rides/log`, `/me/rides/stats`)
- Hazard reporting geolocalizat (`/hazards`)

## Structura proiectului

```text
WeatherForMoto/
├── index.html              # Aplicația principală PWA
├── sw.js                   # Service Worker pentru PWA
├── manifest.json           # Web App Manifest
├── privacy-policy.html     # Politică de confidențialitate
├── capacitor.config.json   # Configurare Capacitor (iOS/Android)
├── package.json            # Dependencies pentru Capacitor
├── Dockerfile              # Container pentru deployment
├── README.md               # Acest fișier
├── icons/                  # Icon-uri PWA
├── www/                    # Asset-uri statice pentru PWA
└── backend/                # Backend FastAPI
		├── main.py
		├── auth_alerts.py
		├── weather_service.py
		├── tests.py
		├── requirements.txt
		├── .env.example
		├── migrate_to_turso.py
		└── entrypoint.sh
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

### 2. Frontend

Deschide `index.html` în browser. Aplicația detectează automat backend-ul local sau folosește fallback Open-Meteo când API-ul nu este disponibil.

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
- `APP_BASE_URL` (URL public al aplicației, folosit în email-uri)
- `API_BASE_URL` (URL public al backend-ului, pentru linkurile de confirmare și dezabonare; implicit URL-ul run.app)
- `MET_NORWAY_USER_AGENT` (identificator cerut de MET Norway; are o valoare implicită reală)

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

**Email alerts:**

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
- `GET /meta/scoring` - Pragurile scorului, benzile de intensitate a ploii și matricea șanse × intensitate
- `GET /` - Serve frontend
- `GET /manifest.json` - Web App Manifest
- `GET /sw.js` - Service Worker
- `GET /privacy-policy` - Politică de confidențialitate

### Weather și geocoding

- `GET /geocode?city=Cluj-Napoca` - Geocoding oraș
- `GET /weather?city=Cluj-Napoca&days=14` - Vreme după oraș
- `GET /weather?lat=46.77&lon=23.59&days=14` - Vreme după coordonate
- `GET /route?origin=Cluj-Napoca&destination=Sibiu&departure=2026-04-13T09:00&avg_speed=80` - Rută simplă
- `GET /route/multi?stops=Cluj-Napoca;Alba-Iulia;Sibiu&departure=2026-04-13T09:00&avg_speed=80` - Rută multi-oprire

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
- `GET /alerts/unsubscribe?token=...` - Pagina de dezabonare cu buton de confirmare
- `POST /alerts/unsubscribe?token=...` - Dezabonare (butonul și one-click din clientul de email)

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
```

Testele nu depind de rețea. Pe Windows, `tests.py` are nevoie de `PYTHONIOENCODING=utf-8` ca să poată afișa simbolurile din output.

## Deploy

### Google Cloud Run (actual)

```bash
# Build și push imagine
docker build -t europe-west1-docker.pkg.dev/weatherformoto/weatherformoto/backend:latest .
docker push europe-west1-docker.pkg.dev/weatherformoto/weatherformoto/backend:latest

# Deploy
gcloud run deploy weatherformoto \
  --image europe-west1-docker.pkg.dev/weatherformoto/weatherformoto/backend:latest \
  --platform managed \
  --region europe-west1 \
  --allow-unauthenticated \
  --port 8080 \
  --memory 512Mi \
  --env-vars-file cloudrun-env.yaml \
  --project=weatherformoto
```

În practică serviciul e publicat din sursă (Cloud Build folosește `Dockerfile`), iar variabilele de mediu existente se păstrează:

```bash
gcloud run deploy weatherformoto --source . --region europe-west1 --project weatherformoto
```

**Migrare schemă (important):** schema nu mai rulează la fiecare pornire (era cauza principală a cold-start-ului lent). Când o versiune adaugă coloane sau tabele, noua revizie pornește întâi fără trafic și rulează migrarea la startup, apoi primește traficul, apoi variabila se scoate:

```bash
gcloud run deploy weatherformoto --source . --region europe-west1 \
  --no-traffic --update-env-vars RUN_DB_MIGRATIONS=true
gcloud run services update-traffic weatherformoto --region europe-west1 --to-latest
gcloud run services update weatherformoto --region europe-west1 \
  --remove-env-vars RUN_DB_MIGRATIONS
```

Codul funcționează și pe o bază nemigrată (vechiul comportament), deci ordinea de mai sus doar evită o fereastră în care instanțele noi nu văd încă coloanele noi.

### Frontend static pe Cloudflare Pages (zero-cost)

Arhitectura recomandată: frontend-ul static servit de la edge prin Cloudflare Pages (instant, fără cold start), iar backend-ul FastAPI rămâne pe Cloud Run doar pentru API. `index.html` cheamă deja API-ul la URL-ul Cloud Run (`CONFIGURED_BACKEND_URL`), iar CORS-ul backend-ului permite originea Pages.

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
- Pentru email alerts reale, trebuie configurat SMTP sau Brevo
- Directorul `www/` conține asset-uri statice pentru PWA, copiate automat de Capacitor