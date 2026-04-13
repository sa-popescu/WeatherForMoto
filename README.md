# WeatherForMoto

WeatherForMoto este o aplicație meteo pentru motocicliști, cu scoring de risc, recomandări de echipament, rutare pre-ride și alerte personalizate (push/email). 

Stack-ul actual este:

- Frontend: HTML + Tailwind + JavaScript (două variante UI: `index.html` și `www/index.html`)
- Backend: FastAPI + SQLite
- PWA: `sw.js` + `manifest.json`
- Deploy: Docker/Railway

## Ce oferă soluția în forma curentă

### Core weather

- Agregare multi-sursă (Open-Meteo + OpenWeatherMap + MET Norway în fallback/enrichment)
- Condiții curente + forecast daily + hourly
- Moto score (0-100) + etichete de risc
- Geo lookup după oraș sau coordonate
- Fereastră optimă de mers (azi/mâine)
- Recomandări de echipament în funcție de ploaie/vânt/temperatură
- Date extinse: UV, presiune, vizibilitate, frost risk, temperatură estimată carosabil

### Phase A (cont + alerting + PWA)

- Cont clasic: signup/login/logout + profil
- Preferințe avansate de alertă:
	- prag scor minim
	- rafale maxime
	- precipitații maxime
	- probabilitate ploaie
	- praguri min/max temperatură
	- frost risk on/off
	- quiet hours + severitate
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
├── index.html
├── sw.js
├── manifest.json
├── Dockerfile
├── Procfile
├── railway.toml
└── backend/
		├── main.py
		├── auth_alerts.py
		├── weather_service.py
		├── tests.py
		├── requirements.txt
		├── .env.example
		└── entrypoint.sh
```

## Rulare locală

### 1. Backend

```bash
cd backend
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

Backend-ul va fi la `http://localhost:8000`.

### 2. Frontend

Ai două variante:

- varianta principală: deschizi `index.html`
- varianta alternativă: deschizi `www/index.html`

Frontend-ul detectează automat backend-ul local (`http://localhost:8000`) sau poate rula și direct cu fallback Open-Meteo când API-ul nu este disponibil.

## Variabile de mediu backend

Minim necesare:

- `OPENWEATHERMAP_API_KEY` (recomandat pentru calitate mai bună a datelor)
- `DEFAULT_CITY` (implicit: `Bucharest`)
- `PORT` (implicit: `8000`)

Pentru funcții avansate:

- `APP_DB_PATH` (path SQLite)
- `AUTH_CODE_TTL_MIN`
- `SESSION_TTL_DAYS`
- `ALLOW_INSECURE_AUTH_CODE`
- `PBKDF2_ITERATIONS`

Email alerts:

- SMTP: `SMTP_HOST`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASS`, `SMTP_FROM`
- sau Brevo API: `BREVO_API_KEY`

Push notifications:

- `VAPID_PUBLIC_KEY`
- `VAPID_PRIVATE_KEY`
- `VAPID_SUBJECT`

Batch dispatch securizat:

- `ALERT_DISPATCH_SECRET`

## API principal

### Meta

- `GET /health`
- `GET /` (serve frontend)
- `GET /manifest.json`
- `GET /sw.js`

### Weather și geocoding

- `GET /geocode?city=Cluj-Napoca`
- `GET /weather?city=Cluj-Napoca&days=14`
- `GET /weather?lat=46.77&lon=23.59&days=14`
- `GET /route?origin=Cluj-Napoca&destination=Sibiu&departure=2026-04-13T09:00&avg_speed=80`
- `GET /route/multi?stops=Cluj-Napoca;Alba-Iulia;Sibiu&departure=2026-04-13T09:00&avg_speed=80`

### Auth + account

- `POST /auth/request-code`
- `POST /auth/verify-code`
- `POST /auth/signup`
- `POST /auth/login`
- `POST /auth/logout`
- `GET /me`
- `PUT /me/profile`
- `PUT /me/prefs`
- `DELETE /me`

### Alerts și push

- `GET /push/public-key`
- `POST /me/push-subscriptions`
- `DELETE /me/push-subscriptions`
- `POST /alerts/check-now`
- `POST /alerts/dispatch-all?secret=...`

### Faza 3 data

- `GET /me/routes`
- `POST /me/routes`
- `DELETE /me/routes/{route_id}`
- `POST /me/rides/log`
- `GET /me/rides/stats`
- `POST /hazards`
- `GET /hazards?lat=...&lon=...&radius_km=120`

## Testare

```bash
cd backend
python tests.py
```

Testele din `backend/tests.py` validează funcțiile de agregare/scoring și logica meteo fără dependență de rețea.

## Deploy

### Railway (actual)

- build: Dockerfile (`railway.toml`)
- start: `backend/entrypoint.sh`
- healthcheck: `GET /health`

### Docker local

```bash
docker build -t weatherformoto .
docker run --rm -p 8000:8000 --env-file backend/.env weatherformoto
```

## Observații practice

- Pentru push real în browser, trebuie VAPID configurat pe backend.
- Pentru email alerts reale, trebuie configurat SMTP sau Brevo.
- `www/index.html` este menținut în paralel cu `index.html`; când se livrează UI fixuri, verifică ambele variante.