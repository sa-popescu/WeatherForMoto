# WeatherForMoto

WeatherForMoto este o aplicație meteo pentru motocicliști, cu scoring de risc, recomandări de echipament, rutare pre-ride și alerte personalizate (push/email).

Stack-ul actual este:

- Frontend: HTML + Tailwind CSS + JavaScript (PWA)
- Backend: FastAPI + Turso
- PWA: Service Worker + Web App Manifest
- Deploy: Docker/Render

## Ce oferă soluția în forma curentă

### Core weather

- Agregare multi-sursă (Open-Meteo + OpenWeatherMap + MET Norway în fallback/enrichment)
- Condiții curente + forecast daily + hourly
- Moto score (0-100) + etichete de risc (IDEAL/OK/ACCEPTABIL/RISCANT/EVITĂ)
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

**Pentru funcții avansate:**

- `AUTH_CODE_TTL_MIN`
- `SESSION_TTL_DAYS`
- `ALLOW_INSECURE_AUTH_CODE`
- `PBKDF2_ITERATIONS`

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
- `GET /me` - Profil utilizator
- `PUT /me/profile` - Actualizează profil
- `PUT /me/prefs` - Actualizează preferințe
- `DELETE /me` - Șterge cont

### Alerts și push

- `GET /push/public-key` - Cheie publică VAPID
- `POST /me/push-subscriptions` - Abonare push
- `DELETE /me/push-subscriptions` - Dezabonare push
- `POST /alerts/check-now` - Verificare alertă manuală
- `POST /alerts/dispatch-all?secret=...` - Dispatch batch

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
python tests.py
```

Testele validează funcțiile de agregare/scoring și logica meteo fără dependență de rețea.

## Deploy

### Render.com (actual)

- **Build**: Dockerfile
- **Start**: `backend/entrypoint.sh`
- **Healthcheck**: `GET /health`
- **Environment variables**: Setează în dashboard-ul Render.com

### Docker local

```bash
docker build -t weatherformoto .
docker run --rm -p 8000:8000 --env-file backend/.env weatherformoto
```

## Observații practice

- Pentru push notifications reale, trebuie configurat VAPID pe backend
- Pentru email alerts reale, trebuie configurat SMTP sau Brevo
- Directorul `www/` conține asset-uri statice pentru PWA, copiate automat de Capacitor