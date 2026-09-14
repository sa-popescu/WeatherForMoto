# MotoMeteo API (backend)

FastAPI service behind the MotoMeteo app. It aggregates several weather
providers, scores riding conditions per hour, and handles accounts, alerts,
saved routes, ride logs and rider-reported hazards. It runs on Google Cloud Run
(`weatherformoto`, europe-west1) with a Turso (libSQL) database.

## Layout

| File | What it does |
|---|---|
| `main.py` | app, CORS, security headers, weather / route / geocode / meta routes |
| `weather_service.py` | provider clients, merge, cache and time budget, moto score |
| `auth_alerts.py` | accounts, sessions, email verification, prefs, push, alerts, routes, rides, hazards |
| `official_stations.py` | official synoptic stations (ANM) through EUMETNET MeteoGate, nearest fresh reading per field |
| `metar.py` | Romanian airport METAR from NOAA AWC: present weather code and visibility |
| `meteoalarm.py` | Meteoalarm CAP county warnings |
| `anm_nowcast.py` | ANM nowcasting warnings with their drawn polygons |
| `migrate_to_turso.py` | one-off copy of a legacy SQLite file into Turso |
| `entrypoint.sh` | container start (uvicorn, no access log) |

## Scoring in one paragraph

Every hour gets a 0-100 score: IDEAL >= 85, OK >= 60, ATENȚIE >= 40, EVITĂ below.
Rain counts by probability x intensity (mm/h bands: traces, light, moderate,
heavy), never by probability alone, so 70% with 0.1 mm is not treated as real
rain. Cold, heat, gusts, fog, snow, ice and storms add penalties or caps. The
constants are published at `GET /meta/scoring`, and the frontend reads them from
there.

## Run locally

```bash
cd backend
python -m venv .venv
. .venv/bin/activate            # Windows: .venv\Scripts\activate
pip install -r requirements.txt  # libsql-experimental has no Windows wheel
cp .env.example .env             # fill in Turso and provider keys
uvicorn main:app --reload --port 8000
```

`requirements.txt` pins every package (resolved on Linux for Python 3.11, the
container runtime). `requirements.in` lists the direct dependencies; update the
pins from it when upgrading on purpose, otherwise let Dependabot propose them.

## Tests

```bash
python tests.py
python -m unittest test_scoring test_auth_alerts test_frontend_routes
```

No network is needed. `test_auth_alerts` runs the real SQL on SQLite through a
stub `libsql_experimental` module. On Windows set `PYTHONIOENCODING=utf-8` for
`tests.py`.

## Configuration

All variables are documented in `.env.example`. In production the sensitive ones
(Turso token, provider keys, SMTP password, Brevo key, VAPID private key,
Netatmo secrets, dispatch secret) come from Secret Manager as `mm-*` secrets;
the rest are plain Cloud Run environment variables.

## Deploy

Pushing to `main` with changes under `backend/`, `Dockerfile` or `.dockerignore`
runs `.github/workflows/deploy-backend.yml`: CI first, then an image built by
GitHub Actions, pushed to Artifact Registry and deployed to Cloud Run through
Workload Identity Federation (no stored keys). For schema changes run that
workflow manually with `run_migrations`.

Alert dispatch runs hourly from the Cloud Scheduler job `mm-dispatch-alerts`,
which calls `POST /alerts/dispatch-all` with the `X-Dispatch-Secret` header.
