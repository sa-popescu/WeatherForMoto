# WeatherForMoto – Python Backend

A FastAPI-based backend that **aggregates weather data from multiple sources** (OpenWeatherMap + Open-Meteo) to deliver accurate statistics optimised for motorcyclists.

---

## Features

| Feature | Detail |
|---|---|
| **Multi-source aggregation** | Open-Meteo (free, no key) + OpenWeatherMap (key required) |
| **Current weather** | Temperature, feels-like, humidity, wind, gusts, pressure, visibility, air quality |
| **7-day daily forecast** | Min/max temps, precipitation probability & amount, wind gusts |
| **Hourly forecast** | 7-day hourly breakdown |
| **Moto suitability score** | 0-100 score + label (IDEAL / OK / ACCEPTABIL / RISCANT / EVITĂ) |
| **Dynamic city** | Name search **or** lat/lon coordinates |
| **Default city** | Bucharest (configurable via env var) |

---

## Quick Start

### 1. Install dependencies

```bash
cd backend
pip install -r requirements.txt
```

### 2. Configure environment

```bash
cp .env.example .env
# Edit .env – set your OpenWeatherMap API key
```

### 3. Run the server

```bash
python main.py
# or
uvicorn main:app --reload --port 8000
```

The API will be available at **http://localhost:8000**.  
Interactive docs: **http://localhost:8000/docs**

---

## API Endpoints

### `GET /health`
Returns server status.

```json
{"status": "ok", "version": "1.0.0"}
```

### `GET /weather`
Returns aggregated weather data.

| Parameter | Type | Description |
|---|---|---|
| `city` | string | City name (e.g. `Cluj-Napoca`) |
| `lat` | float | Latitude (use with `lon`) |
| `lon` | float | Longitude (use with `lat`) |

When no parameter is given, defaults to **Bucharest**.

**Examples:**
```
GET /weather
GET /weather?city=Timisoara
GET /weather?lat=46.77&lon=23.59
```

### `GET /geocode`
Resolves a city name to coordinates.

```
GET /geocode?city=Brasov
```

---

## Response Schema (abbreviated)

```json
{
  "city": "Bucharest, RO",
  "latitude": 44.43,
  "longitude": 26.10,
  "timezone": "Europe/Bucharest",
  "current": {
    "temperature": 18.5,
    "feels_like": 17.2,
    "humidity": 62,
    "wind_speed_kmh": 14.4,
    "wind_gusts_kmh": 22.3,
    "wind_direction": "NE",
    "beaufort": "3",
    "precipitation_mm": 0.0,
    "description": "Parțial înnorat",
    "icon": "⛅",
    "pressure_hpa": 1015,
    "visibility_km": 10.0,
    "aqi": 2,
    "moto_score": 82,
    "moto_label": "IDEAL",
    "sources": ["open-meteo", "openweathermap"]
  },
  "daily": [ /* 7 days */ ],
  "hourly": [ /* 168 hours */ ]
}
```

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `OPENWEATHERMAP_API_KEY` | *(built-in key)* | Your OWM API key |
| `DEFAULT_CITY` | `Bucharest` | City used when no location is specified |
| `PORT` | `8000` | Port to listen on |

---

## Deployment

The server exposes a standard ASGI app (`main:app`) and can be deployed on any platform that supports Python:

- **Railway / Render / Fly.io** – set `PORT` env var, start command: `uvicorn main:app --host 0.0.0.0 --port $PORT`
- **Docker** – `COPY backend/ /app && pip install -r /app/requirements.txt && uvicorn main:app --host 0.0.0.0 --port 8000`
- **PythonAnywhere** – upload `backend/` folder, configure WSGI to point at `main:app`
