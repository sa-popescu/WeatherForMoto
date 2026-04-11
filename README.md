# WeatherForMoto 🏍️

Aplicație meteo optimizată pentru motocicliști. Afișează condițiile curente și prognoza pe 7 zile, cu un **scor de suitabilitate moto** (0-100), grafic orar, calitate aer, vizibilitate și date agregate din două surse (Open-Meteo + OpenWeatherMap).

---

## Structura proiectului

```
WeatherForMoto/
├── backend/               # FastAPI – agregare și expunere date meteo
│   ├── main.py            # Endpoints: /health, /weather, /geocode
│   ├── weather_service.py # Logica de fetch + merge surse
│   ├── tests.py           # Teste unitare (fără rețea)
│   ├── requirements.txt
│   └── .env.example
└── index.html             # Frontend – HTML/CSS/JS + Chart.js
```

---

## Pornire rapidă

### 1. Backend (Python 3.11+)

```bash
# Linux / macOS
cd backend
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# Editează .env și adaugă OPENWEATHERMAP_API_KEY (opțional dar recomandat)
uvicorn main:app --reload
```

```bat
:: Windows
cd backend
python -m venv venv && venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env
uvicorn main:app --reload
```

API-ul rulează pe **http://localhost:8000**.  
Documentație interactivă: http://localhost:8000/docs

### 2. Frontend

Deschide `index.html` direct în browser (nu necesită server de fișiere).  
Asigură-te că backend-ul rulează la `http://localhost:8000` (configurat prin variabila `API_URL` din `index.html`).

---

## Endpoint-uri backend

| Metodă | Path | Descriere |
|--------|------|-----------|
| `GET` | `/health` | Status API |
| `GET` | `/weather?city=Cluj-Napoca` | Date meteo după oraș |
| `GET` | `/weather?lat=46.77&lon=23.59` | Date meteo după coordonate |
| `GET` | `/geocode?city=Timișoara` | Geocodare oraș → coordonate |

---

## Variabile de mediu (`.env`)

| Variabilă | Descriere | Default |
|-----------|-----------|---------|
| `OPENWEATHERMAP_API_KEY` | Cheie API OpenWeatherMap (gratuit la openweathermap.org) | demo key |
| `DEFAULT_CITY` | Oraș implicit | `Bucharest` |
| `PORT` | Port server | `8000` |

---

## Rulare teste

```bash
cd backend
python tests.py
```

---

## Surse de date

- **[Open-Meteo](https://open-meteo.com/)** – gratuit, fără cont, model European de înaltă rezoluție
- **[OpenWeatherMap](https://openweathermap.org/)** – date curente, prognoză 5 zile, calitate aer (necesită cheie gratuită)

Datele numerice sunt **medii ponderate** între cele două surse pentru acuratețe maximă.