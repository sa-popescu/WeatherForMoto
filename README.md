# Motonor

Motonor este o aplicație de vreme și drum pentru motocicliști: scor moto pe ore, trasee cu trafic live și vremea pe care o prinzi pe drum, înregistrarea turelor și alerte personalizate (notificări push). Repo-ul se numește `motonor`; serviciile din cloud (Cloud Run, Worker-ul Cloudflare, baza Turso) păstrează numele `weatherformoto`.

Stack-ul actual este:

- Frontend: `app/` — Vite + React + TypeScript, PWA (service worker Workbox), pregătit pentru Capacitor
- Backend: FastAPI + Turso, pe Google Cloud Run (doar API)
- Deploy frontend: Cloudflare (static, din `scripts/build-pages.sh`)
- URL: https://motonor.bluemouse.cc (adresa run.app redirecționează aici). Vechea adresă, https://weatherformoto.bluemouse.cc, servește aceeași aplicație și oferă mutarea datelor de pe telefon pe adresa nouă (`app/src/features/move/`)

Vechiul frontend dintr-un singur fișier a fost retras: `index.html` și `sw.js` din rădăcină sunt acum doar o pagină de redirecționare și un service worker care îl dezinstalează pe cel vechi (GitHub Pages publică rădăcina repo-ului). Codul vechi rămâne în istoricul git.

## Ce oferă soluția în forma curentă

### Core weather

- Agregare multi-sursă: modele (Open-Meteo, OpenWeatherMap, MET Norway, Pirate Weather) și măsurători (stațiile oficiale ANM prin EUMETNET MeteoGate, observațiile aeroporturilor METAR de la NOAA, WeatherXM, Netatmo, Sensor.Community). Stația oficială cea mai apropiată intră în media condițiilor de acum cu ponderea cea mai mare, care scade cu distanța; aeroportul dă fenomenul observat (ploaie, furtună, ceață) și vizibilitatea
- Rețelele de stații întrebate pe zone (`purpose=forecast` le lasă deoparte pentru punctele de traseu și favorite, unde nu se măsoară „acum”):
	- **Netatmo**: lista stațiilor unei zone de ~5 km se reîmprospătează la 15 minute, iar ultima listă bună rămâne în folosință cât timp se cere una nouă (citirile mai vechi de o oră tot cad); o eroare se reîncearcă după 5 minute, iar o cheie de acces refuzată se reînnoiește o dată;
	- **WeatherXM**: planul are cam 500 de cereri pe lună, așa că aplicația cere lista stațiilor o dată pe săptămână, o citire doar când cea veche nu mai contează și cel mult 15 cereri pe zi (`WXM_DAILY_BUDGET`); după un refuz așteaptă până pe 1 ale lunii, cu o încercare pe zi;
	- **Sensor.Community** (`backend/sensor_community.py`): senzorii cetățenilor din 6 km, fără cei din interior; contează mediana celor care sunt de acord (cel puțin 3), cu pondere sub Netatmo
- „N surse” din antetul ecranului „Acum” se poate atinge: deschide lista surselor, cu rolul fiecăreia, dacă a răspuns acum, stația folosită, distanța și vechimea măsurătorii (`current.source_status` în răspunsul `/weather`)
- Ansamblu de modele: aceleași ore cerute separat de la ECMWF, ICON (DWD), GFS (NOAA), ARPEGE (Météo-France), UKMO, GEM (Canada) și AIFS (modelul cu inteligență artificială al ECMWF, fără rafale), fără cheie suplimentară. Valorile orare sunt trase spre media modelelor (pentru ploaie mediana, ca un singur model cu o aversă să nu devină burniță peste tot; blend-ul Open-Meteo păstrează greutate dublă), codul de vreme e votat de modele, iar dispersia dintre ele dă încrederea afișată: „sunt de acord”, „diferă puțin”, „nu sunt de acord”
- Ploaia din toate sursele (`backend/rain_fusion.py`): șansa de ploaie a fiecărei ore e media ponderată a prognozelor ansamblu (ICON-EU EPS, ECMWF ENS, GEFS, peste 100 de rulări, API-ul de ansamblu Open-Meteo), a modelelor naționale, a probabilității Open-Meteo, Pirate Weather, OpenWeatherMap și MET Norway. Fiecare oră spune și câte surse dau ploaie din câte au răspuns, plus intervalul probabil al cantității. În primele ~90 de minute aplicația pune peste ea radarul extrapolat
- Corecția măsurată: când o stație măsoară acum, diferența dintre ea și prognoza orei curente se păstrează și în orele următoare, tot mai slab, până dispare după 6 ore. Temperatura stației oficiale e adusă la altitudinea locului (0,65°C la 100 m, altitudini de la API-ul de elevație Open-Meteo)
- Condiții curente + forecast daily + hourly
- Moto score (0-100), calculat pe server pentru fiecare oră, cu etichete IDEAL (≥85), OK (60–84), ATENȚIE (40–59), EVITĂ (<40); constantele sunt publicate la `GET /meta/scoring`
- Ploaia se punctează după șanse × intensitate (mm/h: urme, slabă, moderată, puternică), niciodată doar după procent: 70% cu 0,1 mm nu e tratat ca ploaie adevărată
- Ploaia măsurată de radar (`backend/opera_radar.py`): compozitul european OPERA (EUMETNET, CC BY 4.0, prin MeteoGate) dă ploaia fiecărei ore pe o grilă de 2 km, la 15 minute. Serverul păstrează ultimele 6 ore, decupate pe România și vecini (cam 1 MB pe oră), și le citește la fiecare loc ca mediana celor 3×3 celule, fără celulele pe care compozitul le notează ca slabe. Pentru acele ore, ploaia radarului ia locul celei din model la starea drumului. Fișierele HDF5 se citesc cu `h5py`. O cheie MeteoGate (`METEOGATE_API_KEY`, secretul `meteogate-api-key`) ridică limitele; fără ea merge cu limita anonimă
- Fulgerele din satelit (`backend/mtg_lightning.py`): Lightning Imager-ul de pe Meteosat de generația a treia (EUMETSAT MTG-I1) vede fiecare descărcare, inclusiv cele din nor. Fișierele LI-2-LFL din EUMETSAT Data Store vin la 10 minute, cu circa 10 minute întârziere. Serverul păstrează descărcările din ultimele 40 de minute peste România și vecini și cere fișiere noi cel mult la 5 minute. Pentru fiecare loc, răspunsul are `lightning`: câte descărcări au fost pe 30 km în ultimele 30 de minute, câte pe 10 km, cea mai apropiată (distanță, direcție, vechime). Aplicația arată un banner pe ecranul Acum când există descărcări. E nevoie de un cont EUMETSAT gratuit: cheia și secretul API (`EUMETSAT_CONSUMER_KEY`, `EUMETSAT_CONSUMER_SECRET`, secretele `mm-eumetsat-key` și `mm-eumetsat-secret`); fără ele sursa e oprită. Credit: © EUMETSAT
- Praful saharian (CAMS, prin API-ul de calitate a aerului Open-Meteo, aceeași cerere ca pentru AQI și polen): dacă în următoarele 24 de ore praful de la sol trece de 50 µg/m³ (aer încețoșat, ploaie cu noroi) sau de 150 µg/m³ (vizibilitate slabă), `current.dust` spune nivelul, ora cea mai rea și prima oră cu ploaie cât e praf în aer. Aplicația arată un banner pe ecranul Acum. AQI-ul și polenul se citesc acum la ora curentă, nu la miezul nopții
- Drumul după ploaie (`backend/wet_road.py`): după ce se oprește ploaia, drumul e socotit ud o vreme, mai mult la frig, umezeală, noapte, cer acoperit și fără vânt, mai puțin la soare și vânt; iar primele ore ale primei ploi după 5 zile uscate sunt mai alunecoase. Fiecare costă 8 puncte, fără plafon (`wet_road` și `first_rain` în `score_breakdown`, regulile în `/meta/scoring`). Ploaia zilelor trecute vine dintr-o cerere mică la Open-Meteo (`past_days=5`, cu ploaia și ce încălzește sau răcește asfaltul); fără ea, începutul seriei e socotit uscat și nicio ploaie nu e numită prima. E o estimare, nu o măsurătoare
- Temperatura asfaltului (`backend/road_surface.py`): bilanțul termic al unei coloane de asfalt, piatră și sol, rulat pe ultimele 3 zile și pe orele de prognoză, ca modelele de drum (METRo). Intră soarele modelelor (împărțit în oră după înălțimea soarelui), infraroșul cerului din temperatură, umezeală și nori (norii joși contează cel mai mult), vântul, ploaia, zăpada și solul modelelor la 54 cm. Cerul observat la cel mai apropiat aeroport (METAR, până la 40 km) înlocuiește norii modelului acum și se estompează în orele din jur; senzorul de soare al unei stații WeatherXM contează cât modelele. Intervalul vine din aceeași coloană rulată cu cerul mai senin și mai noros, cât de departe le pun modelele naționale, plus o marjă pentru umbră, trafic și vârsta asfaltului. Suprafața solului din modele e un martor: un asfalt departe de toate scade încrederea. Răspunsul are `road_surface_temp`, `road_surface_range` și, la acum, `road_surface` (interval, încredere, de ce). Riscul de polei și scorul se recalculează cu valoarea nouă. Fără date, rămâne estimarea rapidă de dinainte
- Căutare de locuri: orașe, sate, cartiere, străzi și locuri cunoscute (Photon, date OpenStreetMap, cu rezultatele din jurul locului curent primele; Open-Meteo ca rezervă), plus GPS
- Linkurile de partajare poartă și coordonatele (`?q=Nume&ll=lat,lon`), ca un loc cu nume comun să se deschidă exact unde a fost trimis
- Fereastră optimă de mers (azi/mâine)
- Date extinse: UV, presiune, vizibilitate, frost risk, temperatură estimată carosabil
- Avertizări oficiale de la Meteoalarm (ce emite ANM pe județe) și avertizările ANM de tip nowcasting (30-90 de minute, pe zona desenată de meteorologi), afișate ca atare deasupra scorului. Se potrivesc pe poligonul sau cercul din avertizare, iar când feedul dă doar nume de zone, după numele localității

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

- Tab-urile aplicației: **Drum** (ecranul principal), **Vreme** (prognoza și harta radar), **Ture** (turele de pe telefon și jurnalul din cont), **Eu**
- Drum: harta, „Unde mergi?” cu Acasă, Serviciu și destinațiile recente, și vremea locului; după alegerea destinației, până la 3 variante cu trafic live (TomTom prin `/route/urban`, sau OSRM fără trafic când TomTom nu răspunde), fiecare cu vremea întâlnită pe drum la ora la care treci, apoi pornirea turei pe varianta aleasă
- Route planner cu 2-10 opriri, fiecare cu opțiunea „locația mea” (din Drum: „Plan cu opriri”); drumul vine tot de la TomTom (cu vitezele obișnuite ale drumurilor, nu traficul de acum), cu OSRM ca rezervă
- Opțiuni de traseu (`app/src/features/route/routeOptions.ts`, păstrate pe telefon): cel mai rapid, echilibrat, cel mai scurt, economic sau cu curbe (cu cât de multe curbe și cât deal), ce să evite (autostrăzi, taxe, feriboturi, neasfaltate, tuneluri, treceri de graniță) și viteza maximă a motocicletei. Se aplică în Drum, la plecarea perfectă, la recalcularea din tură și în planificator. Când TomTom nu găsește nimic cu ele, aplicația cere o dată cel mai rapid traseu și spune asta; OSRM nu le știe
- Route weather snapshots pe waypoint-uri estimate
- Soarele în ochi (`app/src/features/route/sunGlare.ts`): drumul e verificat din 2 în 2 km, cu poziția soarelui calculată în aplicație (formulele Astronomical Almanac, fără bibliotecă) și direcția de mers de la vântul lateral. O porțiune de cel puțin 3 km spre un soare sub 20° deasupra orizontului, la cel mult 25° de direcția de mers, cu cerul acoperit sub 60%, apare ca avertizare în planificator și ca rând pe varianta din Drum
- Hărțile (tura, traseul, Hartă): MapLibre cu hărți vectoriale OpenFreeMap (date OpenStreetMap)
- Hartă cu o singură bandă de timp, pe același scrubber, în două moduri:
	- **Ploaie**: radarul observat (RainViewer, ultimele 2 ore) cu fulgerele văzute de satelit (EUMETSAT MTG Lightning Imager, la 5 minute), apoi radarul extrapolat 90 de minute (ultimul cadru mutat pe direcția și viteza măsurate din ultimele cadre, block matching într-un Web Worker), care după 20 de minute se estompează treptat în ploaia din modelul ICON-EU al DWD (celule de ~7 km, citite ca valori prin WCS de pe `maps.dwd.de`), din 30 în 30 de minute pentru primele 6 ore, apoi orar; partea care cade ca zăpadă vine din grila Open-Meteo (`snowfall`), fiindcă ICON-EU pe serverul DWD are doar precipitații;
	- **Nori**: norii din satelit (EUMETSAT: masca de nori Meteosat pentru unde e înnorat, infraroșul MTG pentru cât de sus), extrapolați la fel, apoi prognoza de nebulozitate Open-Meteo pe o grilă;
	- nimic nu e afișat ca imagine întinsă: fiecare sursă devine valori pe un canvas la zoomul radarului (doi octeți pe pixel; la ploaie intensitatea în dBZ și cât e zăpadă, la nori cât se vede și cât de sus, la fulgere câte descărcări), iar un strat WebGL propriu (`app/src/features/map/valueLayer.ts`) le citește lin pentru fiecare pixel de ecran și abia apoi le colorează, cu o paletă pentru harta închisă și una pentru cea deschisă;
	- radarul se citește din dalele RainViewer nenetezite (`/2/0_1.png`), decodate exact înapoi în dBZ din tabelul public de culori; serviciul gratuit dă doar paleta Universal Blue, până la zoom 7 și fără prognoză proprie;
	- dalele și imaginile din satelit se citesc într-un Web Worker (`fieldWorker.ts`, cu OffscreenCanvas), sau pe pagină unde browserul nu poate;
	- între două momente la cel mult 20 de minute distanță ploaia se deplasează pe direcția măsurată, nu se estompează pe loc; în timpul redării, trecerea ține cât un pas;
	- cu economizorul de date (Save-Data, 2G sau `prefers-reduced-data`) banda nu pornește singură și se descarcă doar momentele afișate; altfel o bandă de radar înseamnă cam 1,5 MB pe telefon și 2,5 MB pe desktop;
	- `e2e/map.spec.ts` verifică în CI că harta chiar desenează ploaia, cu dale radar din fixtures
- Saved routes per user (`/me/routes`)
- Ride logs + stats (`/me/rides/log`, `/me/rides/stats`)
- Hazard reporting geolocalizat (`/hazards`)

## Structura proiectului

```text
motonor/
├── app/                    # Frontend: Vite + React + TypeScript, PWA (vezi mai jos)
├── backend/                # API FastAPI (vezi backend/README.md)
│   ├── main.py, weather_service.py, auth_alerts.py
│   ├── official_stations.py, metar.py, meteoalarm.py, anm_nowcast.py  # surse de observații și avertizări
│   ├── tests.py, test_scoring.py, test_auth_alerts.py, test_frontend_routes.py
│   ├── requirements.in     # dependențe directe
│   ├── requirements.txt    # toate versiunile fixate (Linux, Python 3.11)
│   └── .env.example, entrypoint.sh, migrate_to_turso.py
├── scripts/build-pages.sh  # build-ul frontend-ului pentru Cloudflare
├── .github/                # CI, deploy API, redirect GitHub Pages, Dependabot
├── Dockerfile              # imaginea API-ului (Cloud Run)
├── wrangler.jsonc          # Cloudflare: static assets (dist/) + linkurile scurte de traseu (KV)
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
- `TOMTOM_API_KEY` (traseele urbane cu trafic live, `/route/urban`; pe Cloud Run vine din secretul `mm-tomtom-key`. Fără ea, aplicația face traseul fără trafic)
- `APP_BASE_URL` (URL public al aplicației, folosit în emailurile de cont)
- `API_BASE_URL` (URL public al backend-ului, pentru linkurile de confirmare a adresei; implicit URL-ul run.app)
- `MET_NORWAY_USER_AGENT` (identificator cerut de MET Norway; are o valoare implicită reală)
- `METEOALARM_FEED_URL` (feedul CAP de avertizări; implicit cel pentru România)
- `VERIFICATION_LOG` (implicit `true`; cu `false` backend-ul nu mai scrie jurnalul de verificare a prognozei)

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

- `GET /health` - Status server; răspunde 200 cât timp procesul e pornit, iar câmpul `database` spune dacă baza de date răspunde (`ok`, `unavailable`, sau `off` când nu e configurată)
- `GET /health/db` - 200 dacă baza de date răspunde, 503 dacă nu (pentru un monitor)
- `GET /meta/scoring` - Pragurile scorului, benzile de intensitate a ploii, matricea șanse × intensitate și regula de încredere între modele
- `GET /` - Redirecționează către aplicație (`APP_BASE_URL`)
- `GET /sw.js` - Service worker care îl retrage pe cel vechi de pe acest domeniu
- `GET /privacy-policy` - Politică de confidențialitate

### Weather și geocoding

- `GET /geocode?city=Cluj-Napoca` - Geocoding oraș
- `GET /weather?city=Cluj-Napoca&days=14` - Vreme după oraș
- `GET /weather?lat=46.77&lon=23.59&days=14` - Vreme după coordonate

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

- `GET /route/urban?points=lat,lon;lat,lon[;...]` - Trasee TomTom pentru motocicletă, 2-10 puncte; opțional `depart_at`, `route_type` (fastest, short, shortest, eco, thrilling), `avoid` (listă cu virgulă; lipsă = neasfaltate, `none` = nimic), `windingness`/`hilliness` (doar la thrilling), `top_speed` (km/h), `traffic=false` (vitezele obișnuite, pentru un plan)
- `GET /me/routes` - Rute salvate
- `POST /me/routes` - Salvează rută (un traseu importat din GPX își păstrează și drumul, în `track`; coloana `saved_routes.track_json` apare după migrare)
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
python -m unittest test_sources       # stații oficiale, METAR, nowcasting ANM, starea surselor, ploaia combinată, jurnalul de verificare
```

Testele nu depind de rețea. Pe Windows, `tests.py` are nevoie de `PYTHONIOENCODING=utf-8` ca să poată afișa simbolurile din output.

## Verificarea prognozei

Când Turso e configurat, backend-ul scrie un jurnal mic (`backend/verification.py`): pentru fiecare zonă de ~5 km, cel mult o dată pe oră, ce a prezis fiecare sursă de ploaie și temperatura finală la 1, 3, 6, 12, 24 și 48 de ore, plus ce au măsurat stația oficială, aeroportul, WeatherXM sau Netatmo. Nu conține nimic despre utilizator. Tabelele `forecast_log` și `observation_log` se creează la migrare (`RUN_DB_MIGRATIONS=true`); până atunci scrierile se sar.

Raportul citește jurnalul și arată, pe fiecare sursă și orizont, cât de bine a nimerit ploaia (scor Brier și skill față de frecvența locală) și temperatura (eroarea finală față de Open-Meteo brut), plus ponderile propuse pentru `rain_fusion.WEIGHTS`:

```bash
cd backend
python verify_report.py --days 30
```

Jurnalul păstrează 90 de zile (`verification.RETENTION_DAYS`). `backend/calibration.py` îl citește o dată pe zi, învață din el ponderile surselor de ploaie și ține rezultatul în `app_state`, de unde îl iau celelalte instanțe fără să mai citească jurnalul. Turso taxează fiecare rând peste care trece o interogare, așa că potrivirea dintre prognoze și măsurători se face în Python (`verification.rain_pairs`), nu printr-un join în SQL.

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

**Vreme severă:** furtună, vijelie, grindină, ploi torențiale (`backend/severe_alerts.py`). Ora de la `dispatch-all` le trimite deja. Ca o avertizare nowcasting ANM (valabilă 30 - 90 de minute) să ajungă la timp, `POST /alerts/dispatch-severe` verifică doar avertizările și fulgerele, fără furnizorii de prognoză, și e făcut să ruleze la 10 minute. Job-ul se creează o singură dată, cu același secret ca `mm-dispatch-alerts`:

```bash
gcloud scheduler jobs create http mm-dispatch-severe --location europe-west1 --project weatherformoto \
  --schedule "*/10 * * * *" --time-zone "Europe/Bucharest" --http-method POST \
  --uri "https://weatherformoto-1056457771445.europe-west1.run.app/alerts/dispatch-severe" \
  --headers "X-Dispatch-Secret=<același secret ca la mm-dispatch-alerts>"
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
- Custom domains pe Worker (Settings › Domains & Routes): `motonor.bluemouse.cc` și, cât timp mai e folosită, vechea `weatherformoto.bluemouse.cc`

Linkuri scurte de traseu: Worker-ul are un singur script, `app/src/worker/shortLinks.ts`, care primește doar `/s` și `/s/*` (`run_worker_first` în `wrangler.jsonc`). `POST /s` păstrează planul unui traseu partajat în Workers KV (binding `SHORT_LINKS`) timp de 90 de zile, iar `GET /s/<id>` redirecționează către `/#/traseu?…`. Binding-ul KV are ID-ul spațiului scris în `wrangler.jsonc` (din dashboard: Storage & Databases › KV). Fără ID, crearea automată a lui wrangler pică la al doilea deploy din Workers Builds (eroarea 10014, cloudflare/workers-sdk#14284). Dacă binding-ul lipsește, `/s` răspunde 503 și aplicația trimite linkul lung.

Statistici de utilizare (`app/src/worker/usage.ts`, `app/src/lib/usage.ts`): aplicația notează ce părți folosește o vizită (lista e în `app/src/lib/usageEvents.ts`) și le trimite o dată, cu `sendBeacon`, la `POST /usage`, când pagina e ascunsă sau închisă. Nu trimite nimic în dev sau când browserul cere Global Privacy Control / Do Not Track. Un Durable Object (`UsageStats`, binding `USAGE_STATS`) ține doar totaluri pe zi (ziua României), cel mult 400 de zile, cu o limită de raportări pe adresă și pe zi. Totalurile se văd la `https://motonor.bluemouse.cc/stats`, după cheia din secretul `STATS_KEY` (cel puțin 24 de caractere; fără el pagina spune că statisticile sunt oprite). Secretul se pune în dashboard: Workers & Pages › weatherformoto › Settings › Variables and Secrets › Add, tip Secret. O cheie aleatorie se poate face local, în PowerShell:

```powershell
$bytes = New-Object byte[] 30; [Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($bytes); [Convert]::ToBase64String($bytes)
```

Pe backend (Cloud Run) setează `ALLOWED_ORIGINS` ca să includă originea frontend-ului:

```bash
gcloud run services update weatherformoto --region europe-west1 \
  --update-env-vars "^@^ALLOWED_ORIGINS=https://motonor.bluemouse.cc,https://weatherformoto.bluemouse.cc,https://localhost,capacitor://localhost@APP_BASE_URL=https://motonor.bluemouse.cc"
```

Backend-ul permite și originile de preview, ca un PR să poată fi încercat cu vremea live înainte de merge: cele din Workers Builds (`<versiune sau branch>-weatherformoto.spopescu.workers.dev`) și cele vechi din Pages (`*.weatherformoto.pages.dev`). Un preview vorbește cu API-ul și baza de date de producție.

> De când Worker-ul are Durable Objects (locația live), Cloudflare nu mai generează linkuri de preview pentru versiuni (`wrangler versions upload`). Preview-urile vin acum din Cloudflare Worker Previews, pornite din dashboard (Settings › Builds › Set up Worker Previews; ele rulează `wrangler preview`, din wrangler 4.135), iar blocul `previews` din `wrangler.jsonc` le dă fiecăruia propriile Durable Objects și niciun KV (linkurile scurte sunt oprite acolo, aplicația trimite linkul întreg). Un preview e la `<branch>-weatherformoto.spopescu.workers.dev`, e public pentru oricine are linkul și vorbește cu API-ul de producție.
>
> Preview-ul Vercel din verificările PR-ului cere login în Vercel, iar originea lui nu e permisă de backend, deci acolo merge harta radar, dar nu datele meteo din API.

> Cheia OpenWeatherMap nu mai este expusă în client. Este folosită doar server-side de backend; calea browser-direct (fallback când backend-ul nu răspunde) rulează fără ea, pe Open-Meteo.

### Docker local

```bash
docker build -t motonor .
docker run --rm -p 8000:8000 --env-file backend/.env motonor
```

## Observații practice

- Pentru push notifications reale, trebuie configurat VAPID pe backend
- Emailurile de cont au nevoie de SMTP sau Brevo; dacă contul Brevo are restricție de IP („Authorized IPs”), trebuie să accepte ieșirea Cloud Run, altfel Brevo răspunde 401
- Directorul `www/` conține asset-uri statice pentru PWA, copiate automat de Capacitor
