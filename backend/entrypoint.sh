#!/bin/sh
# Entrypoint: start uvicorn, replacing this shell so signals (SIGTERM/SIGINT)
# propagate directly to the server process.
set -e
PORT="${PORT:-8000}"
echo "[entrypoint] Starting WeatherForMoto backend on port ${PORT}"
exec uvicorn main:app \
  --host 0.0.0.0 \
  --port "${PORT}" \
  --log-level info \
  --access-log
