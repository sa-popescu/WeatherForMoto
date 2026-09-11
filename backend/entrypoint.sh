#!/bin/sh
# Entrypoint: start uvicorn, replacing this shell so signals (SIGTERM/SIGINT)
# propagate directly to the server process.
#
# uvicorn's access log is disabled: it only duplicated Cloud Run's request logs,
# which already record every request, INCLUDING the full URL and query string.
# So query parameters (hazard coordinates, push endpoints on DELETE, and the
# email-link tokens for /auth/verify-email and /alerts/unsubscribe) still reach
# Cloud Run logging. That is an accepted trade-off: those link tokens are
# single-purpose (verification tokens are single-use and expire in 48 h;
# unsubscribe tokens can only turn email alerts off), and log access is
# restricted to project members.
set -e
PORT="${PORT:-8000}"
echo "[entrypoint] Starting WeatherForMoto backend on port ${PORT}"
exec uvicorn main:app \
  --host 0.0.0.0 \
  --port "${PORT}" \
  --log-level info \
  --no-access-log
