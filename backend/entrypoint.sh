#!/bin/sh
# Entrypoint: start uvicorn, replacing this shell so signals (SIGTERM/SIGINT)
# propagate directly to the server process.
set -e
exec uvicorn main:app --host 0.0.0.0 --port "${PORT:-8000}"
