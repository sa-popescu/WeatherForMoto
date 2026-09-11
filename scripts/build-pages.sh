#!/bin/sh
# Build the static frontend for Cloudflare (Workers static assets).
#
# Produces dist/ with the new app (app/, Vite + React) only: no backend
# source, no Dockerfile, no secrets. Cloudflare serves dist/ from the edge,
# so the UI loads instantly regardless of the Cloud Run API's cold start.
#
# Cloudflare build settings:
#   Build command:            sh scripts/build-pages.sh
#   Build output directory:   dist
set -e

cd app
npm ci --no-audit --no-fund
npm run build
cd ..

rm -rf dist
cp -r app/dist dist

echo "Built dist/ for Cloudflare:"
ls -1 dist
