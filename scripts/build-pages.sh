#!/bin/sh
# Build the static frontend bundle for Cloudflare Pages.
#
# Produces dist/ containing ONLY the client assets — no backend source, no
# Dockerfile, no secrets. Cloudflare Pages serves dist/ from the edge, so the
# UI loads instantly regardless of backend (Cloud Run) cold-start state.
#
# Cloudflare Pages build settings:
#   Build command:            sh scripts/build-pages.sh
#   Build output directory:   dist
set -e

rm -rf dist
mkdir -p dist

# Core single-page app + PWA files
cp index.html sw.js manifest.json privacy-policy.html dist/

# Root-level PWA icons referenced by the manifest / service worker
cp icon-192.png icon-512.png icon-apple.png dist/ 2>/dev/null || true

# Icon set
cp -r icons dist/

# Cloudflare Pages cache/header rules
cp _headers dist/ 2>/dev/null || true

echo "Built dist/ for Cloudflare Pages:"
ls -1 dist
