# Base image pinned by digest for reproducible builds; Dependabot proposes updates.
# ---- Build stage ----
FROM python:3.11-slim@sha256:da047cb8f9d1d98e5c070f5300ba9f7274e33b8fc0e5be5ed88740aed1b95ba9 AS builder

WORKDIR /app

COPY backend/requirements.txt ./
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# ---- Runtime stage ----
FROM python:3.11-slim@sha256:da047cb8f9d1d98e5c070f5300ba9f7274e33b8fc0e5be5ed88740aed1b95ba9

# Create a non-root user for security
RUN useradd -m -u 1000 appuser

WORKDIR /app

# Copy installed packages from builder
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy the frontend assets (served by FastAPI). The privacy policy is served
# at /privacy-policy too, so store listings can link to the API's address.
COPY index.html sw.js manifest.json privacy-policy.html ./
COPY icons/ ./icons/

# Copy the backend source code
COPY backend/ ./backend/

# Normalise line endings (Windows checkouts may introduce CRLF, which breaks the
# shebang -> "no such file or directory") and fix permissions BEFORE switching
# to the non-root user.
RUN sed -i 's/\r$//' /app/backend/entrypoint.sh \
    && chmod +x /app/backend/entrypoint.sh \
    && chown -R appuser:appuser /app

# Drop root privileges for everything that runs from here on.
USER appuser

ENV PORT=8000
EXPOSE 8000

WORKDIR /app/backend

# entrypoint.sh uses exec so signals (SIGTERM/SIGINT) reach uvicorn directly
ENTRYPOINT ["./entrypoint.sh"]
