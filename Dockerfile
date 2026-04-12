# ---- Build stage ----
FROM python:3.11-slim AS builder

WORKDIR /app

COPY backend/requirements.txt ./
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# ---- Runtime stage ----
FROM python:3.11-slim

# Create a non-root user for security
RUN useradd -m -u 1000 appuser

WORKDIR /app

# Copy installed packages from builder
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy the frontend assets (served by FastAPI)
COPY index.html sw.js manifest.json ./
COPY icons/ ./icons/

# Copy the backend source code
COPY backend/ ./backend/

# Fix permissions BEFORE switching to non-root user
RUN chmod +x /app/backend/entrypoint.sh \
    && chown -R appuser:appuser /app

ENV PORT=8000
EXPOSE 8000

WORKDIR /app/backend

# entrypoint.sh uses exec so signals (SIGTERM/SIGINT) reach uvicorn directly
ENTRYPOINT ["./entrypoint.sh"]
