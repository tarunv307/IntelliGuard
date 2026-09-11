# ── Stage 1: builder ──────────────────────────────────────────────
FROM python:3.11-slim AS builder

WORKDIR /build

# Install build deps
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir -r requirements.txt --target /build/deps

# ── Stage 2: runtime ─────────────────────────────────────────────
FROM python:3.11-slim AS runtime

# Create non-root user
RUN addgroup --system appgroup && adduser --system --ingroup appgroup appuser

WORKDIR /app

# Copy installed packages from builder
COPY --from=builder /build/deps /usr/local/lib/python3.11/site-packages

# Copy application source
COPY app/ ./app/

# Run as non-root
USER appuser

EXPOSE 8000

CMD ["python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
