# ─── Stage 1: Builder ─────────────────────────────────────────────────────────
# Installs dependencies into a clean virtual environment.
# Isolating this stage means the final image contains no build tools,
# pip cache, or unnecessary system packages.
FROM python:3.12-slim AS builder

# Prevents Python from writing .pyc files and enables unbuffered stdout/stderr.
# Unbuffered output is critical for Cloud Run structured logging — buffered
# output causes log entries to arrive out of order or be lost on SIGTERM.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /build

COPY requirements.txt .

RUN pip install --upgrade pip && \
    pip install --prefix=/install -r requirements.txt


# ─── Stage 2: Runtime ─────────────────────────────────────────────────────────
# Minimal runtime image — no build tools, no pip, no shell history.
FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    # Ensure installed packages are found at runtime
    PYTHONPATH=/app

# ── Security: run as non-root user ────────────────────────────────────────────
# Cloud Run recommends non-root. UID 1001 avoids collision with system users.
RUN groupadd --gid 1001 appgroup && \
    useradd --uid 1001 --gid appgroup --shell /bin/false --no-create-home appuser

WORKDIR /app

# Copy installed Python packages from builder stage
COPY --from=builder /install /usr/local

# Copy application source only — not tests, Dockerfile, or dev deps
COPY src/ ./src/

# Transfer ownership to non-root user
RUN chown -R appuser:appgroup /app

USER appuser

# ── Health check ──────────────────────────────────────────────────────────────
# Cloud Run also performs its own health checks via the /health endpoint.
# This HEALTHCHECK is for local docker-compose usage.
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:${PORT:-8080}/health')"

# ── Expose & Launch ───────────────────────────────────────────────────────────
# Cloud Run injects PORT env var. We default to 8080 to match.
EXPOSE 8080

# uvicorn flags:
#   --host 0.0.0.0        Required for Cloud Run (container networking)
#   --workers 1           Single worker — WebSocket state is in-process
#   --loop uvloop         High-performance async loop (installed via uvicorn[standard])
#   --log-level warning   Suppress uvicorn access logs; our JSON logger handles logging
CMD ["uvicorn", "src.main:app", \
     "--host", "0.0.0.0", \
     "--port", "8080", \
     "--workers", "1", \
     "--loop", "uvloop", \
     "--log-level", "warning"]