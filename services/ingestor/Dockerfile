# ─── Stage 1: Builder ─────────────────────────────────────────────────────────
FROM python:3.13-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /build

# confluent-kafka requires librdkafka C library — install build deps here only
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        g++ \
        librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --upgrade pip && \
    pip install --prefix=/install -r requirements.txt


# ─── Stage 2: Runtime ─────────────────────────────────────────────────────────
FROM python:3.13-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

# Runtime dependency for confluent-kafka (librdkafka shared library)
RUN apt-get update && apt-get install -y --no-install-recommends \
        librdkafka1 \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd --gid 1001 appgroup && \
    useradd --uid 1001 --gid appgroup --shell /bin/false --no-create-home appuser

WORKDIR /app

COPY --from=builder /install /usr/local
COPY src/ ./src/

RUN chown -R appuser:appgroup /app

USER appuser

# Render injects PORT at runtime. Default 8080 matches Render's expected port.
EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:${PORT:-8080}/ping')"

CMD ["uvicorn", "src.main:app", \
     "--host", "0.0.0.0", \
     "--port", "8080", \
     "--workers", "1", \
     "--loop", "uvloop", \
     "--log-level", "warning"]