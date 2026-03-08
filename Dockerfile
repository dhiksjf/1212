# ══════════════════════════════════════════════════════════════════════════════
#  PyDeploy — Dockerfile
#  Production-ready image for Koyeb Docker deployment.
#
#  Usage on Koyeb:
#    - Select "Docker" deployment
#    - Set environment variables (SUPABASE_URL, SUPABASE_KEY, etc.)
#    - Koyeb will automatically use the $PORT env var
# ══════════════════════════════════════════════════════════════════════════════

FROM python:3.11-slim

# ── Metadata ────────────────────────────────────────────────────────────────
LABEL maintainer="PyDeploy"
LABEL description="Mini Python deployment platform — Koyeb optimized"
LABEL version="1.0.0"

# ── System dependencies ──────────────────────────────────────────────────────
# Install build tools, Node.js (for frontend builds), and other system deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    # Build essentials (for compiling Python packages like psycopg2)
    build-essential \
    gcc \
    g++ \
    # PostgreSQL client libs (for psycopg2)
    libpq-dev \
    # Process utilities
    procps \
    # Networking tools
    curl \
    wget \
    # Git (some pip packages need it)
    git \
    # Node.js & npm (for building React/Next.js frontends in deployed apps)
    nodejs \
    npm \
    # Zip/unzip utilities
    zip \
    unzip \
    # Clean up apt cache to reduce image size
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# ── Install yarn & pnpm globally for frontend builds ─────────────────────────
RUN npm install -g yarn pnpm --quiet

# ── Create non-root user for security ────────────────────────────────────────
RUN groupadd -r pydeploy && useradd -r -g pydeploy -m -d /home/pydeploy pydeploy

# ── Set working directory ────────────────────────────────────────────────────
WORKDIR /app

# ── Copy requirements first (Docker cache optimization) ──────────────────────
COPY requirements.txt .

# ── Install Python dependencies ───────────────────────────────────────────────
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# ── Copy application source ───────────────────────────────────────────────────
COPY app.py .

# ── Create runtime directories with correct permissions ──────────────────────
RUN mkdir -p /tmp/pydeploy_apps \
    && chown -R pydeploy:pydeploy /tmp/pydeploy_apps \
    && chown -R pydeploy:pydeploy /app

# ── Switch to non-root user ───────────────────────────────────────────────────
# Note: deploying child processes may require root in some environments.
# Comment this out if you encounter permission issues on Koyeb.
# USER pydeploy

# ── Environment variable defaults ─────────────────────────────────────────────
# These are overridden by Koyeb environment variables at runtime.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app \
    PORT=8000 \
    BASE_DEPLOY_DIR=/tmp/pydeploy_apps \
    BASE_APP_PORT=9000 \
    MAX_ZIP_SIZE_MB=100 \
    MAX_UNZIPPED_SIZE_MB=500 \
    MAX_CONCURRENT_DEPLOYMENTS=10

# ── Health check ──────────────────────────────────────────────────────────────
# Koyeb uses this to determine if the service is healthy
HEALTHCHECK --interval=30s --timeout=10s --start-period=20s --retries=3 \
    CMD curl -f http://localhost:${PORT}/api/health || exit 1

# ── Expose port ───────────────────────────────────────────────────────────────
# Koyeb maps this automatically via the $PORT env var
EXPOSE ${PORT}

# ── Start command ─────────────────────────────────────────────────────────────
# Use shell form so $PORT is evaluated at runtime from Koyeb's env vars.
# Single worker is intentional: the deployment engine uses in-memory state.
CMD uvicorn app:app \
    --host 0.0.0.0 \
    --port ${PORT} \
    --workers 1 \
    --log-level info \
    --access-log \
    --no-use-colors
