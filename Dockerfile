FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY api.py aggregator.py resolver.py verifier.py seasons.py cache.py ./
COPY sources/ ./sources/

# Data directory for Fribb JSON (downloaded at startup)
RUN mkdir -p /app/data
COPY data/.gitignore ./data/.gitignore

# Environment
ENV PYTHONUNBUFFERED=1
ENV PORT=8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD curl -f http://localhost:${PORT}/health || exit 1

EXPOSE 8000

CMD ["sh", "-c", "uvicorn api:app --host 0.0.0.0 --port ${PORT:-8000}"]
