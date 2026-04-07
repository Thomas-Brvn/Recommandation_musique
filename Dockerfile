FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

# Copier uv depuis son image officielle
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

WORKDIR /app

# Installer les dépendances (couche cacheable)
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

# Copier le code source
COPY src/ ./src/

# Dossier data pour la persistance de la bibliothèque (library.json)
RUN mkdir -p data

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
  CMD curl -f http://localhost:8000/health || exit 1

CMD ["uv", "run", "uvicorn", "src.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
