#!/usr/bin/env bash
set -euo pipefail
BUILD=${1:-}
if [ ! -f ".env" ]; then
  echo "Falta .env. Copia .env.example a .env y edítalo."; exit 1
fi
mkdir -p data/postgres data/redis data/caddy data/caddy_config
if [ "$BUILD" = "--build" ]; then
  docker compose build --no-cache
fi
docker compose up -d
docker compose ps
docker compose logs -f api
