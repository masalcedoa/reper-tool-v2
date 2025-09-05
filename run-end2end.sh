#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# run-end2end.sh  —  Subir META y CONSUMOS, obtener job_id, esperar finalización
#                    y consultar conteos en PostgreSQL.
#
# Uso:
#   chmod +x run-end2end.sh
#   ./run-end2end.sh #       --meta /opt/fraude-app/reper-tool-v2/uploads/meta_META.csv #       --consumos /opt/fraude-app/reper-tool-v2/uploads/basefinal_filtrada.csv #       --base http://127.0.0.1:8000
#
# Opcionales:
#   --db-host 127.0.0.1 --db-port 5432 --db-user postgres --db-pass postgres --db-name frauddb
#   --timeout-secs 900   (máximo tiempo de espera por el job, por defecto 900s)
#
# Requisitos: curl, (recomendado) jq, y psql si quieres hacer las consultas.
# =============================================================================

META=""
CONSUMOS=""
BASE="${BASE:-http://127.0.0.1:8000}"
DB_HOST="${DB_HOST:-127.0.0.1}"
DB_PORT="${DB_PORT:-5432}"
DB_USER="${DB_USER:-postgres}"
DB_PASSWORD="${DB_PASSWORD:-postgres}"
DB_NAME="${DB_NAME:-frauddb}"
TIMEOUT_SECS=900

while [[ $# -gt 0 ]]; do
  case "$1" in
    --meta) META="$2"; shift 2;;
    --consumos) CONSUMOS="$2"; shift 2;;
    --base) BASE="$2"; shift 2;;
    --db-host) DB_HOST="$2"; shift 2;;
    --db-port) DB_PORT="$2"; shift 2;;
    --db-user) DB_USER="$2"; shift 2;;
    --db-pass|--db-password) DB_PASSWORD="$2"; shift 2;;
    --db-name) DB_NAME="$2"; shift 2;;
    --timeout-secs) TIMEOUT_SECS="$2"; shift 2;;
    -h|--help) grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0;;
    *) echo "Parámetro desconocido: $1" >&2; exit 1;;
  esac
done

# Si existe .env en el cwd, intenta cargar por defecto (sin romper params ya dados)
if [[ -f ".env" ]]; then
  set -a
  source .env
  set +a
  DB_HOST="${DB_HOST:-127.0.0.1}"
  DB_PORT="${DB_PORT:-5432}"
  DB_USER="${DB_USER:-postgres}"
  DB_PASSWORD="${DB_PASSWORD:-postgres}"
  DB_NAME="${DB_NAME:-frauddb}"
fi

if [[ -z "${META}" || -z "${CONSUMOS}" ]]; then
  echo "Faltan rutas de archivos. Usa --meta y --consumos" >&2
  exit 1
fi
if [[ ! -f "${META}" ]]; then
  echo "No existe archivo META: ${META}" >&2; exit 1
fi
if [[ ! -f "${CONSUMOS}" ]]; then
  echo "No existe archivo CONSUMOS: ${CONSUMOS}" >&2; exit 1
fi

echo ">>> BASE = ${BASE}"
echo ">>> META = ${META}"
echo ">>> CONSUMOS = ${CONSUMOS}"
echo ">>> DB = ${DB_USER}@${DB_HOST}:${DB_PORT}/${DB_NAME}"
echo

echo "1) Health check..."
set +e
HC=$(curl -sS "${BASE}/health")
RC=$?
set -e
if [[ $RC -ne 0 ]]; then
  echo "Warning: /health no respondió con 200 (curl rc=${RC}). Continuamos..." >&2
else
  echo "Health => ${HC}"
fi
echo

echo "2) Subiendo META..."
curl -sS -X POST -F "file=@${META}" "${BASE}/meta/upload"
echo; echo

echo "3) Subiendo CONSUMOS..."
RESP=$(curl -sS -X POST -F "file=@${CONSUMOS}" "${BASE}/ingest/upload" || true)
echo "${RESP}"
echo

JOB_ID=""
if command -v jq >/dev/null 2>&1; then
  JOB_ID=$(echo "$RESP" | jq -r '.job_id // empty')
fi
if [[ -z "${JOB_ID}" ]] && command -v python3 >/dev/null 2>&1; then
  JOB_ID=$(python3 - <<'PY' "$RESP"
import sys, json
try:
  print(json.loads(sys.argv[1]).get("job_id",""))
except Exception:
  pass
PY
)
fi
if [[ -z "${JOB_ID}" ]]; then
  JOB_ID=$(echo "$RESP" | grep -oE '[0-9a-fA-F-]{36}' | head -n1 || true)
fi
if [[ -z "${JOB_ID}" ]]; then
  echo "No se pudo obtener job_id de la respuesta. Se intentará tomar el último job..." >&2
  if command -v jq >/dev/null 2>&1; then
    JOB_ID=$(curl -sS "${BASE}/jobs" | jq -r '.[0].job_id // empty')
  else
    JOB_ID=$(curl -sS "${BASE}/jobs" | grep -oE '[0-9a-fA-F-]{36}' | head -n1 || true)
  fi
fi
if [[ -z "${JOB_ID}" ]]; then
  echo "ERROR: No se pudo determinar el job_id. Aborta." >&2
  exit 2
fi
echo "JOB_ID = ${JOB_ID}"
echo

echo "4) Esperando a que termine el job (timeout: ${TIMEOUT_SECS}s)..."
START=$(date +%s)
LAST_STATUS=""
while true; do
  OUT=$(curl -sS "${BASE}/jobs/${JOB_ID}" || true)
  echo "$OUT"
  if command -v jq >/dev/null 2>&1; then
    STATUS=$(echo "$OUT" | jq -r '.status // .state // .pipeline_status // .job.status // empty' | tr '[:upper:]' '[:lower:]')
  else
    STATUS=$(echo "$OUT" | grep -oEi '"(status|state|pipeline_status)"\s*:\s*"[^"]+"' | head -n1 | sed -E 's/.*:"([^"]+)".*/\1/i' | tr '[:upper:]' '[:lower:]')
  fi

  if [[ "$STATUS" == "done" || "$STATUS" == "completed" || "$STATUS" == "success" || "$STATUS" == "succeeded" ]]; then
    echo "Job finalizado con estado: $STATUS"
    break
  fi
  if echo "$OUT" | grep -qi '"failed"\|"error"\|"exception"'; then
    echo "El job reporta error/failed. Revisa logs del worker." >&2
    break
  fi
  NOW=$(date +%s)
  ELAPSED=$((NOW - START))
  if (( ELAPSED > TIMEOUT_SECS )); then
    echo "Timeout esperando al job (${TIMEOUT_SECS}s)." >&2
    break
  fi
  sleep 5
done
echo

if command -v psql >/dev/null 2>&1; then
  echo "5) Consultas a BD (psql)"
  export PGPASSWORD="${DB_PASSWORD}"
  psql "host=${DB_HOST} port=${DB_PORT} user=${DB_USER} dbname=${DB_NAME}" -c "SELECT COUNT(*) AS stg_consumo FROM stg_consumo;"
  psql "host=${DB_HOST} port=${DB_PORT} user=${DB_USER} dbname=${DB_NAME}" -c "SELECT COUNT(*) AS features_curvas FROM features_curvas;"
  psql "host=${DB_HOST} port=${DB_PORT} user=${DB_USER} dbname=${DB_NAME}" -c "SELECT COUNT(*) AS resultados FROM resultados WHERE job_id = '${JOB_ID}';"
  psql "host=${DB_HOST} port=${DB_PORT} user=${DB_USER} dbname=${DB_NAME}" -c "
    SELECT cuenta,
           ROUND(COALESCE(score_supervisado,0)::numeric,4) AS score_sup,
           ROUND(COALESCE(score_hibrido,0)::numeric,4)     AS score_hib,
           umbral_aplicado, decision
    FROM resultados
    WHERE job_id = '${JOB_ID}'
    ORDER BY score_hibrido DESC NULLS LAST
    LIMIT 10;"
else
  echo "psql no encontrado, omitiendo consultas a BD. Para instalar: sudo apt-get install -y postgresql-client" >&2
fi

echo "Fin."
