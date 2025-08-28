#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "[run_api] Working directory: $PWD"

# --- Virtualenv bootstrap (idempotent) ---
if command -v python >/dev/null 2>&1; then
  PY=python
elif command -v python3 >/dev/null 2>&1; then
  PY=python3
else
  echo "[run_api] ERROR: python not found in PATH" >&2
  exit 127
fi

if [ ! -d .venv ]; then
  echo "[run_api] Creating virtualenv (.venv) with $PY"
  $PY -m venv .venv || { echo "[run_api] Failed to create venv" >&2; exit 1; }
fi
source .venv/bin/activate || { echo "[run_api] Failed to activate venv" >&2; exit 1; }
if [ ! -f .venv/.deps_installed ]; then
  echo "[run_api] Installing Python dependencies" 
  $PY -m pip install -q --upgrade pip
  $PY -m pip install -q -r requirements.txt || { echo "[run_api] Failed to install requirements" >&2; exit 1; }
  touch .venv/.deps_installed
fi

# Load .env (if exists) for secrets
if [ -f .env ]; then
  set -o allexport
  . ./.env
  set +o allexport
  echo "[run_api] Loaded .env variables"
fi

# --- Mongo readiness ---
: "${MONGO_URI:=mongodb://localhost:27017}"
export MONGO_URI
: "${DB_NAME:=talent_match}"
export DB_NAME
TRIES=${MONGO_WAIT_TRIES:-40}
SLEEP=${MONGO_WAIT_SLEEP:-1}
echo "[run_api] Waiting for Mongo $MONGO_URI (tries=$TRIES sleep=${SLEEP}s) ..."
for i in $(seq 1 $TRIES); do
  if python - <<'PY'
import os,sys
from pymongo import MongoClient
try:
    c=MongoClient(os.environ['MONGO_URI'], serverSelectionTimeoutMS=800)
    c.admin.command('ping')
    print('OK')
    sys.exit(0)
except Exception as e:
    print('WAIT', e)
    sys.exit(1)
PY
  then
    break
  fi
  if [ "$i" -eq "$TRIES" ]; then
    echo "[run_api] Mongo not reachable after $TRIES attempts" >&2
    exit 1
  fi
  sleep "$SLEEP"
done

# --- Environment summary ---
echo "[run_api] OPENAI_MODEL=${OPENAI_MODEL:-gpt-5-nano} (override with export OPENAI_MODEL=...)"
echo "[run_api] OPENAI_MODEL_INGEST=${OPENAI_MODEL_INGEST:-gpt-4o-mini} (set to change CV/job ingestion model)"
echo "[run_api] PYTHONPATH will include: $SCRIPT_DIR"
export PYTHONPATH="$SCRIPT_DIR:${PYTHONPATH:-}"

# Allow long LLM operations (default 300s already handled in code)
export OPENAI_REQUEST_TIMEOUT="${OPENAI_REQUEST_TIMEOUT:-300}"
export OPENAI_OVERALL_TIMEOUT="${OPENAI_OVERALL_TIMEOUT:-320}"

RELOAD_FLAG=""
if [ "${DEV_RELOAD:-}" = "1" ]; then
  RELOAD_FLAG="--reload"
  echo "[run_api] Dev reload enabled"
fi

PORT=${API_PORT:-8080}
HOST=${API_HOST:-0.0.0.0}
echo "[run_api] Starting API on http://${HOST}:${PORT}" 
echo "[run_api] Health:   http://localhost:${PORT}/health"
echo "[run_api] Ready:    http://localhost:${PORT}/ready"
echo "[run_api] Letters:  POST /personal-letter  GET /personal-letter/{share_id}"

if command -v open >/dev/null 2>&1 && [ "${OPEN_BROWSER:-1}" = "1" ]; then
  ( sleep 2 && open "http://localhost:${PORT}/" >/dev/null 2>&1 || true ) &
  ( sleep 3 && open "http://localhost:${PORT}/recommend.html" >/dev/null 2>&1 || true ) &
fi

exec $PY -m uvicorn scripts.api:app --host "$HOST" --port "$PORT" $RELOAD_FLAG
