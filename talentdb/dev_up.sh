#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FRONTEND_DIR="$ROOT_DIR/../frontend"

API_PORT=${API_PORT:-8000}
WEB_PORT=${WEB_PORT:-5174}

printf '[dev_up] Launching stack (API:%s  WEB:%s)\n' "$API_PORT" "$WEB_PORT"

# Start API (background)
( cd "$ROOT_DIR" && OPEN_BROWSER=0 ./run_api.sh ) &
API_PID=$!

# Start frontend (background)
if [ -d "$FRONTEND_DIR" ]; then
  ( cd "$FRONTEND_DIR" && if [ ! -f node_modules/.installed ]; then
        echo '[dev_up] Installing frontend deps'
        npm install --silent && mkdir -p node_modules && touch node_modules/.installed
     fi && echo '[dev_up] Starting Vite dev server' && npx vite --port $WEB_PORT ) &
  WEB_PID=$!
else
  echo '[dev_up] Frontend directory missing, skipping'
  WEB_PID=0
fi

echo '[dev_up] Waiting for /health ...'
for i in $(seq 1 60); do
  if curl -fsS "http://localhost:${API_PORT}/health" >/dev/null 2>&1; then
    echo "[dev_up] API is up: http://localhost:${API_PORT}"
    break
  fi
  sleep 1
  if [ $i -eq 60 ]; then
    echo '[dev_up] API failed to start in time' >&2
  fi
 done

echo '[dev_up] Open URLs:'
echo "  API        http://localhost:${API_PORT}/health"
echo "  Recommend  http://localhost:${API_PORT}/recommend.html"
echo "  Frontend   http://localhost:${WEB_PORT} (Vite proxy)"

echo '[dev_up] Press Ctrl+C to stop all.'
trap 'echo; echo "[dev_up] Stopping..."; kill $API_PID $WEB_PID 2>/dev/null || true; exit 0' INT TERM
while true; do sleep 5; done
