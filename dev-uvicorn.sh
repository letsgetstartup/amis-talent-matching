#!/usr/bin/env bash
# Dev helper to start/stop/restart uvicorn with env and venv.
# Usage:
#   ./dev-uvicorn.sh start    # start in background
#   ./dev-uvicorn.sh stop     # stop background server
#   ./dev-uvicorn.sh restart  # restart background server
#   ./dev-uvicorn.sh status   # show status
#   ./dev-uvicorn.sh logs     # tail logs
#
# Env vars (optional):
#   VENV=./.venv                    # path to python venv
#   HOST=0.0.0.0                    # host to bind
#   PORT=8000                       # port to bind
#   LOG_FILE=./server.out           # log file path
#   RELOAD=1                        # use --reload (default 1)
#   WORKERS=1                       # uvicorn workers when RELOAD=0
#
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV_BIN="${VENV:-$ROOT_DIR/.venv}/bin"
PYTHON_BIN="$VENV_BIN/python"
UVICORN_BIN="$VENV_BIN/uvicorn"
HOST="${HOST:-0.0.0.0}"
PORT="${PORT:-8000}"
LOG_FILE="${LOG_FILE:-$ROOT_DIR/server.out}"
PID_FILE="$ROOT_DIR/.uvicorn.pid"
RELOAD="${RELOAD:-1}"
WORKERS="${WORKERS:-1}"

# Export variables from .env if present
if [ -f "$ROOT_DIR/.env" ]; then
  set +u
  set -a
  # shellcheck source=/dev/null
  . "$ROOT_DIR/.env"
  set +a
  set -u
fi

need_tools() {
  if [ ! -x "$PYTHON_BIN" ]; then
    echo "Python venv not found at $PYTHON_BIN" >&2
    echo "Create it and install deps: python3 -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt" >&2
    exit 1
  fi
  if [ ! -x "$UVICORN_BIN" ]; then
    echo "uvicorn not found in venv. Installing..." >&2
    "$PYTHON_BIN" -m pip install -q uvicorn[standard] watchfiles || {
      echo "Failed to install uvicorn" >&2; exit 1; }
  fi
}

pids() {
  pgrep -f "uvicorn .*scripts.api:app" || true
}

is_running() {
  if [ -f "$PID_FILE" ] && ps -p "$(cat "$PID_FILE" 2>/dev/null)" >/dev/null 2>&1; then
    return 0
  fi
  # fallback by pattern
  [ -n "$(pids)" ]
}

start() {
  need_tools
  if is_running; then
    echo "Server already running (PID $(cat "$PID_FILE" 2>/dev/null || pids))"
    exit 0
  fi
  echo "Starting uvicorn on $HOST:$PORT ..."
  mkdir -p "$(dirname "$LOG_FILE")"
  # Use --app-dir to include talentdb on sys.path
  # If RELOAD=1, use reload watcher (single worker); else allow multiple workers
  if [ "$RELOAD" = "1" ]; then
      nohup "$UVICORN_BIN" --app-dir "$ROOT_DIR/talentdb" scripts.api:app \
        --host "$HOST" --port "$PORT" --reload \
        --reload-dir "$ROOT_DIR/talentdb" \
        --reload-exclude 'server.out' \
        --reload-exclude '*.log' \
        --reload-exclude 'mongo_backups' \
        --reload-exclude 'frontend' \
        --reload-exclude 'docs' \
        --reload-exclude 'backup' \
        --reload-exclude '*.csv' \
        --reload-exclude '*.json' >> "$LOG_FILE" 2>&1 &
  else
    nohup "$UVICORN_BIN" --app-dir "$ROOT_DIR/talentdb" scripts.api:app \
      --host "$HOST" --port "$PORT" --workers "$WORKERS" >> "$LOG_FILE" 2>&1 &
  fi
  echo $! > "$PID_FILE"
  sleep 0.4
  if is_running; then
    echo "Started (PID $(cat "$PID_FILE")) | Logs: $LOG_FILE"
  else
    echo "Failed to start. See logs: $LOG_FILE" >&2
    exit 1
  fi
}

stop() {
  if is_running; then
    PID="$(cat "$PID_FILE" 2>/dev/null || true)"
    if [ -n "$PID" ] && kill -0 "$PID" 2>/dev/null; then
      echo "Stopping PID $PID ..."
      kill "$PID" || true
      sleep 0.5
    fi
  fi
  # Ensure no stray uvicorn for this app
  for p in $(pids); do
    echo "Killing stray uvicorn PID $p ..."
    kill "$p" || true
  done
  rm -f "$PID_FILE"
  echo "Stopped"
}

restart() {
  stop
  start
}

status() {
  if is_running; then
    echo "Server running (PID $(cat "$PID_FILE" 2>/dev/null || pids)) on $HOST:$PORT"
  else
    echo "Server not running"
    exit 1
  fi
}

logs() {
  touch "$LOG_FILE"
  echo "Tailing $LOG_FILE (Ctrl-C to stop)"
  tail -n 200 -f "$LOG_FILE"
}

case "${1:-}" in
  start) start ;;
  stop) stop ;;
  restart) restart ;;
  status) status ;;
  logs) logs ;;
  *) echo "Usage: $0 {start|stop|restart|status|logs}"; exit 2 ;;
 esac
