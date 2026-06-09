#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RESULT_DIR="${RESULT_DIR:-$ROOT_DIR/bench_results}"
NOVAIO_BIN="${NOVAIO_BIN:-$ROOT_DIR/build/http_server}"
NODE_SERVER="${NODE_SERVER:-$ROOT_DIR/scripts/bench/node_http_server.js}"

detect_physical_cores() {
  local cores
  if command -v lscpu >/dev/null 2>&1; then
    cores="$(lscpu -p=Core,Socket 2>/dev/null | awk -F, '!/^#/ {print $1 "," $2}' | sort -u | wc -l)"
    if [[ "$cores" =~ ^[0-9]+$ ]] && (( cores > 0 )); then
      echo "$cores"
      return
    fi
  fi
  nproc
}

PHYSICAL_CORES="${PHYSICAL_CORES:-$(detect_physical_cores)}"
SERVER_WORKERS="${SERVER_WORKERS:-$PHYSICAL_CORES}"
WRK_THREADS="${WRK_THREADS:-$SERVER_WORKERS}"
WRK_CONNECTIONS="${WRK_CONNECTIONS:-$((SERVER_WORKERS * 64))}"
WRK_DURATION="${WRK_DURATION:-10s}"
NOVAIO_PORT="${NOVAIO_PORT:-8080}"
NODE_PORT="${NODE_PORT:-8081}"
NODE_WORKERS="${NODE_WORKERS:-$SERVER_WORKERS}"

mkdir -p "$RESULT_DIR"

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  cat <<EOF
Usage: ./scripts/bench/wrk_compare.sh

Environment overrides:
  PHYSICAL_CORES   Default: auto-detected physical core count
  SERVER_WORKERS   Default: PHYSICAL_CORES
  WRK_THREADS      Default: SERVER_WORKERS
  WRK_CONNECTIONS  Default: SERVER_WORKERS * 64
  WRK_DURATION     Default: 10s
  NODE_WORKERS     Default: SERVER_WORKERS
  NOVAIO_BIN       Default: build/http_server
  RESULT_DIR       Default: bench_results
EOF
  exit 0
fi

need_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing command: $1" >&2
    exit 1
  fi
}

wait_port() {
  local port="$1"
  python3 - "$port" <<'PY'
import socket
import sys
import time

port = int(sys.argv[1])
deadline = time.time() + 8
while time.time() < deadline:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.2):
            sys.exit(0)
    except OSError:
        time.sleep(0.1)
print(f"port {port} did not become ready", file=sys.stderr)
sys.exit(1)
PY
}

run_wrk() {
  local name="$1"
  local port="$2"
  local output="$RESULT_DIR/${name}_wrk.txt"

  echo "== $name =="
  wrk -t"$WRK_THREADS" -c"$WRK_CONNECTIONS" -d"$WRK_DURATION" "http://127.0.0.1:$port/" | tee "$output"
  echo
}

cleanup_pid() {
  local pid="${1:-}"
  if [[ -n "$pid" ]] && kill -0 "$pid" >/dev/null 2>&1; then
    kill "$pid" >/dev/null 2>&1 || true
    wait "$pid" >/dev/null 2>&1 || true
  fi
}

need_cmd wrk
need_cmd node
need_cmd python3

if [[ ! -x "$NOVAIO_BIN" ]]; then
  echo "NovaIO server binary not executable: $NOVAIO_BIN" >&2
  exit 1
fi

echo "Benchmark parameters:"
echo "  physical cores  : $PHYSICAL_CORES"
echo "  server workers  : $SERVER_WORKERS"
echo "  wrk threads     : $WRK_THREADS"
echo "  wrk connections : $WRK_CONNECTIONS"
echo "  wrk duration    : $WRK_DURATION"
echo "  node workers    : $NODE_WORKERS"
echo "  result dir      : $RESULT_DIR"
echo

novaio_pid=""
node_pid=""
trap 'cleanup_pid "$novaio_pid"; cleanup_pid "$node_pid"' EXIT

NOVAIO_CORES="$SERVER_WORKERS" NOVAIO_LISTENERS="$SERVER_WORKERS" "$NOVAIO_BIN" >"$RESULT_DIR/novaio_server.log" 2>&1 &
novaio_pid="$!"
wait_port "$NOVAIO_PORT"
run_wrk "novaio" "$NOVAIO_PORT"
cleanup_pid "$novaio_pid"
novaio_pid=""

PORT="$NODE_PORT" WORKERS="$NODE_WORKERS" node "$NODE_SERVER" >"$RESULT_DIR/node_server.log" 2>&1 &
node_pid="$!"
wait_port "$NODE_PORT"
run_wrk "node_libuv" "$NODE_PORT"
cleanup_pid "$node_pid"
node_pid=""

echo "Raw results written to $RESULT_DIR"
