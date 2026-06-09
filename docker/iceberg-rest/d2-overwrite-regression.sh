#!/usr/bin/env bash
# Regression guard for the D2 "INSERT OVERWRITE multi-BE hang".
#
# History: a data_runtime block_on / block_in_place starvation race made
# `INSERT OVERWRITE <iceberg>` hang under 1FE+N>=2 BE (cross-process). It stopped
# reproducing after the Iceberg write-path cutover to the transaction runner
# (#266 / #268 / #270). See the roadmap section "Iceberg Distributed Write
# Pipeline" and the note D2-insert-overwrite-multi-be-hang.
#
# This guard re-runs the cross-process (1FE+2BE) `iceberg_rest_insert_select`
# sequence (which includes INSERT OVERWRITE) under a deliberately LOW
# data_runtime worker count, so the starvation threshold stays sensitive
# (worker=2 is harsher than the 8-worker box where D2 originally reproduced).
# It exits non-zero if any run hangs (output stalls) or the case fails.
#
# Prereqs: iceberg-rest docker fixture up (docker/iceberg-rest/up.sh) and
# runtime/current generated. Tunables (env):
#   D2_GUARD_RUNS           (default 5)
#   D2_GUARD_WORKERS        (default 2)
#   D2_GUARD_QUERY_TIMEOUT  (default 60, seconds)
#   D2_GUARD_STALL_SECS     (default 30, seconds of no output => treated as hang)
set -uo pipefail
cd "$(cd "$(dirname "$0")/../.." && pwd)"
source docker/iceberg-rest/runtime/current/env.sh

RUNS=${D2_GUARD_RUNS:-5}
WORKERS=${D2_GUARD_WORKERS:-2}
QTO=${D2_GUARD_QUERY_TIMEOUT:-60}
STALL=${D2_GUARD_STALL_SECS:-30}

CFG="$(mktemp -t d2_guard_cfg).toml"
cp "$NOVAROCKS_STANDALONE_CONFIG" "$CFG"
printf '\n[runtime]\ndata_runtime_worker_threads = %s\n' "$WORKERS" >> "$CFG"
export NOVAROCKS_STANDALONE_CONFIG="$CFG"
LOG="$(mktemp -t d2_guard_log)"

RUNNER_PID=""
cleanup() { kill "${RUNNER_PID:-}" 2>/dev/null; pkill -9 -f "standalone-server --role" 2>/dev/null; rm -f "$CFG" "$LOG"; }
trap cleanup EXIT

echo "[d2-guard] runs=$RUNS data_runtime_worker_threads=$WORKERS query_timeout=${QTO}s stall=${STALL}s"
for r in $(seq 1 "$RUNS"); do
  echo "[d2-guard] === run $r/$RUNS ==="; : > "$LOG"
  NO_PROXY=127.0.0.1,localhost cargo run -q --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
    --config "$NOVAROCKS_SQL_TEST_CONFIG" \
    --suite iceberg-rest --only iceberg_rest_insert_select \
    --cluster-mode cross-process --cluster-size 2 --mode verify \
    --query-timeout "$QTO" > "$LOG" 2>&1 &
  RUNNER_PID=$!
  # wait for fe to come up (covers compile + cluster startup)
  for _ in $(seq 1 300); do
    pgrep -f "standalone-server --role fe" >/dev/null 2>&1 && break
    kill -0 "$RUNNER_PID" 2>/dev/null || break
    sleep 1
  done
  # watch for hang: runner output stalled >= STALL seconds
  while kill -0 "$RUNNER_PID" 2>/dev/null; do
    now=$(date +%s); mt=$(stat -f %m "$LOG" 2>/dev/null || echo "$now")
    if [ $((now - mt)) -ge "$STALL" ]; then
      echo "[d2-guard] run $r: output stalled >=${STALL}s -> HANG (D2 REGRESSION)"; tail -20 "$LOG"; exit 1
    fi
    sleep 2
  done
  wait "$RUNNER_PID"; rc=$?
  if [ "$rc" -ne 0 ] || ! grep -q "fail=0" "$LOG"; then
    echo "[d2-guard] run $r: runner exit=$rc / case not all-pass"; tail -20 "$LOG"; exit 1
  fi
  pkill -9 -f "standalone-server --role" 2>/dev/null; sleep 1
done
echo "[d2-guard] OK: no hang in $RUNS runs (data_runtime_worker_threads=$WORKERS)"
