#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FIXTURE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
exports_file="$FIXTURE_DIR/runtime/current/env.sh"
requested_database="${NOVAROCKS_MYSQL_DATABASE:-}"
if [[ ! -f "$exports_file" ]]; then
  echo "MySQL state-store environment is not initialized" >&2
  exit 1
fi
# shellcheck disable=SC1090
source "$exports_file"
readiness_database="$NOVAROCKS_MYSQL_DATABASE"
if [[ -z "$requested_database" ]]; then
  echo "physical probes require a caller-provisioned database override" >&2
  exit 2
fi
if [[ "$requested_database" = "$readiness_database" ]]; then
  echo "physical probes must not use the shared readiness database" >&2
  exit 2
fi
NOVAROCKS_MYSQL_DATABASE="$requested_database"
if [[ ! "$NOVAROCKS_MYSQL_DATABASE" =~ ^novarocks_ss3_[A-Za-z0-9_]{1,49}$ ]]; then
  echo "physical probes require a unique novarocks_ss3_ database" >&2
  exit 2
fi
echo "Physical probe database: $NOVAROCKS_MYSQL_DATABASE"

run_with_timeout() {
  local timeout_seconds="$1"
  shift
  set -m
  "$@" &
  local child="$!"
  set +m
  local elapsed=0
  while kill -0 "$child" >/dev/null 2>&1; do
    if (( elapsed >= timeout_seconds )); then
      kill -TERM -- "-$child" >/dev/null 2>&1 || true
      sleep 1
      kill -KILL -- "-$child" >/dev/null 2>&1 || true
      wait "$child" 2>/dev/null || true
      echo "MySQL physical probe command timed out" >&2
      return 124
    fi
    sleep 1
    elapsed=$((elapsed + 1))
  done
  wait "$child"
}

mysql_client() {
  run_with_timeout 25 docker compose \
    --env-file "$NOVA_MYSQL_COMPOSE_ENV" \
    -p "$NOVA_MYSQL_COMPOSE_PROJECT" \
    -f "$NOVA_MYSQL_COMPOSE_FILE" \
    exec -T mysql mysql \
    --defaults-extra-file=/run/secrets/novarocks-mysql-provider.cnf \
    --database="$NOVAROCKS_MYSQL_DATABASE" \
    --batch --skip-column-names --unbuffered "$@"
}

tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/novarocks-ss3-probes.XXXXXX")"
background_pids=()
cleanup() {
  local original_status="$?"
  local pid
  local cleanup_deadline
  trap - EXIT
  for pid in "${background_pids[@]}"; do
    if [[ -n "$pid" ]] && kill -0 "$pid" >/dev/null 2>&1; then
      kill -TERM -- "-$pid" >/dev/null 2>&1 || true
    fi
  done
  cleanup_deadline=$((SECONDS + 5))
  while (( SECONDS < cleanup_deadline )); do
    local running=false
    for pid in "${background_pids[@]}"; do
      if [[ -n "$pid" ]] && kill -0 "$pid" >/dev/null 2>&1; then
        running=true
        break
      fi
    done
    if [[ "$running" == false ]]; then
      break
    fi
    sleep 0.1
  done
  for pid in "${background_pids[@]}"; do
    if [[ -n "$pid" ]] && kill -0 "$pid" >/dev/null 2>&1; then
      kill -KILL -- "-$pid" >/dev/null 2>&1 || true
    fi
  done
  for pid in "${background_pids[@]}"; do
    if [[ -n "$pid" ]]; then
      wait "$pid" 2>/dev/null || true
    fi
  done
  if ! run_with_timeout 10 rm -rf "$tmp_dir"; then
    echo "failed to remove MySQL physical probe temporary directory" >&2
    exit 1
  fi
  exit "$original_status"
}
trap cleanup EXIT

forget_background_pid() {
  local completed_pid="$1"
  local retained=()
  local pid
  for pid in "${background_pids[@]}"; do
    if [[ "$pid" != "$completed_pid" ]]; then
      retained+=("$pid")
    fi
  done
  background_pids=("${retained[@]}")
}

wait_for_marker() {
  local file="$1"
  local marker="$2"
  local deadline=$((SECONDS + 15))
  while (( SECONDS < deadline )); do
    if grep -Fx "$marker" "$file" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.1
  done
  echo "timed out waiting for probe barrier: $marker" >&2
  return 1
}

barrier_prefix="ss3_${BASHPID}"
gate_pid=""
gate_connection_id=""
start_gate() {
  local gate_name="$1"
  local gate_label="$2"
  set -m
  (
    set +e
    mysql_client --execute="
      SELECT CONCAT('gate_connection=', CONNECTION_ID());
      SELECT CONCAT('gate_ready=', GET_LOCK('${gate_name}', 0));
      SELECT SLEEP(30);
    " >"$tmp_dir/${gate_label}.out" 2>"$tmp_dir/${gate_label}.err"
    printf '%s\n' "$?" > "$tmp_dir/${gate_label}.rc"
  ) &
  gate_pid="$!"
  set +m
  background_pids+=("$gate_pid")
  wait_for_marker "$tmp_dir/${gate_label}.out" "gate_ready=1"
  gate_connection_id="$(sed -n 's/^gate_connection=//p' "$tmp_dir/${gate_label}.out")"
  if [[ ! "$gate_connection_id" =~ ^[1-9][0-9]*$ ]]; then
    echo "failed to discover MySQL gate connection for $gate_label" >&2
    return 1
  fi
}

release_gate() {
  local gate_label="$1"
  local release_pid="${2:-$gate_pid}"
  local release_connection_id="${3:-$gate_connection_id}"
  mysql_client --execute="KILL CONNECTION ${release_connection_id};" >/dev/null
  set +e
  wait "$release_pid"
  local gate_rc="$?"
  set -e
  forget_background_pid "$release_pid"
  test "$gate_rc" -ne 124
  grep -E 'ERROR (1317|2013) ' "$tmp_dir/${gate_label}.err" >/dev/null
  if [[ "$#" -eq 1 ]]; then
    gate_pid=""
    gate_connection_id=""
  fi
}

mysql_client < "$SCRIPT_DIR/schema.sql"

database_keyword="DATABASE"
create_keyword="CREATE"
drop_keyword="DROP"
for denied_keyword in "$create_keyword" "$drop_keyword"; do
  set +e
  mysql_client --execute="${denied_keyword} ${database_keyword} novarocks_ss3_forbidden_privilege" \
    >"$tmp_dir/privilege-${denied_keyword}.out" \
    2>"$tmp_dir/privilege-${denied_keyword}.err"
  denied_rc="$?"
  set -e
  test "$denied_rc" -ne 0
  grep -E 'ERROR (1044|1045) \(42000\)' "$tmp_dir/privilege-${denied_keyword}.err" >/dev/null
done
printf 'CHECK SS3_MYSQL_PROBE_PRIVILEGE_SEPARATION_PASS\n'

mysql_client --execute="
  INSERT INTO ss3_probe_keys(key_bytes, value_bytes)
  VALUES (CAST(REPEAT('k', 3072) AS BINARY), X'01');
  SELECT OCTET_LENGTH(key_bytes) FROM ss3_probe_keys;
" | grep -Fx '3072' >/dev/null
printf 'PASS SS3_MYSQL_PROBE_KEY_3072_PASS\n'

set +e
mysql_client --execute="
  CREATE TABLE ss3_probe_key_3073 (
    key_bytes VARBINARY(3073) NOT NULL,
    PRIMARY KEY (key_bytes)
  ) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;
" >"$tmp_dir/key3073.out" 2>"$tmp_dir/key3073.err"
key3073_rc="$?"
set -e
test "$key3073_rc" -ne 0
grep -F 'ERROR 1071 (42000)' "$tmp_dir/key3073.err" >/dev/null
printf 'PASS SS3_MYSQL_PROBE_KEY_3073_ERROR_1071_PASS\n'

mysql_client --execute="
  DELETE FROM ss3_probe_keys;
  INSERT INTO ss3_probe_keys(key_bytes, value_bytes) VALUES
    (X'', X'00'),
    (X'00', X'00'),
    (X'0000', X'00'),
    (X'00FF', X'00'),
    (X'01', X'00'),
    (X'FF', X'00');
"
mysql_client --execute="
  SELECT CONCAT('[', HEX(key_bytes), ']')
  FROM ss3_probe_keys
  ORDER BY key_bytes;
" > "$tmp_dir/order.out"
diff -u <(printf '[]\n[00]\n[0000]\n[00FF]\n[01]\n[FF]\n') "$tmp_dir/order.out"
printf 'PASS SS3_MYSQL_PROBE_BINARY_ORDER_PASS\n'

mysql_client --execute="
  SELECT CONCAT('[', HEX(key_bytes), ']')
  FROM ss3_probe_keys
  WHERE key_bytes >= X'00' AND key_bytes < X'FF'
  ORDER BY key_bytes;
" > "$tmp_dir/range-forward.out"
mysql_client --execute="
  SELECT CONCAT('[', HEX(key_bytes), ']')
  FROM ss3_probe_keys
  WHERE key_bytes >= X'00' AND key_bytes < X'FF'
  ORDER BY key_bytes DESC;
" > "$tmp_dir/range-reverse.out"
diff -u <(printf '[00]\n[0000]\n[00FF]\n[01]\n') "$tmp_dir/range-forward.out"
diff -u <(printf '[01]\n[00FF]\n[0000]\n[00]\n') "$tmp_dir/range-reverse.out"
printf 'PASS SS3_MYSQL_PROBE_RANGE_FORWARD_REVERSE_PASS\n'

mysql_client --execute="
  EXPLAIN FORMAT=JSON
  SELECT key_bytes
  FROM ss3_probe_keys
  WHERE key_bytes >= X'00' AND key_bytes < X'FF'
  ORDER BY key_bytes;
" > "$tmp_dir/explain.out"
grep -E '"access_type": "range"' "$tmp_dir/explain.out" >/dev/null
grep -E '"key": "PRIMARY"' "$tmp_dir/explain.out" >/dev/null
printf 'PASS SS3_MYSQL_PROBE_PRIMARY_RANGE_EXPLAIN_PASS\n'

mysql_client --execute="
  DELETE FROM ss3_probe_snapshot;
  INSERT INTO ss3_probe_snapshot VALUES (1, 10);
"
snapshot_gate="${barrier_prefix}_snapshot_gate"
snapshot_ready="${barrier_prefix}_snapshot_ready"
start_gate "$snapshot_gate" "snapshot-gate"
set -m
(
  set +e
  mysql_client --execute="
    SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;
    START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY;
    SELECT CONCAT('before=', value_bytes) FROM ss3_probe_snapshot WHERE id = 1;
    SELECT CONCAT('snapshot_reader_ready=', GET_LOCK('${snapshot_ready}', 0));
    SELECT GET_LOCK('${snapshot_gate}', 15);
    SELECT CONCAT('after=', value_bytes) FROM ss3_probe_snapshot WHERE id = 1;
    SELECT RELEASE_LOCK('${snapshot_gate}');
    SELECT RELEASE_LOCK('${snapshot_ready}');
    COMMIT;
  " >"$tmp_dir/snapshot-reader.out" 2>"$tmp_dir/snapshot-reader.err"
  printf '%s\n' "$?" > "$tmp_dir/snapshot-reader.rc"
) &
snapshot_reader="$!"
set +m
background_pids+=("$snapshot_reader")
wait_for_marker "$tmp_dir/snapshot-reader.out" "snapshot_reader_ready=1"
mysql_client --execute="UPDATE ss3_probe_snapshot SET value_bytes = 20 WHERE id = 1;"
release_gate "snapshot-gate"
wait "$snapshot_reader"
forget_background_pid "$snapshot_reader"
test "$(cat "$tmp_dir/snapshot-reader.rc")" = "0"
grep -Fx 'before=10' "$tmp_dir/snapshot-reader.out" >/dev/null
grep -Fx 'after=10' "$tmp_dir/snapshot-reader.out" >/dev/null
test "$(mysql_client --execute="SELECT value_bytes FROM ss3_probe_snapshot WHERE id = 1;")" = "20"
printf 'PASS SS3_MYSQL_PROBE_RR_SNAPSHOT_PASS\n'

mysql_client --execute="DELETE FROM ss3_probe_snapshot;"
dual_gate="${barrier_prefix}_dual_gate"
start_gate "$dual_gate" "dual-gate"
for reader in one two; do
  reader_ready="${barrier_prefix}_reader_${reader}_ready"
  set -m
  (
    set +e
    mysql_client --execute="
      SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;
      START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY;
      SELECT CONCAT('observed_before=', COUNT(*))
      FROM ss3_probe_snapshot
      WHERE id >= 100 AND id < 200;
      SELECT CONCAT('reader_${reader}_ready=', GET_LOCK('${reader_ready}', 0));
      SELECT GET_LOCK('${dual_gate}', 15);
      SELECT CONCAT('observed_after=', COUNT(*))
      FROM ss3_probe_snapshot
      WHERE id >= 100 AND id < 200;
      SELECT RELEASE_LOCK('${dual_gate}');
      SELECT RELEASE_LOCK('${reader_ready}');
      COMMIT;
    " >"$tmp_dir/reader-${reader}.out" 2>"$tmp_dir/reader-${reader}.err"
    printf '%s\n' "$?" > "$tmp_dir/reader-${reader}.rc"
  ) &
  set +m
  eval "reader_${reader}=$!"
  background_pids+=("$!")
done
wait_for_marker "$tmp_dir/reader-one.out" "reader_one_ready=1"
wait_for_marker "$tmp_dir/reader-two.out" "reader_two_ready=1"
mysql_client --execute="INSERT INTO ss3_probe_snapshot VALUES (100, 100);"
release_gate "dual-gate"
wait "$reader_one"
forget_background_pid "$reader_one"
wait "$reader_two"
forget_background_pid "$reader_two"
for reader in one two; do
  test "$(cat "$tmp_dir/reader-${reader}.rc")" = "0"
  grep -Fx 'observed_before=0' "$tmp_dir/reader-${reader}.out" >/dev/null
  grep -Fx 'observed_after=0' "$tmp_dir/reader-${reader}.out" >/dev/null
done
test "$(mysql_client --execute="SELECT COUNT(*) FROM ss3_probe_snapshot WHERE id = 100;")" = "1"
printf 'PASS SS3_MYSQL_PROBE_RR_DUAL_NONLOCKING_READERS_PASS\n'

mysql_client --execute="
  DELETE FROM ss3_probe_locks;
  INSERT INTO ss3_probe_locks VALUES (1, 0), (2, 0), (3, 0), (4, 0);
"
deadlock_a_gate="${barrier_prefix}_deadlock_a_gate"
deadlock_b_gate="${barrier_prefix}_deadlock_b_gate"
deadlock_a_ready="${barrier_prefix}_deadlock_a_ready"
deadlock_b_ready="${barrier_prefix}_deadlock_b_ready"
start_gate "$deadlock_a_gate" "deadlock-a-gate"
deadlock_a_gate_pid="$gate_pid"
deadlock_a_gate_connection_id="$gate_connection_id"
start_gate "$deadlock_b_gate" "deadlock-b-gate"
deadlock_b_gate_pid="$gate_pid"
deadlock_b_gate_connection_id="$gate_connection_id"
set -m
(
  set +e
  mysql_client --execute="
    SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;
    START TRANSACTION;
    UPDATE ss3_probe_locks SET value_bytes = value_bytes + 1 WHERE id = 1;
    SELECT CONCAT('deadlock_a_ready=', GET_LOCK('${deadlock_a_ready}', 0));
    SELECT GET_LOCK('${deadlock_a_gate}', 15);
    UPDATE ss3_probe_locks SET value_bytes = value_bytes + 1 WHERE id = 2;
    SELECT RELEASE_LOCK('${deadlock_a_gate}');
    SELECT RELEASE_LOCK('${deadlock_a_ready}');
    COMMIT;
  " >"$tmp_dir/deadlock-a.out" 2>"$tmp_dir/deadlock-a.err"
  printf '%s\n' "$?" > "$tmp_dir/deadlock-a.rc"
) &
deadlock_a="$!"
set +m
background_pids+=("$deadlock_a")
set -m
(
  set +e
  mysql_client --execute="
    SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;
    START TRANSACTION;
    UPDATE ss3_probe_locks SET value_bytes = value_bytes + 1 WHERE id = 2;
    SELECT CONCAT('deadlock_b_ready=', GET_LOCK('${deadlock_b_ready}', 0));
    SELECT GET_LOCK('${deadlock_b_gate}', 15);
    UPDATE ss3_probe_locks SET value_bytes = value_bytes + 1 WHERE id = 1;
    SELECT RELEASE_LOCK('${deadlock_b_gate}');
    SELECT RELEASE_LOCK('${deadlock_b_ready}');
    COMMIT;
  " >"$tmp_dir/deadlock-b.out" 2>"$tmp_dir/deadlock-b.err"
  printf '%s\n' "$?" > "$tmp_dir/deadlock-b.rc"
) &
deadlock_b="$!"
set +m
background_pids+=("$deadlock_b")
wait_for_marker "$tmp_dir/deadlock-a.out" "deadlock_a_ready=1"
wait_for_marker "$tmp_dir/deadlock-b.out" "deadlock_b_ready=1"
release_gate \
  "deadlock-a-gate" \
  "$deadlock_a_gate_pid" \
  "$deadlock_a_gate_connection_id"
release_gate \
  "deadlock-b-gate" \
  "$deadlock_b_gate_pid" \
  "$deadlock_b_gate_connection_id"
wait "$deadlock_a"
forget_background_pid "$deadlock_a"
wait "$deadlock_b"
forget_background_pid "$deadlock_b"
deadlock_a_rc="$(cat "$tmp_dir/deadlock-a.rc")"
deadlock_b_rc="$(cat "$tmp_dir/deadlock-b.rc")"
if [[ "$deadlock_a_rc" = "0" ]]; then
  test "$deadlock_b_rc" -ne 0
else
  test "$deadlock_b_rc" = "0"
fi
deadlock_1213_count="$(
  cat "$tmp_dir/deadlock-a.err" "$tmp_dir/deadlock-b.err" \
    | grep -c 'ERROR 1213 (40001)' || true
)"
test "$deadlock_1213_count" = "1"
printf 'PASS SS3_MYSQL_PROBE_DEADLOCK_1213_PASS\n'

lock_gate="${barrier_prefix}_lock_gate"
lock_holder_ready="${barrier_prefix}_lock_holder_ready"
start_gate "$lock_gate" "lock-gate"
set -m
(
  set +e
  mysql_client --execute="
    START TRANSACTION;
    UPDATE ss3_probe_locks SET value_bytes = value_bytes + 1 WHERE id = 3;
    SELECT CONCAT('lock_holder_ready=', GET_LOCK('${lock_holder_ready}', 0));
    SELECT GET_LOCK('${lock_gate}', 15);
    SELECT RELEASE_LOCK('${lock_gate}');
    SELECT RELEASE_LOCK('${lock_holder_ready}');
    COMMIT;
  " >"$tmp_dir/lock-holder.out" 2>"$tmp_dir/lock-holder.err"
  printf '%s\n' "$?" > "$tmp_dir/lock-holder.rc"
) &
lock_holder="$!"
set +m
background_pids+=("$lock_holder")
wait_for_marker "$tmp_dir/lock-holder.out" "lock_holder_ready=1"
set +e
printf '%s\n' \
  'SET SESSION innodb_lock_wait_timeout = 1;' \
  'START TRANSACTION;' \
  'UPDATE ss3_probe_locks SET value_bytes = 7 WHERE id = 4;' \
  'UPDATE ss3_probe_locks SET value_bytes = value_bytes + 1 WHERE id = 3;' \
  "SELECT CONCAT('prior_write_visible_after_timeout=', value_bytes) FROM ss3_probe_locks WHERE id = 4;" \
  'ROLLBACK;' \
  "SELECT CONCAT('value_after_explicit_rollback=', value_bytes) FROM ss3_probe_locks WHERE id = 4;" \
  | mysql_client --force >"$tmp_dir/lock-timeout.out" 2>"$tmp_dir/lock-timeout.err"
lock_timeout_rc="$?"
set -e
release_gate "lock-gate"
wait "$lock_holder"
forget_background_pid "$lock_holder"
test "$(cat "$tmp_dir/lock-holder.rc")" = "0"
test "$lock_timeout_rc" -ne 124
grep -F 'ERROR 1205 (HY000)' "$tmp_dir/lock-timeout.err" >/dev/null
grep -Fx 'prior_write_visible_after_timeout=7' "$tmp_dir/lock-timeout.out" >/dev/null
grep -Fx 'value_after_explicit_rollback=0' "$tmp_dir/lock-timeout.out" >/dev/null
printf 'PASS SS3_MYSQL_PROBE_LOCK_TIMEOUT_1205_ROLLBACK_PASS\n'

printf '%s\n' \
  "SELECT CONCAT('initial=', @@session.time_zone, ':', @@session.sql_mode);" \
  "SET SESSION time_zone = '+05:30';" \
  "SET SESSION sql_mode = '';" \
  "SELECT CONCAT('polluted=', @@session.time_zone, ':', @@session.sql_mode);" \
  '\x' \
  "SELECT CONCAT('reset=', @@session.time_zone, ':', @@session.sql_mode);" \
  | mysql_client --commands > "$tmp_dir/session-reset.out"
grep -Fx 'polluted=+05:30:' "$tmp_dir/session-reset.out" >/dev/null
grep -F 'reset=+00:00:' "$tmp_dir/session-reset.out" | grep -F 'STRICT_TRANS_TABLES' >/dev/null
printf 'PASS SS3_MYSQL_PROBE_SESSION_RESET_PASS\n'
