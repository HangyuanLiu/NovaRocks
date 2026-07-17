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
umask 077

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exports_file="$SCRIPT_DIR/runtime/current/env.sh"
if [[ ! -f "$exports_file" ]]; then
  echo "MySQL state-store environment is not initialized" >&2
  exit 1
fi
# shellcheck disable=SC1090
source "$exports_file"
test -n "$NOVA_MYSQL_PROVISIONER_USERNAME"
test -n "$NOVA_MYSQL_PROVISIONER_PASSWORD"
test -n "$NOVAROCKS_MYSQL_USERNAME"
test -n "$NOVAROCKS_MYSQL_PASSWORD"

PROVIDER_TABLE_PRIVILEGES="SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, INDEX"
PROVIDER_TABLES=(
  state_store_meta
  state_store_kv
  state_store_changes
  state_store_commits
  fixture_readiness
  ss3_probe_keys
  ss3_probe_snapshot
  ss3_probe_locks
  ss3_probe_key_3073
)

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
      echo "MySQL provisioner command timed out" >&2
      return 124
    fi
    sleep 1
    elapsed=$((elapsed + 1))
  done
  wait "$child"
}

mysql_admin() {
  run_with_timeout 20 docker compose \
    --env-file "$NOVA_MYSQL_COMPOSE_ENV" \
    -p "$NOVA_MYSQL_COMPOSE_PROJECT" \
    -f "$NOVA_MYSQL_COMPOSE_FILE" \
    exec -T mysql mysql \
    --defaults-extra-file=/run/secrets/novarocks-mysql-provisioner.cnf \
    --protocol=socket --batch --skip-column-names "$@"
}

usage() {
  echo "usage: $0 create <case-id>" >&2
  echo "       $0 drop <database-name>" >&2
}

drop_database() {
  local database="$1"
  {
    local table
    for table in "${PROVIDER_TABLES[@]}"; do
      printf "REVOKE IF EXISTS %s ON \`%s\`.\`%s\` FROM '%s'@'%%';\n" \
        "$PROVIDER_TABLE_PRIVILEGES" "$database" "$table" "$NOVAROCKS_MYSQL_USERNAME"
    done
  } | mysql_admin >/dev/null
  printf 'DROP DATABASE IF EXISTS `%s`;\n' "$database" | mysql_admin >/dev/null
}

if [[ "$#" -ne 2 ]]; then
  usage
  exit 2
fi
action="$1"
argument="$2"

case "$action" in
  create)
    if [[ ! "$argument" =~ ^[A-Za-z0-9][A-Za-z0-9_-]{0,31}$ ]]; then
      echo "case ID must be 1..32 ASCII alphanumeric, underscore, or hyphen characters" >&2
      exit 2
    fi
    case_id="$(printf '%s' "$argument" | tr '[:upper:]-' '[:lower:]_')"
    case_id="${case_id:0:20}"
    nonce="$(if command -v openssl >/dev/null 2>&1; then openssl rand -hex 4; else od -An -N4 -tx1 /dev/urandom | tr -d ' \n'; fi)"
    database="novarocks_ss3_${NOVA_MYSQL_ENV_ID#nr-mysql-}_${BASHPID}_${case_id}_${nonce}"
    database="${database:0:64}"
    created_database="$database"
    rollback_create() {
      local original_status="$?"
      trap - EXIT
      if ! drop_database "$created_database"; then
        echo "failed to roll back test database creation: $created_database" >&2
        exit 1
      fi
      exit "$original_status"
    }
    trap rollback_create EXIT
    {
      printf 'CREATE DATABASE `%s` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;\n' "$database"
      printf "CREATE USER IF NOT EXISTS '%s'@'%%' IDENTIFIED BY '%s';\n" \
        "$NOVAROCKS_MYSQL_USERNAME" "$NOVAROCKS_MYSQL_PASSWORD"
      for table in "${PROVIDER_TABLES[@]}"; do
        printf "GRANT %s ON \`%s\`.\`%s\` TO '%s'@'%%';\n" \
          "$PROVIDER_TABLE_PRIVILEGES" "$database" "$table" "$NOVAROCKS_MYSQL_USERNAME"
      done
    } | mysql_admin >/dev/null
    printf '%s\n' "$database"
    trap - EXIT
    ;;
  drop)
    if [[ ! "$argument" =~ ^novarocks_ss3_[A-Za-z0-9_]{1,49}$ || "${#argument}" -gt 64 ]]; then
      echo "refusing to drop database outside the novarocks_ss3_ test namespace" >&2
      exit 2
    fi
    drop_database "$argument"
    ;;
  *)
    usage
    exit 2
    ;;
esac
