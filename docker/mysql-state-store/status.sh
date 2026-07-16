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
      echo "command timed out after ${timeout_seconds}s: $1" >&2
      return 124
    fi
    sleep 1
    elapsed=$((elapsed + 1))
  done
  wait "$child"
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exports_file="$SCRIPT_DIR/runtime/current/env.sh"
self_check=false
if [[ "${1:-}" == "--self-check" ]]; then
  self_check=true
  shift
fi
if [[ "$#" -ne 0 ]]; then
  echo "usage: $0 [--self-check]" >&2
  exit 2
fi
if [[ ! -f "$exports_file" ]]; then
  echo "MySQL state-store environment is not initialized; run up.sh --prepare-only first" >&2
  exit 1
fi

# shellcheck disable=SC1090
source "$exports_file"
test "$NOVAROCKS_MYSQL_VERSION" = "8.4.10"
test -n "$NOVA_MYSQL_ENV_ID"
test -n "$NOVA_MYSQL_COMPOSE_PROJECT"
test -n "$NOVAROCKS_MYSQL_DATABASE"
test -n "$NOVAROCKS_MYSQL_USERNAME"
test "$NOVAROCKS_MYSQL_PASSWORD_ENV" = "NOVAROCKS_MYSQL_PASSWORD"
test "$(stat -f '%Lp' "$exports_file" 2>/dev/null || stat -c '%a' "$exports_file")" = "600"

readiness="$(run_with_timeout 15 docker compose \
  --env-file "$NOVA_MYSQL_COMPOSE_ENV" \
  -p "$NOVA_MYSQL_COMPOSE_PROJECT" \
  -f "$NOVA_MYSQL_COMPOSE_FILE" \
  exec -T mysql mysql \
  --defaults-extra-file=/run/secrets/novarocks-mysql-provider.cnf \
  --database="$NOVAROCKS_MYSQL_DATABASE" \
  --batch --skip-column-names \
  --execute="SELECT VERSION(), @@innodb_page_size, @@default_storage_engine, @@session.time_zone, @@session.sql_mode;")"
IFS=$'\t' read -r actual_version page_size engine time_zone sql_mode <<<"$readiness"
test "$actual_version" = "8.4.10"
test "$page_size" = "16384"
test "${engine,,}" = "innodb"
test "$time_zone" = "+00:00"
case ",$sql_mode," in
  *,STRICT_TRANS_TABLES,*) ;;
  *)
    echo "MySQL strict SQL mode is not active" >&2
    exit 1
    ;;
esac

if [[ "$self_check" == true ]]; then
  timeout_marker="$(mktemp "${TMPDIR:-/tmp}/novarocks-mysql-timeout.XXXXXX")"
  rm -f "$timeout_marker"
  set +e
  run_with_timeout 1 bash -c \
    'sleep 60 & printf "%s" "$!" > "$1"; wait' \
    _ "$timeout_marker" >/dev/null 2>&1
  timeout_rc="$?"
  set -e
  test "$timeout_rc" = "124"
  timeout_child="$(cat "$timeout_marker")"
  rm -f "$timeout_marker"
  if kill -0 "$timeout_child" >/dev/null 2>&1; then
    kill "$timeout_child" >/dev/null 2>&1 || true
    kill -9 "$timeout_child" >/dev/null 2>&1 || true
    echo "MySQL timeout self-check leaked child $timeout_child" >&2
    exit 1
  fi
  echo "MySQL state-store fixture self-check passed"
  exit 0
fi

run_with_timeout 15 docker compose \
  --env-file "$NOVA_MYSQL_COMPOSE_ENV" \
  -p "$NOVA_MYSQL_COMPOSE_PROJECT" \
  -f "$NOVA_MYSQL_COMPOSE_FILE" \
  ps
echo "MySQL state-store fixture:"
echo "  server version: $actual_version"
echo "  InnoDB page size: $page_size"
echo "  default engine: $engine"
echo "  session time zone: $time_zone"
echo "  database: $NOVAROCKS_MYSQL_DATABASE"
echo "  environment: $exports_file"
