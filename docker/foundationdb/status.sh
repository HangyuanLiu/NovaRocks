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
  # Give the command and all descendants a dedicated process group so a
  # timeout cannot leave a Compose CLI plugin or helper running.
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
      echo "command timed out after ${timeout_seconds}s: $*" >&2
      return 124
    fi
    sleep 1
    elapsed=$((elapsed + 1))
  done
  wait "$child"
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exports_file="$SCRIPT_DIR/runtime/current/env.sh"
self_check_only=false

if [[ "${1:-}" == "--self-check" ]]; then
  self_check_only=true
  shift
fi
if [[ "$#" -ne 0 ]]; then
  echo "usage: $0 [--self-check]" >&2
  exit 2
fi
if [[ ! -f "$exports_file" ]]; then
  echo "FoundationDB environment is not initialized; run up.sh --prepare-only first" >&2
  exit 1
fi

# shellcheck disable=SC1090
source "$exports_file"
test "$NOVAROCKS_FDB_VERSION" = "7.3.69"
test -n "$NOVA_FDB_ENV_ID"
test -n "$NOVA_FDB_COMPOSE_PROJECT"
test -n "$NOVAROCKS_FDB_KEYSPACE_ID"
test -f "$NOVAROCKS_FDB_CLUSTER_FILE"
test -d "$FDB_CLIENT_LIB_PATH"
test "$FDB_CLIENT_LIB_PATH" = "$NOVA_FDB_CLIENT_LIBRARY_DIR"
test -f "$NOVA_FDB_CLIENT_LIBRARY_FILE"
test -x "$NOVA_FDB_FDBCLI"

case "$NOVA_FDB_CLIENT_PLATFORM" in
  darwin-arm64)
    test "$NOVA_FDB_CLIENT_ASSET_SHA256" = "6bfbd48ac21356de0baa0c1e84c6e33d15d95d0b9d022c35a7625e5d9293b71e"
    ;;
  linux-x86_64)
    test "$NOVA_FDB_CLIENT_ASSET_SHA256" = "ea59d1708519798c7bc4f514cd29af1ac8e41dccbec4371f22d86b713ea81cbf"
    ;;
  *)
    echo "unexpected FoundationDB client platform: $NOVA_FDB_CLIENT_PLATFORM" >&2
    exit 1
    ;;
esac

client_version="$("$NOVA_FDB_FDBCLI" --version 2>&1)"
if [[ "$client_version" != *"7.3.69"* ]]; then
  echo "unexpected fdbcli version: $client_version" >&2
  exit 1
fi
if ! LC_ALL=C grep -a -q '7.3.69' "$NOVA_FDB_CLIENT_LIBRARY_FILE"; then
  echo "FoundationDB client library does not identify itself as version 7.3.69" >&2
  exit 1
fi

if [[ "$NOVAROCKS_FDB_CLUSTER_FILE" != /* ]]; then
  echo "FoundationDB cluster-file bind source must be absolute" >&2
  exit 1
fi
rendered_compose="$(run_with_timeout 15 docker compose \
  --env-file "$NOVA_FDB_COMPOSE_ENV" \
  -p "$NOVA_FDB_COMPOSE_PROJECT" \
  -f "$NOVA_FDB_COMPOSE_FILE" \
  config)"
for expected in \
  "source: $NOVAROCKS_FDB_CLUSTER_FILE" \
  "target: /var/fdb/fdb.cluster" \
  "read_only: true" \
  "FDB_CLUSTER_FILE: /var/fdb/fdb.cluster" \
  "--cluster-file /var/fdb/fdb.cluster"
do
  if ! grep -F -- "$expected" <<<"$rendered_compose" >/dev/null; then
    echo "rendered FoundationDB Compose config is missing: $expected" >&2
    exit 1
  fi
done

if [[ "$self_check_only" == true ]]; then
  timeout_marker="$(mktemp "${TMPDIR:-/tmp}/novarocks-fdb-timeout.XXXXXX")"
  rm -f "$timeout_marker"
  set +e
  run_with_timeout 1 bash -c \
    'sleep 60 & printf "%s" "$!" > "$1"; wait' \
    _ "$timeout_marker" >/dev/null 2>&1
  timeout_status="$?"
  set -e
  if [[ "$timeout_status" -ne 124 || ! -s "$timeout_marker" ]]; then
    rm -f "$timeout_marker"
    echo "FoundationDB timeout process-group self-check did not time out as expected" >&2
    exit 1
  fi
  timeout_child="$(cat "$timeout_marker")"
  rm -f "$timeout_marker"
  if kill -0 "$timeout_child" >/dev/null 2>&1; then
    kill "$timeout_child" >/dev/null 2>&1 || true
    kill -9 "$timeout_child" >/dev/null 2>&1 || true
    echo "FoundationDB timeout process-group self-check leaked child $timeout_child" >&2
    exit 1
  fi
  echo "FoundationDB fixture self-check passed"
  exit 0
fi

run_with_timeout 15 docker compose \
  --env-file "$NOVA_FDB_COMPOSE_ENV" \
  -p "$NOVA_FDB_COMPOSE_PROJECT" \
  -f "$NOVA_FDB_COMPOSE_FILE" \
  ps

server_version="$(run_with_timeout 15 docker compose --env-file "$NOVA_FDB_COMPOSE_ENV" -p "$NOVA_FDB_COMPOSE_PROJECT" -f "$NOVA_FDB_COMPOSE_FILE" exec -T foundationdb fdbserver --version 2>&1)"
if [[ "$server_version" != *"7.3.69"* ]]; then
  echo "unexpected FoundationDB server version: $server_version" >&2
  exit 1
fi

echo
echo "FoundationDB fixture:"
echo "  server version: $server_version"
echo "  client binary version: $client_version"
echo "  client library version: 7.3.69 (verified official package)"
echo "  client link directory: $FDB_CLIENT_LIB_PATH"
echo "  client library: $NOVA_FDB_CLIENT_LIBRARY_FILE"
echo "  cluster file: $NOVAROCKS_FDB_CLUSTER_FILE"
echo "  keyspace UUID: $NOVAROCKS_FDB_KEYSPACE_ID"
echo "  environment: $exports_file"
echo
run_with_timeout 7 "$NOVA_FDB_FDBCLI" -C "$NOVAROCKS_FDB_CLUSTER_FILE" --exec status --timeout 5
