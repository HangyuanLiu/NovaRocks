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
runtime_base="$SCRIPT_DIR/runtime"
current_link="$runtime_base/current"
exports_file="$current_link/env.sh"
stop_docker=false
for arg in "$@"; do
  case "$arg" in
    --docker)
      stop_docker=true
      ;;
    *)
      echo "usage: $0 [--docker]" >&2
      exit 2
      ;;
  esac
done

if [[ ! -f "$exports_file" ]]; then
  rm -f "$current_link"
  echo "MySQL state-store environment is not initialized"
  exit 0
fi

# shellcheck disable=SC1090
source "$exports_file"
case "$NOVA_MYSQL_RUNTIME_DIR" in
  "$runtime_base"/nr-mysql-*) ;;
  *)
    echo "refusing to remove unexpected MySQL runtime: $NOVA_MYSQL_RUNTIME_DIR" >&2
    exit 1
    ;;
esac

if [[ "$stop_docker" == true ]]; then
  if command -v docker >/dev/null 2>&1; then
    run_with_timeout 90 docker compose \
      --env-file "$NOVA_MYSQL_COMPOSE_ENV" \
      -p "$NOVA_MYSQL_COMPOSE_PROJECT" \
      -f "$NOVA_MYSQL_COMPOSE_FILE" \
      down --remove-orphans
  fi
else
  echo "MySQL Docker project is left running: $NOVA_MYSQL_COMPOSE_PROJECT"
fi

rm -f "$current_link"
rm -rf "$NOVA_MYSQL_RUNTIME_DIR"
echo "Removed MySQL state-store runtime: $NOVA_MYSQL_RUNTIME_DIR"
