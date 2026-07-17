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

sha256_text() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum | awk '{print $1}'
  else
    shasum -a 256 | awk '{print $1}'
  fi
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "${NOVAROCKS_WORKSPACE_ROOT:-$SCRIPT_DIR/../..}" && pwd)"
workspace_hash="$(printf '%s' "$WORKSPACE_ROOT" | sha256_text)"
env_id="nr-mysql-${workspace_hash:0:12}"
compose_project="nrss3${workspace_hash:0:12}"
runtime_base="$SCRIPT_DIR/runtime"
runtime_dir="$runtime_base/$env_id"
current_link="$runtime_base/current"
exports_file="$runtime_dir/env.sh"
compose_file="$SCRIPT_DIR/compose.yml"
compose_env="$runtime_dir/compose.env"
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

if [[ -f "$exports_file" ]]; then
  # shellcheck disable=SC1090
  source "$exports_file"
  if [[ "$NOVA_MYSQL_ENV_ID" != "$env_id"
    || "$NOVA_MYSQL_COMPOSE_PROJECT" != "$compose_project"
    || "$NOVA_MYSQL_RUNTIME_DIR" != "$runtime_dir"
    || "$NOVA_MYSQL_COMPOSE_FILE" != "$compose_file" ]]; then
    echo "refusing mismatched MySQL runtime identity" >&2
    exit 1
  fi
fi

temporary_compose_env=""
if [[ ! -f "$compose_env" ]]; then
  temporary_compose_env="$(mktemp "${TMPDIR:-/tmp}/novarocks-mysql-down.XXXXXX")"
  {
    printf 'NOVA_MYSQL_PORT=43000\n'
    printf 'NOVA_MYSQL_RUNTIME_DIR=%s\n' "$runtime_dir"
  } > "$temporary_compose_env"
  chmod 600 "$temporary_compose_env"
  compose_env="$temporary_compose_env"
fi
cleanup_temporary_env() {
  if [[ -n "$temporary_compose_env" ]]; then
    rm -f "$temporary_compose_env"
  fi
}
trap cleanup_temporary_env EXIT

if ! command -v docker >/dev/null 2>&1; then
  if [[ "$stop_docker" == true ]]; then
    echo "docker is required to stop the MySQL state-store fixture; runtime is retained" >&2
    exit 1
  fi
  echo "Docker is unavailable; MySQL runtime is retained for safe retry"
  exit 0
fi

project_running() {
  run_with_timeout 20 docker compose \
    --env-file "$compose_env" \
    -p "$compose_project" \
    -f "$compose_file" \
    ps --all --quiet mysql
}

if ! running_ids="$(project_running)"; then
  echo "failed to inspect MySQL Compose project; runtime is retained" >&2
  exit 1
fi

if [[ "$stop_docker" == true ]]; then
  if ! run_with_timeout 90 docker compose \
    --env-file "$compose_env" \
    -p "$compose_project" \
    -f "$compose_file" \
    down --remove-orphans; then
    echo "failed to stop MySQL Compose project; runtime is retained" >&2
    exit 1
  fi
  if [[ -d "$runtime_dir/data" ]]; then
    if ! run_with_timeout 90 docker compose \
      --profile cleanup \
      --env-file "$compose_env" \
      -p "$compose_project" \
      -f "$compose_file" \
      run --rm --no-deps runtime-cleaner; then
      echo "failed to clean MySQL container-owned runtime data; runtime is retained" >&2
      exit 1
    fi
    if ! run_with_timeout 90 docker compose \
      --profile cleanup \
      --env-file "$compose_env" \
      -p "$compose_project" \
      -f "$compose_file" \
      down --remove-orphans; then
      echo "failed to remove MySQL cleanup project resources; runtime is retained" >&2
      exit 1
    fi
  fi
elif [[ -n "$running_ids" ]]; then
  echo "MySQL Docker project is running; backing runtime is retained: $compose_project"
  exit 0
elif [[ -d "$runtime_dir/data" ]]; then
  echo "MySQL runtime data requires --docker cleanup; backing runtime is retained: $runtime_dir"
  exit 0
else
  echo "MySQL Docker project is not running; removing stale runtime"
fi

rm -f "$current_link"
if [[ -d "$runtime_dir" ]]; then
  run_with_timeout 30 rm -rf "$runtime_dir"
fi
echo "Removed MySQL state-store runtime: $runtime_dir"
