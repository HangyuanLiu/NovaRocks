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

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
tmpdir="$(mktemp -d)"
runtime_base="$REPO_ROOT/docker/iceberg-rest/runtime"
current_link="$runtime_base/current"

workspace_env_id() {
  local workspace="$1" slug hash
  slug="$(basename "$workspace" | tr '[:upper:]' '[:lower:]' | tr -c 'a-z0-9' '-' | sed 's/^-*//;s/-*$//;s/--*/-/g')"
  slug="$(printf '%s' "$slug" | cut -c1-24)"
  hash="$(printf '%s' "$workspace" | shasum -a 1 | awk '{print substr($1, 1, 8)}')"
  printf '%s-%s\n' "$slug" "$hash"
}

saved_current_kind="missing"
saved_current_ref=""
saved_current_backup="$tmpdir/current.backup"
if [[ -L "$current_link" ]]; then
  saved_current_kind="symlink"
  saved_current_ref="$(readlink "$current_link")"
elif [[ -e "$current_link" ]]; then
  saved_current_kind="path"
  mv "$current_link" "$saved_current_backup"
fi

workspace_a="$tmpdir/workspace-a/NovaRocks"
workspace_b="$tmpdir/workspace-b/NovaRocks"
env_id_a="$(workspace_env_id "$workspace_a")"
env_id_b="$(workspace_env_id "$workspace_b")"
runtime_a="$runtime_base/$env_id_a"
runtime_b="$runtime_base/$env_id_b"

cleanup() {
  rm -rf "$runtime_a" "$runtime_b" "$current_link"
  case "$saved_current_kind" in
    symlink)
      ln -s "$saved_current_ref" "$current_link"
      ;;
    path)
      mv "$saved_current_backup" "$current_link"
      ;;
    missing)
      ;;
  esac
  rm -rf "$tmpdir"
}
trap cleanup EXIT

mkdir -p "$workspace_a" "$workspace_b"
config_file="$tmpdir/tst10-isolated.env"
cat >"$config_file" <<'EOF'
NOVA_ENV_SHARED_DOCKER=true
NOVA_ENV_SHARED_COMPOSE_PROJECT=nr-tst10-environment-test
NOVA_ENV_MINIO_PORT=19100
NOVA_ENV_MINIO_CONSOLE_PORT=19101
NOVA_ENV_REST_PORT=19181
NOVA_ENV_SPARK_UI_PORT=19404
NOVA_ENV_SHARED_BENCHMARK_ROOT=s3://novarocks/shared/benchmarks
NOVA_ENV_BENCHMARK_LEASE_IMAGE=docker.io/library/busybox@sha256:3c6ae8008e2c2eedd141725c30b20d9c36b026eb796688f88205845ef17aa213
NOVA_ENV_BENCHMARK_LEASE_HEARTBEAT_SECONDS=1
NOVA_ENV_BENCHMARK_LEASE_EXPIRY_SECONDS=4
NOVA_ENV_BENCHMARK_LEASE_WAIT_SECONDS=8
NOVA_ENV_BENCHMARK_BUILD_TIMEOUT_SECONDS=16
EOF

fakebin="$tmpdir/bin"
mkdir -p "$fakebin"
cat >"$fakebin/docker" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"$DOCKER_CALLS"
EOF
chmod +x "$fakebin/docker"
export DOCKER_CALLS="$tmpdir/docker.calls"
touch "$DOCKER_CALLS"

PATH="$fakebin:$PATH" NOVAROCKS_WORKSPACE_ROOT="$workspace_a" NOVA_ENV_CONFIG_FILE="$config_file" \
  "$REPO_ROOT/docker/iceberg-rest/up.sh" --prepare-only >"$tmpdir/up-a.out"
PATH="$fakebin:$PATH" NOVAROCKS_WORKSPACE_ROOT="$workspace_b" NOVA_ENV_CONFIG_FILE="$config_file" \
  "$REPO_ROOT/docker/iceberg-rest/up.sh" --prepare-only >"$tmpdir/up-b.out"

if [[ -s "$DOCKER_CALLS" ]]; then
  echo "up.sh --prepare-only must not call Docker" >&2
  cat "$DOCKER_CALLS" >&2
  exit 1
fi

env_a="$runtime_a/env.sh"
env_b="$runtime_b/env.sh"
for path in "$env_a" "$env_b" "$runtime_a/manifest.json" "$runtime_b/manifest.json" "$runtime_a/sql-test.toml" "$runtime_b/sql-test.toml"; do
  [[ -f "$path" ]] || { echo "missing generated fixture artifact: $path" >&2; exit 1; }
done

for env_file in "$env_a" "$env_b"; do
  grep -Fx 'export NOVA_ENV_SHARED_BENCHMARK_ROOT="s3://novarocks/shared/benchmarks"' "$env_file" >/dev/null
  grep -Fx 'export NOVA_ENV_BENCHMARK_LEASE_NAMESPACE="nr-tst10-environment-test"' "$env_file" >/dev/null
  grep -Fx 'export NOVA_ENV_BENCHMARK_LEASE_IMAGE="docker.io/library/busybox@sha256:3c6ae8008e2c2eedd141725c30b20d9c36b026eb796688f88205845ef17aa213"' "$env_file" >/dev/null
  grep -Fx 'export NOVA_ENV_BENCHMARK_LEASE_HEARTBEAT_SECONDS="1"' "$env_file" >/dev/null
  grep -Fx 'export NOVA_ENV_BENCHMARK_LEASE_EXPIRY_SECONDS="4"' "$env_file" >/dev/null
done

if cmp -s "$env_a" "$env_b"; then
  echo "different workspaces must retain different private runtime identities" >&2
  exit 1
fi

for manifest in "$runtime_a/manifest.json" "$runtime_b/manifest.json"; do
  grep -F '"shared_root": "s3://novarocks/shared/benchmarks"' "$manifest" >/dev/null
  grep -F '"lease_namespace": "nr-tst10-environment-test"' "$manifest" >/dev/null
done
for runner_config in "$runtime_a/sql-test.toml" "$runtime_b/sql-test.toml"; do
  grep -Fx 'benchmark_shared_root = "s3://novarocks/shared/benchmarks"' "$runner_config" >/dev/null
done

canonical_config="$tmpdir/canonical.env"
sed 's/nr-tst10-environment-test/nr-iceberg-rest/' "$config_file" >"$canonical_config"
if PATH="$fakebin:$PATH" NOVAROCKS_WORKSPACE_ROOT="$workspace_b" NOVA_ENV_CONFIG_FILE="$canonical_config" \
  "$REPO_ROOT/docker/iceberg-rest/down.sh" --docker --volumes >"$tmpdir/canonical.out" 2>"$tmpdir/canonical.err"; then
  echo "canonical project volume deletion must be rejected" >&2
  exit 1
fi
grep -F 'refusing to delete canonical shared Docker volume' "$tmpdir/canonical.err" >/dev/null

: >"$DOCKER_CALLS"
PATH="$fakebin:$PATH" NOVAROCKS_WORKSPACE_ROOT="$workspace_b" NOVA_ENV_CONFIG_FILE="$config_file" \
  "$REPO_ROOT/docker/iceberg-rest/down.sh" --docker >"$tmpdir/preserve.out" 2>"$tmpdir/preserve.err"
grep -F 'Stopping Docker project: nr-tst10-environment-test (preserving volume: nr-tst10-environment-test_minio-data)' "$tmpdir/preserve.out" >/dev/null
if grep -F -- '--volumes' "$DOCKER_CALLS" >/dev/null; then
  echo "ordinary --docker cleanup must preserve the MinIO volume" >&2
  cat "$DOCKER_CALLS" >&2
  exit 1
fi

: >"$DOCKER_CALLS"
PATH="$fakebin:$PATH" NOVAROCKS_WORKSPACE_ROOT="$workspace_b" NOVA_ENV_CONFIG_FILE="$config_file" \
  NOVA_ENV_ALLOW_VOLUME_DELETE=true \
  NOVA_ENV_EXPECTED_COMPOSE_PROJECT=nr-tst10-environment-test \
  NOVA_ENV_EXPECTED_MINIO_VOLUME=nr-tst10-environment-test_minio-data \
  "$REPO_ROOT/docker/iceberg-rest/down.sh" --docker --volumes >"$tmpdir/isolated.out" 2>"$tmpdir/isolated.err"

grep -F 'Volume deletion authorized for exact project: nr-tst10-environment-test; volume: nr-tst10-environment-test_minio-data' "$tmpdir/isolated.out" >/dev/null
grep -F -- '-p nr-tst10-environment-test' "$DOCKER_CALLS" >/dev/null
