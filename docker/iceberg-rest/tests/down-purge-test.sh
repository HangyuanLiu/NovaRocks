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
cleanup() {
  if [[ -n "${runtime_dir:-}" ]]; then
    rm -rf "$runtime_dir"
  fi
  if [[ -n "${current_link:-}" ]]; then
    rm -rf "$current_link"
    case "${saved_current_kind:-missing}" in
      symlink)
        ln -s "$saved_current_ref" "$current_link"
        ;;
      path)
        mv "$saved_current_backup" "$current_link"
        ;;
      missing)
        ;;
    esac
  fi
  rm -rf "$tmpdir"
}
trap cleanup EXIT

workspace="$tmpdir/workspace/NovaRocks"
mkdir -p "$workspace"

slug="$(basename "$workspace" | tr '[:upper:]' '[:lower:]' | tr -c 'a-z0-9' '-' | sed 's/^-*//;s/-*$//;s/--*/-/g')"
slug="$(printf '%s' "$slug" | cut -c1-24)"
hash="$(printf '%s' "$workspace" | shasum -a 1 | awk '{print substr($1, 1, 8)}')"
env_id="${slug}-${hash}"

runtime_base="$REPO_ROOT/docker/iceberg-rest/runtime"
runtime_dir="$runtime_base/$env_id"
current_link="$runtime_base/current"
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

mkdir -p "$runtime_dir"
printf 'compose env\n' >"$runtime_dir/compose.env"
cat >"$runtime_dir/env.sh" <<EOF
export NOVA_ENV_SHARED_DOCKER="true"
export NOVA_ENV_COMPOSE_PROJECT="nr-iceberg-rest"
export MINIO_ROOT_USER="admin"
export MINIO_ROOT_PASSWORD="admin123"
EOF
rm -rf "$current_link"
ln -s "$env_id" "$current_link"

fakebin="$tmpdir/bin"
mkdir -p "$fakebin"
cat >"$fakebin/docker" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"$DOCKER_CALLS"
exit 0
EOF
chmod +x "$fakebin/docker"

export DOCKER_CALLS="$tmpdir/docker.calls"
touch "$DOCKER_CALLS"

PATH="$fakebin:$PATH" \
NOVAROCKS_WORKSPACE_ROOT="$workspace" \
  "$REPO_ROOT/docker/iceberg-rest/down.sh" --runtime-only --purge >"$tmpdir/stdout" 2>"$tmpdir/stderr"

if [[ -e "$runtime_dir" ]]; then
  echo "expected --purge to remove runtime dir" >&2
  exit 1
fi

if [[ -e "$current_link" ]]; then
  echo "expected --purge to remove current symlink for purged runtime" >&2
  exit 1
fi

if ! grep -q "minio/novarocks/$env_id/" "$DOCKER_CALLS"; then
  echo "expected --purge to remove current novarocks object-store prefix" >&2
  cat "$DOCKER_CALLS" >&2
  exit 1
fi

if ! grep -q "minio/warehouse/$env_id/" "$DOCKER_CALLS"; then
  echo "expected --purge to remove current warehouse object-store prefix" >&2
  cat "$DOCKER_CALLS" >&2
  exit 1
fi

if grep -q " down " "$DOCKER_CALLS"; then
  echo "runtime-only purge must not stop shared Docker services" >&2
  cat "$DOCKER_CALLS" >&2
  exit 1
fi
