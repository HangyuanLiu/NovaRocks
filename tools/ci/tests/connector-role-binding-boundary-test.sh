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
CHECKER="$REPO_ROOT/tools/ci/check-connector-role-binding-boundary.py"
tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

metadata="$tmpdir/metadata.json"
source_root="$tmpdir/source"
mkdir -p "$source_root/novarocks/frontend/src/connector" \
  "$source_root/novarocks/backend/src/connector"
cargo metadata --manifest-path "$REPO_ROOT/Cargo.toml" --format-version 1 >"$metadata"

"$CHECKER" --metadata-path "$metadata" --source-root "$source_root"

assert_rejected() {
  local metadata_path="$1"
  local expected="$2"
  if "$CHECKER" --metadata-path "$metadata_path" --source-root "$source_root" \
    >"$metadata_path.stdout" 2>"$metadata_path.stderr"; then
    echo "connector role binding mutation was accepted: $metadata_path" >&2
    exit 1
  fi
  grep -Fq "$expected" "$metadata_path.stderr"
}

binding_server="$tmpdir/binding-server.json"
jq '
  (.packages[] | select(.name == "novarocks-connector-binding") | .dependencies) += [{
    name: "novarocks-server", kind: null, optional: false
  }]
' "$metadata" >"$binding_server"
assert_rejected "$binding_server" \
  "novarocks-connector-binding internal normal dependencies must be exactly"

backend_missing="$tmpdir/backend-missing-binding.json"
jq '
  (.packages[] | select(.name == "novarocks-backend") | .dependencies) |= map(
    select(.name != "novarocks-connector-binding")
  )
' "$metadata" >"$backend_missing"
assert_rejected "$backend_missing" \
  "novarocks-backend must directly declare a normal dependency on novarocks-connector-binding"

touch "$source_root/novarocks/backend/src/connector/typed_registry.rs"
if "$CHECKER" --metadata-path "$metadata" --source-root "$source_root" \
  >"$tmpdir/source.stdout" 2>"$tmpdir/source.stderr"; then
  echo "connector role binding source mutation was accepted" >&2
  exit 1
fi
grep -Fq "legacy parallel registry must be removed" "$tmpdir/source.stderr"

echo "connector-role-binding-boundary-test: PASS"
