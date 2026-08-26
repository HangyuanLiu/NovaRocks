#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership and limitations under the License.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
CHECKER="$REPO_ROOT/tools/ci/check-native-trust-dependency-boundary.py"
tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

base_metadata="$tmpdir/base-metadata.json"
cargo metadata --manifest-path "$REPO_ROOT/Cargo.toml" --format-version 1 >"$base_metadata"
"$CHECKER" --metadata-path "$base_metadata"

assert_rejected() {
  local metadata_path="$1"
  local expected_error="$2"
  if "$CHECKER" --metadata-path "$metadata_path" >"$metadata_path.stdout" 2>"$metadata_path.stderr"; then
    echo "Native trust dependency mutation was accepted: $metadata_path" >&2
    exit 1
  fi
  grep -Fq "$expected_error" "$metadata_path.stderr"
}

server_direct="$tmpdir/trust-server-direct.json"
jq '
  (.packages[] | select(.name == "novarocks-native-trust") | .dependencies) += [{
    name: "novarocks-server", kind: null, optional: false
  }]
' "$base_metadata" >"$server_direct"
assert_rejected "$server_direct" \
  "novarocks-native-trust internal normal dependencies must be exactly"

missing_secret="$tmpdir/trust-missing-secret.json"
jq '
  (.packages[] | select(.name == "novarocks-native-trust") | .dependencies) |= map(
    select(.name != "novarocks-secret")
  )
' "$base_metadata" >"$missing_secret"
assert_rejected "$missing_secret" \
  "novarocks-native-trust internal normal dependencies must be exactly"

echo "native-trust-dependency-boundary-test: PASS"
