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
CHECKER="$REPO_ROOT/tools/ci/check-iceberg-provider-dependency-boundary.py"
tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

write_package() {
  local root="$1"
  local name="$2"
  mkdir -p "$root/$name/src"
  printf '[package]\nname = "%s"\nversion = "0.1.0"\nedition = "2024"\n' "$name" \
    >"$root/$name/Cargo.toml"
  : >"$root/$name/src/lib.rs"
}

write_fixture() {
  local root="$1"
  mkdir -p "$root"
  for package in novarocks-connector-iceberg novarocks-server novarocks-server-application novarocks-frontend-application novarocks-state-store-sqlite novarocks-fs novarocks-spi iceberg-catalog-rest iceberg-sdk-bridge; do
    write_package "$root" "$package"
  done
  cat >"$root/Cargo.toml" <<'EOF'
[workspace]
members = [
  "novarocks-connector-iceberg",
  "novarocks-server",
  "novarocks-server-application",
  "novarocks-frontend-application",
  "novarocks-state-store-sqlite",
  "novarocks-fs",
  "novarocks-spi",
  "iceberg-catalog-rest",
  "iceberg-sdk-bridge",
]
resolver = "3"
EOF
  cat >>"$root/novarocks-connector-iceberg/Cargo.toml" <<'EOF'

[dependencies]
novarocks-fs = { path = "../novarocks-fs" }
novarocks-spi = { path = "../novarocks-spi" }
EOF
  cat >>"$root/novarocks-server/Cargo.toml" <<'EOF'

[dependencies]
novarocks-server-application = { path = "../novarocks-server-application" }
EOF
  cat >>"$root/novarocks-server-application/Cargo.toml" <<'EOF'

[dependencies]
novarocks-connector-iceberg = { path = "../novarocks-connector-iceberg" }
novarocks-state-store-sqlite = { path = "../novarocks-state-store-sqlite" }
EOF
}

assert_rejected() {
  local root="$1"
  local expected="$2"
  if "$CHECKER" --manifest-path "$root/Cargo.toml" >"$root/stdout" 2>"$root/stderr"; then
    echo "Iceberg dependency boundary mutation was accepted: $root" >&2
    exit 1
  fi
  grep -Fq "$expected" "$root/stderr"
}

valid="$tmpdir/valid"
write_fixture "$valid"
"$CHECKER" --manifest-path "$valid/Cargo.toml"

forbidden="$tmpdir/forbidden"
cp -R "$valid" "$forbidden"
write_package "$forbidden" novarocks
cat >>"$forbidden/novarocks-connector-iceberg/Cargo.toml" <<'EOF'
novarocks = { path = "../novarocks" }
EOF
assert_rejected "$forbidden" "provider closure contains forbidden packages: novarocks"

role="$tmpdir/role"
cp -R "$valid" "$role"
cat >>"$role/novarocks-frontend-application/Cargo.toml" <<'EOF'

[dependencies]
novarocks-connector-iceberg = { path = "../novarocks-connector-iceberg" }
EOF
assert_rejected "$role" "novarocks-frontend-application must not directly depend on novarocks-connector-iceberg"

# HMS remains an ordinary provider read dependency; the REST-specific
# prohibition must not become a blanket ban of catalog implementations.
hms="$tmpdir/hms-read-dependency"
cp -R "$valid" "$hms"
write_package "$hms" iceberg-catalog-hms
printf 'iceberg-catalog-hms = { path = "../iceberg-catalog-hms" }\n' \
  >>"$hms/novarocks-connector-iceberg/Cargo.toml"
"$CHECKER" --manifest-path "$hms/Cargo.toml"

# Merely being a workspace member must not be mistaken for root reachability.
# The valid fixture already includes an unreachable REST SDK package.
for owner in novarocks-connector-iceberg novarocks-server; do
  for kind in dependencies build-dependencies dev-dependencies; do
    fixture="$tmpdir/$owner-direct-$kind"
    cp -R "$valid" "$fixture"
    # The normal dependency table already exists in the selected owner.
    if [[ "$kind" != "dependencies" ]]; then
      printf '\n[%s]\n' "$kind" >>"$fixture/$owner/Cargo.toml"
    fi
    printf 'iceberg-catalog-rest = { path = "../iceberg-catalog-rest" }\n' \
      >>"$fixture/$owner/Cargo.toml"
    if [[ "$kind" == "dev-dependencies" ]]; then
      "$CHECKER" --manifest-path "$fixture/Cargo.toml"
    else
      assert_rejected "$fixture" "$owner normal/build closure contains forbidden REST SDK: iceberg-catalog-rest"
    fi
  done

  # Follow both dependency kinds through an ordinary external bridge.
  for root_kind in dependencies build-dependencies; do
    for nested_kind in dependencies build-dependencies; do
      fixture="$tmpdir/$owner-transitive-$root_kind-$nested_kind"
      cp -R "$valid" "$fixture"
      if [[ "$root_kind" != "dependencies" ]]; then
        printf '\n[%s]\n' "$root_kind" >>"$fixture/$owner/Cargo.toml"
      fi
      printf 'iceberg-sdk-bridge = { path = "../iceberg-sdk-bridge" }\n' \
        >>"$fixture/$owner/Cargo.toml"
      printf '\n[%s]\niceberg-catalog-rest = { path = "../iceberg-catalog-rest" }\n' "$nested_kind" \
        >>"$fixture/iceberg-sdk-bridge/Cargo.toml"
      assert_rejected "$fixture" "$owner normal/build closure contains forbidden REST SDK: iceberg-catalog-rest"
    done
  done

  # A dev-only bridge can use normal/build SDK dependencies without becoming
  # a production consumer. Do not accidentally inspect an unfiltered graph.
  fixture="$tmpdir/$owner-dev-bridge"
  cp -R "$valid" "$fixture"
  printf '\n[dev-dependencies]\niceberg-sdk-bridge = { path = "../iceberg-sdk-bridge" }\n' \
    >>"$fixture/$owner/Cargo.toml"
  printf '\n[build-dependencies]\niceberg-catalog-rest = { path = "../iceberg-catalog-rest" }\n' \
    >>"$fixture/iceberg-sdk-bridge/Cargo.toml"
  "$CHECKER" --manifest-path "$fixture/Cargo.toml"

  # A renamed, optional dependency reachable from an exposed root feature is
  # still the REST SDK package. The graph output uses package identity.
  fixture="$tmpdir/$owner-feature-alias"
  cp -R "$valid" "$fixture"
  cat >>"$fixture/$owner/Cargo.toml" <<'EOF'
rest_reference = { package = "iceberg-catalog-rest", path = "../iceberg-catalog-rest", optional = true }
[features]
rest-reference = ["dep:rest_reference"]
EOF
  assert_rejected "$fixture" "$owner normal/build closure contains forbidden REST SDK: iceberg-catalog-rest"

  fixture="$tmpdir/$owner-target-specific"
  cp -R "$valid" "$fixture"
  cat >>"$fixture/$owner/Cargo.toml" <<'EOF'
[target.'cfg(target_os = "windows")'.build-dependencies]
iceberg-catalog-rest = { path = "../iceberg-catalog-rest" }
EOF
  assert_rejected "$fixture" "$owner normal/build closure contains forbidden REST SDK: iceberg-catalog-rest"
done

# The older, broader provider prohibition still includes dev paths. Do not
# weaken it to normal/build when adding the independent REST-specific guard.
for kind in build-dependencies dev-dependencies; do
  fixture="$tmpdir/old-forbidden-$kind"
  cp -R "$valid" "$fixture"
  write_package "$fixture" novarocks
  printf '\n[%s]\nnovarocks = { path = "../novarocks" }\n' "$kind" \
    >>"$fixture/novarocks-connector-iceberg/Cargo.toml"
  assert_rejected "$fixture" "provider closure contains forbidden packages: novarocks"
done

echo "iceberg-provider-dependency-boundary-test: PASS"
