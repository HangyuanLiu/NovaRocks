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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
CHECKER="$REPO_ROOT/tools/ci/check-physical-plan-dependency-boundary.py"
tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

write_package() {
  local fixture_root="$1"
  local directory="$2"
  local package_name="$3"
  local package_root="$fixture_root/crates/$directory"

  mkdir -p "$package_root/src"
  {
    printf '[package]\nname = "%s"\nversion = "0.1.0"\nedition = "2024"\n' "$package_name"
    printf '\n[dependencies]\n'
  } >"$package_root/Cargo.toml"
  : >"$package_root/src/lib.rs"
}

append_dependency() {
  local fixture_root="$1"
  local directory="$2"
  local dependency="$3"

  printf '%s\n' "$dependency" >>"$fixture_root/crates/$directory/Cargo.toml"
}

write_fixture() {
  local fixture_root="$1"

  mkdir -p "$fixture_root"
  cat >"$fixture_root/Cargo.toml" <<'EOF'
[workspace]
resolver = "2"
exclude = ["crates/arrow-schema-v2", "crates/bytes-v2"]
members = [
  "crates/arrow-array",
  "crates/connector-contract",
  "crates/execution",
  "crates/feature-user",
  "crates/hyper",
  "crates/physical-plan",
  "crates/proto-models",
  "crates/serde",
  "crates/sql",
  "crates/tonic",
  "crates/type-contract",
]
EOF

  write_package "$fixture_root" arrow-array arrow-array
  write_package "$fixture_root" arrow-schema-v2 arrow-schema
  replace_text "$fixture_root/crates/arrow-schema-v2/Cargo.toml" \
    'version = "0.1.0"' 'version = "58.2.0"'
  append_dependency_section "$fixture_root" arrow-schema-v2 features \
    'serde = []'
  write_package "$fixture_root" bytes-v2 bytes
  replace_text "$fixture_root/crates/bytes-v2/Cargo.toml" \
    'version = "0.1.0"' 'version = "1.11.0"'
  write_package "$fixture_root" connector-contract novarocks-connector-contract
  write_package "$fixture_root" execution novarocks-execution
  write_package "$fixture_root" feature-user feature-user
  write_package "$fixture_root" hyper hyper
  write_package "$fixture_root" physical-plan novarocks-physical-plan
  write_package "$fixture_root" proto-models novarocks-proto-models
  write_package "$fixture_root" serde serde
  write_package "$fixture_root" sql novarocks-sql
  write_package "$fixture_root" tonic tonic
  write_package "$fixture_root" type-contract novarocks-type-contract

  append_dependency "$fixture_root" physical-plan \
    'arrow-schema = "=58.2.0"'
  append_dependency "$fixture_root" physical-plan \
    'novarocks-connector-contract = { path = "../connector-contract" }'
  append_dependency "$fixture_root" physical-plan \
    'novarocks-type-contract = { path = "../type-contract" }'
  append_dependency "$fixture_root" connector-contract \
    'bytes = "=1.11.0"'
  append_dependency "$fixture_root" type-contract \
    'arrow-schema = "=58.2.0"'

  # Another workspace member enables a feature on a shared dependency. Cargo
  # metadata's workspace resolve graph sees serde, while physical-plan's own
  # package-selected tree must remain serde-free.
  append_dependency "$fixture_root" feature-user \
    'arrow-schema = { version = "=58.2.0", features = ["serde"] }'
  append_dependency "$fixture_root" feature-user \
    'bytes_alt = { package = "bytes", path = "../bytes-v2" }'
}

append_dependency_section() {
  local fixture_root="$1"
  local directory="$2"
  local section="$3"
  local dependency="$4"

  {
    printf '\n[%s]\n' "$section"
    printf '%s\n' "$dependency"
  } >>"$fixture_root/crates/$directory/Cargo.toml"
}

replace_text() {
  local file="$1"
  local old="$2"
  local new="$3"

  python3 - "$file" "$old" "$new" <<'PY'
from pathlib import Path
import sys

path = Path(sys.argv[1])
old = sys.argv[2]
new = sys.argv[3]
text = path.read_text()
if text.count(old) != 1:
    raise SystemExit(f"expected exactly one mutation target in {path}: {old!r}")
path.write_text(text.replace(old, new))
PY
}

new_mutation() {
  local name="$1"
  local mutation_root="$tmpdir/$name"

  cp -R "$baseline_root" "$mutation_root"
  rm -f "$mutation_root/Cargo.lock"
  printf '%s' "$mutation_root"
}

assert_accepted() {
  local fixture_root="$1"

  if ! cargo generate-lockfile --offline --manifest-path "$fixture_root/Cargo.toml" \
      >"$fixture_root/lock-stdout" 2>"$fixture_root/lock-stderr"; then
    echo "failed to generate mutation fixture lockfile: $fixture_root" >&2
    cat "$fixture_root/lock-stderr" >&2
    exit 1
  fi
  if ! python3 "$CHECKER" --manifest-path "$fixture_root/Cargo.toml" \
      >"$fixture_root/stdout" 2>"$fixture_root/stderr"; then
    echo "physical-plan dependency boundary rejected a legal graph: $fixture_root" >&2
    cat "$fixture_root/stderr" >&2
    exit 1
  fi
  grep -Fq "physical-plan dependency boundary: PASS" "$fixture_root/stdout"
}

assert_rejected() {
  local fixture_root="$1"
  shift

  if ! cargo generate-lockfile --offline --manifest-path "$fixture_root/Cargo.toml" \
      >"$fixture_root/lock-stdout" 2>"$fixture_root/lock-stderr"; then
    echo "failed to generate mutation fixture lockfile: $fixture_root" >&2
    cat "$fixture_root/lock-stderr" >&2
    exit 1
  fi
  if python3 "$CHECKER" --manifest-path "$fixture_root/Cargo.toml" \
      >"$fixture_root/stdout" 2>"$fixture_root/stderr"; then
    echo "physical-plan dependency boundary mutation was accepted: $fixture_root" >&2
    exit 1
  fi
  local expected
  for expected in "$@"; do
    if ! grep -Fq "$expected" "$fixture_root/stderr"; then
      echo "missing expected violation in $fixture_root/stderr: $expected" >&2
      cat "$fixture_root/stderr" >&2
      exit 1
    fi
  done
}

# The current repository is the production accept path.
python3 "$CHECKER" --manifest-path "$REPO_ROOT/Cargo.toml" >"$tmpdir/repo-stdout"
grep -Fq "physical-plan dependency boundary: PASS" "$tmpdir/repo-stdout"

# The minimal legal graph proves the direct contract allow-list and the neutral
# Connector contract's bytes carrier edge.
baseline_root="$tmpdir/baseline"
write_fixture "$baseline_root"
cargo generate-lockfile --offline --manifest-path "$baseline_root/Cargo.toml" \
  >"$baseline_root/lock-stdout" 2>"$baseline_root/lock-stderr"
cargo tree --package feature-user --edges normal --locked --offline \
  --prefix none --format '{p}' --manifest-path "$baseline_root/Cargo.toml" \
  >"$baseline_root/feature-user-tree"
grep -Fq "serde v" "$baseline_root/feature-user-tree"
grep -Fq "bytes v1.11.0 ($baseline_root/crates/bytes-v2)" \
  "$baseline_root/feature-user-tree"
assert_accepted "$baseline_root"

# The Connector contract may consume the lower-level type vocabulary. This
# accepted direction must not make the inverse dependency legal.
connector_to_type_root="$(new_mutation connector-to-type)"
append_dependency "$connector_to_type_root" connector-contract \
  'novarocks-type-contract = { path = "../type-contract" }'
assert_accepted "$connector_to_type_root"

# A forbidden application owner declared directly must be rejected.
direct_root="$(new_mutation direct-application)"
append_dependency "$direct_root" physical-plan \
  'novarocks-sql = { path = "../sql" }'
assert_rejected "$direct_root" \
  "declares internal normal dependencies outside the direct allow-list" \
  "novarocks-sql"

# A forbidden owner reached through an allowed neutral crate must be rejected.
transitive_root="$(new_mutation transitive-execution)"
append_dependency "$transitive_root" type-contract \
  'novarocks-execution = { path = "../execution" }'
assert_rejected "$transitive_root" \
  "resolved normal dependency closure contains forbidden application/execution owner" \
  "novarocks-execution"

# The foundational type vocabulary cannot depend upward on Connector identity.
reverse_contract_root="$(new_mutation reverse-contract-direction)"
append_dependency "$reverse_contract_root" type-contract \
  'novarocks-connector-contract = { path = "../connector-contract" }'
assert_rejected "$reverse_contract_root" \
  "novarocks-type-contract declares normal dependencies outside its exact owner allow-list" \
  "novarocks-connector-contract"

# Cargo reports the canonical package name alongside the local crate rename.
# The guard must judge the former so aliases cannot hide a wire dependency.
renamed_root="$(new_mutation renamed-wire)"
append_dependency "$renamed_root" physical-plan \
  'wire_models = { package = "novarocks-proto-models", path = "../proto-models" }'
assert_rejected "$renamed_root" \
  "declared normal dependencies contains forbidden Native wire owner" \
  "novarocks-proto-models"

# This optional dependency is not enabled and therefore is absent from the
# resolved graph.  The declared dependency scan must still reject it.
optional_root="$(new_mutation optional-disabled-rpc)"
append_dependency "$optional_root" physical-plan \
  'rpc_runtime = { package = "tonic", path = "../tonic", optional = true }'
assert_rejected "$optional_root" \
  "declared normal dependencies contains forbidden wire/RPC capability" \
  "tonic"

# Even an allow-listed package cannot be hidden behind a physical-plan feature.
physical_optional_root="$(new_mutation physical-plan-optional)"
append_dependency "$physical_optional_root" physical-plan \
  'optional_arrow = { package = "arrow-schema", version = "=58.2.0", optional = true }'
assert_rejected "$physical_optional_root" \
  "declares optional dependencies, but the physical-plan contract requires one closed dependency surface" \
  "arrow-schema"

physical_target_root="$(new_mutation physical-plan-target)"
append_dependency_section "$physical_target_root" physical-plan \
  'target.'"'"'cfg(unix)'"'"'.dependencies' \
  'bytes = "=1.11.0"'
assert_rejected "$physical_target_root" \
  "declares target-specific dependencies, but the physical-plan contract must be target invariant" \
  "bytes"

physical_feature_root="$(new_mutation physical-plan-feature)"
append_dependency_section "$physical_feature_root" physical-plan features \
  'alternate = []'
assert_rejected "$physical_feature_root" \
  "declares Cargo features, but the physical-plan contract requires one closed dependency surface" \
  "alternate"

dependency_feature_root="$(new_mutation physical-plan-dependency-feature)"
replace_text "$dependency_feature_root/crates/physical-plan/Cargo.toml" \
  'arrow-schema = "=58.2.0"' \
  'arrow-schema = { version = "=58.2.0", features = ["canonical_extension_types"] }'
assert_rejected "$dependency_feature_root" \
  "novarocks-physical-plan enables dependency features, but its dependency semantics must be invariant" \
  "arrow-schema=[canonical_extension_types]"

dependency_default_root="$(new_mutation physical-plan-dependency-default-features)"
replace_text "$dependency_default_root/crates/physical-plan/Cargo.toml" \
  'arrow-schema = "=58.2.0"' \
  'arrow-schema = { version = "=58.2.0", default-features = false }'
assert_rejected "$dependency_default_root" \
  "novarocks-physical-plan disables dependency default features" \
  "arrow-schema"

# Build dependencies are code that executes while compiling the contract. They
# are forbidden even when the package is legal as a normal dependency.
build_dependency_root="$(new_mutation build-dependency)"
append_dependency_section "$build_dependency_root" physical-plan \
  build-dependencies \
  'bytes = "=1.11.0"'
assert_rejected "$build_dependency_root" \
  "declares build dependencies, but the physical-plan contract permits none" \
  "bytes"

# Test-only dependency kinds must not become a capability escape hatch. The
# physical-plan crate keeps contract tests within its production vocabulary.
dev_dependency_root="$(new_mutation dev-dependency)"
append_dependency_section "$dev_dependency_root" physical-plan \
  dev-dependencies \
  'bytes = "=1.11.0"'
assert_rejected "$dev_dependency_root" \
  "declares dev dependencies, but the physical-plan contract permits none" \
  "bytes"

# A build script is executable build-time authority even without a
# [build-dependencies] section.
custom_build_root="$(new_mutation custom-build-target)"
: >"$custom_build_root/crates/physical-plan/build.rs"
assert_rejected "$custom_build_root" \
    "declares a custom build target" \
    "build.rs"

# A neutral dependency's build script executes while compiling physical-plan
# and therefore belongs to the guarded capability closure too.
transitive_custom_build_root="$(new_mutation transitive-custom-build-target)"
: >"$transitive_custom_build_root/crates/connector-contract/build.rs"
assert_rejected "$transitive_custom_build_root" \
  "novarocks-connector-contract declares a custom build target" \
  "build.rs"

# Build dependencies of an allowed neutral dependency are invisible to a
# normal-only dependency tree but still execute in the physical-plan build.
transitive_build_dependency_root="$(new_mutation transitive-build-dependency)"
append_dependency_section "$transitive_build_dependency_root" type-contract \
  build-dependencies \
  'bytes = "=1.11.0"'
assert_rejected "$transitive_build_dependency_root" \
  "novarocks-type-contract declares build dependencies" \
  "bytes"

# Test-only edges on a neutral contract compile in repository test contexts and
# therefore cannot bypass the same owner boundary.
transitive_dev_dependency_root="$(new_mutation transitive-dev-dependency)"
append_dependency_section "$transitive_dev_dependency_root" connector-contract \
  dev-dependencies \
  'novarocks-execution = { path = "../execution" }'
assert_rejected "$transitive_dev_dependency_root" \
  "novarocks-connector-contract declared dev dependencies contains forbidden application/execution owner" \
  "novarocks-connector-contract declares dev dependencies, but the physical-plan closure permits none" \
  "novarocks-execution"

# The local neutral contracts have one invariant dependency surface. Optional
# and target-specific edges cannot create unaudited feature/target closures.
transitive_optional_root="$(new_mutation transitive-optional)"
append_dependency "$transitive_optional_root" connector-contract \
  'hyper = { path = "../hyper", optional = true }'
assert_rejected "$transitive_optional_root" \
  "novarocks-connector-contract declares optional normal dependencies" \
  "hyper"

transitive_target_root="$(new_mutation transitive-target)"
append_dependency_section "$transitive_target_root" type-contract \
  'target.'"'"'cfg(unix)'"'"'.dependencies' \
  'bytes = "=1.11.0"'
assert_rejected "$transitive_target_root" \
  "novarocks-type-contract declares target-specific normal dependencies" \
  "bytes"

# A patched external package must retain both checks: its target-specific
# dependencies are inspected, and its local identity cannot impersonate the
# audited registry package.
selected_target_root="$(new_mutation selected-external-target)"
append_dependency_section "$selected_target_root" arrow-schema-v2 \
  'target.'"'"'cfg(target_os="windows")'"'"'.dependencies' \
  'hyper = { path = "../hyper" }'
{
  printf '\n[patch.crates-io]\n'
  printf 'arrow-schema = { path = "crates/arrow-schema-v2" }\n'
} >>"$selected_target_root/Cargo.toml"
assert_rejected "$selected_target_root" \
  "resolved normal dependency arrow-schema declares target-specific normal dependencies outside the exact audited closure allow-list" \
  "resolved normal dependency closure contains package identities outside the exact audited allow-list" \
  "crates/arrow-schema-v2/Cargo.toml" \
  "hyper"

# An inactive target edge must resolve to its exact package identity. Reusing
# an allow-listed name cannot hide a different package with build authority.
selected_target_identity_root="$(new_mutation selected-external-target-identity)"
append_dependency_section "$selected_target_identity_root" arrow-schema-v2 \
  'target.'"'"'cfg(target_os="windows")'"'"'.dependencies' \
  'bytes_alt = { package = "bytes", path = "../bytes-v2" }'
: >"$selected_target_identity_root/crates/bytes-v2/build.rs"
{
  printf '\n[patch.crates-io]\n'
  printf 'arrow-schema = { path = "crates/arrow-schema-v2" }\n'
} >>"$selected_target_identity_root/Cargo.toml"
assert_rejected "$selected_target_identity_root" \
  "resolved normal dependency closure contains package identities outside the exact audited allow-list" \
  "bytes declares a custom build target" \
  "build.rs"

transitive_feature_root="$(new_mutation transitive-feature)"
append_dependency_section "$transitive_feature_root" connector-contract features \
  'extra = []'
assert_rejected "$transitive_feature_root" \
  "novarocks-connector-contract declares Cargo features" \
  "extra"

# Exact closure admission catches packages such as hyper that do not appear in
# the descriptive capability deny-list.
unknown_transitive_root="$(new_mutation unknown-transitive-runtime)"
append_dependency "$unknown_transitive_root" type-contract \
  'hyper = { path = "../hyper" }'
assert_rejected "$unknown_transitive_root" \
  "resolved normal dependency closure contains package identities outside the exact audited allow-list" \
  "hyper"

# Another workspace member selects a local package with the audited bytes name
# and version. It is absent from physical-plan's package-selected closure, so
# the accepted baseline above proves that identity checks do not scan unrelated
# packages by name. Selecting that same dependency from the Connector contract
# must fail even though it has no build script, dependencies, or forbidden name.
same_name_identity_root="$(new_mutation selected-same-name-bytes)"
replace_text "$same_name_identity_root/crates/connector-contract/Cargo.toml" \
  'bytes = "=1.11.0"' \
  'bytes = { path = "../bytes-v2" }'
assert_rejected "$same_name_identity_root" \
  "resolved normal dependency closure contains package identities outside the exact audited allow-list" \
  "bytes v1.11.0" \
  "crates/bytes-v2/Cargo.toml"

# Only arrow-schema is part of the direct pure-contract vocabulary. A broader
# Arrow runtime dependency must not enter through a package that is absent from
# the capability deny-list.
arrow_runtime_root="$(new_mutation direct-arrow-runtime)"
append_dependency "$arrow_runtime_root" physical-plan \
  'arrow-array = { path = "../arrow-array" }'
assert_rejected "$arrow_runtime_root" \
  "declares packages outside the exact direct allow-list" \
  "arrow-array"

echo "physical-plan-dependency-boundary-test: PASS"
