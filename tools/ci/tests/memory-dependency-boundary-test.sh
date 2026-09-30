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

# Mutation fixtures use local path dependencies only. Cargo is forced offline
# for every fixture; none may contact a registry to exercise the graph guard.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
CHECKER="$REPO_ROOT/tools/ci/check-memory-dependency-boundary.py"
tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

write_package() {
  local fixture_root="$1" directory="$2" package_name="$3"
  local package_root="$fixture_root/crates/$directory"
  mkdir -p "$package_root/src"
  printf '[package]\nname = "%s"\nversion = "0.1.0"\nedition = "2024"\n\n[dependencies]\n' \
    "$package_name" >"$package_root/Cargo.toml"
  : >"$package_root/src/lib.rs"
}

append_dependency() {
  printf '%s\n' "$3" >>"$1/crates/$2/Cargo.toml"
}

append_dev_dependency() {
  printf '\n[dev-dependencies]\n%s\n' "$2" >>"$1/crates/memory/Cargo.toml"
}

write_fixture() {
  local fixture_root="$1"
  mkdir -p "$fixture_root"
  printf '[workspace]\nresolver = "2"\nmembers = ["crates/*"]\n' >"$fixture_root/Cargo.toml"
  write_package "$fixture_root" memory novarocks-memory
  write_package "$fixture_root" state-store-api novarocks-state-store-api
  write_package "$fixture_root" frontend novarocks-frontend-application
  write_package "$fixture_root" types novarocks-types
  write_package "$fixture_root" neutral-lib neutral-lib
  write_package "$fixture_root" arrow-buffer arrow-buffer
  write_package "$fixture_root" tokio tokio
  append_dependency "$fixture_root" frontend 'novarocks-memory = { path = "../memory" }'
}

new_mutation() {
  local mutation_root="$tmpdir/$1"
  cp -R "$baseline_root" "$mutation_root"
  rm -f "$mutation_root/Cargo.lock"
  printf '%s' "$mutation_root"
}

assert_accepted() {
  local fixture_root="$1"
  shift
  if ! CARGO_NET_OFFLINE=true "$CHECKER" --manifest-path "$fixture_root/Cargo.toml" \
      >"$fixture_root/stdout" 2>"$fixture_root/stderr"; then
    echo "memory dependency boundary rejected a legal graph: $fixture_root" >&2
    cat "$fixture_root/stderr" >&2
    exit 1
  fi
  grep -Fq "memory dependency boundary: PASS" "$fixture_root/stdout"
  local expected
  for expected in "$@"; do
    if ! grep -Fq "$expected" "$fixture_root/stdout"; then
      echo "missing expected report line: $expected" >&2
      cat "$fixture_root/stdout" >&2
      exit 1
    fi
  done
}

assert_rejected() {
  local fixture_root="$1"
  shift
  if CARGO_NET_OFFLINE=true "$CHECKER" --manifest-path "$fixture_root/Cargo.toml" \
      >"$fixture_root/stdout" 2>"$fixture_root/stderr"; then
    echo "memory dependency boundary mutation was accepted: $fixture_root" >&2
    exit 1
  fi
  local expected
  for expected in "$@"; do
    if ! grep -Fq "$expected" "$fixture_root/stderr"; then
      echo "missing expected violation: $expected" >&2
      cat "$fixture_root/stderr" >&2
      exit 1
    fi
  done
}

echo "asserting: the real workspace satisfies the memory dependency boundary"
"$CHECKER" --manifest-path "$REPO_ROOT/Cargo.toml" >"$tmpdir/repo-stdout"
grep -Fq "memory dependency boundary: PASS" "$tmpdir/repo-stdout"

echo "asserting: a neutral core and byte-oriented StateStore fixture pass"
baseline_root="$tmpdir/baseline"
write_fixture "$baseline_root"
assert_accepted "$baseline_root" "novarocks-state-store-api: memory crates in normal closure: none"

# Zero normal dependencies includes neutral utilities, not just known runtimes.
for capability in 'arrow-buffer' 'novarocks-types' 'neutral-lib'; do
  echo "asserting: core normal dependency on $capability is rejected"
  root="$(new_mutation "core-normal-$capability")"
  directory="$capability"
  if [ "$capability" = novarocks-types ]; then directory=types; fi
  append_dependency "$root" memory "$capability = { path = \"../$directory\" }"
  assert_rejected "$root" "novarocks-memory normal dependency closure must be empty" "$capability"
done

echo "asserting: an inactive optional core normal dependency is rejected"
root="$(new_mutation core-optional)"
append_dependency "$root" memory 'neutral-lib = { path = "../neutral-lib", optional = true }'
assert_rejected "$root" "novarocks-memory declares normal dependencies (neutral-lib)"

for capability in 'arrow-buffer' 'novarocks-types' 'tokio'; do
  echo "asserting: core direct dev dependency on $capability is rejected"
  root="$(new_mutation "core-dev-$capability")"
  directory="$capability"
  if [ "$capability" = novarocks-types ]; then directory=types; fi
  append_dev_dependency "$root" "$capability = { path = \"../$directory\" }"
  assert_rejected "$root" "novarocks-memory dev dependency closure contains a forbidden" "$capability"
done

for capability in 'arrow-buffer' 'novarocks-types' 'tokio'; do
  echo "asserting: core dev helper transitively reaching $capability is rejected"
  root="$(new_mutation "core-dev-transitive-$capability")"
  directory="$capability"
  if [ "$capability" = novarocks-types ]; then directory=types; fi
  append_dev_dependency "$root" 'neutral-lib = { path = "../neutral-lib" }'
  append_dependency "$root" neutral-lib "$capability = { path = \"../$directory\" }"
  assert_rejected "$root" "novarocks-memory dev dependency closure contains a forbidden" "$capability"
done

for capability in 'arrow-buffer' 'novarocks-types' 'tokio'; do
  echo "asserting: core dev helper's inactive optional $capability edge is rejected"
  root="$(new_mutation "core-dev-optional-$capability")"
  directory="$capability"
  if [ "$capability" = novarocks-types ]; then directory=types; fi
  append_dev_dependency "$root" 'neutral-lib = { path = "../neutral-lib" }'
  append_dependency "$root" neutral-lib "$capability = { path = \"../$directory\", optional = true }"
  assert_rejected "$root" "novarocks-memory declared dev dependency closure contains a forbidden" "$capability"
done

echo "asserting: neutral core dev tooling and byte-oriented storage utility are accepted"
root="$(new_mutation neutral-dev)"
append_dev_dependency "$root" 'neutral-lib = { path = "../neutral-lib" }'
append_dependency "$root" state-store-api 'neutral-lib = { path = "../neutral-lib" }'
assert_accepted "$root"

echo "asserting: build-only capabilities are reported without becoming a dev closure"
root="$(new_mutation build-reported)"
printf '\n[build-dependencies]\ntokio = { path = "../tokio" }\n' >>"$root/crates/memory/Cargo.toml"
assert_accepted "$root" "build-only reach, reported not enforced"

echo "asserting: StateStore direct memory dependency is rejected"
root="$(new_mutation storage-direct)"
append_dependency "$root" state-store-api 'novarocks-memory = { path = "../memory" }'
assert_rejected "$root" "novarocks-state-store-api normal dependency closure contains a memory crate: novarocks-memory"

echo "asserting: StateStore transitive memory dependency is rejected"
root="$(new_mutation storage-transitive)"
append_dependency "$root" state-store-api 'neutral-lib = { path = "../neutral-lib" }'
append_dependency "$root" neutral-lib 'novarocks-memory = { path = "../memory" }'
assert_rejected "$root" "novarocks-state-store-api normal dependency closure contains a memory crate: novarocks-memory"

echo "asserting: StateStore inactive optional memory dependency is rejected"
root="$(new_mutation storage-optional)"
append_dependency "$root" state-store-api 'novarocks-memory = { path = "../memory", optional = true }'
assert_rejected "$root" "novarocks-state-store-api declared normal dependency closure contains a memory crate: novarocks-memory"

echo "asserting: StateStore utility's inactive optional memory dependency is rejected"
root="$(new_mutation storage-transitive-optional)"
append_dependency "$root" state-store-api 'neutral-lib = { path = "../neutral-lib" }'
append_dependency "$root" neutral-lib 'novarocks-memory = { path = "../memory", optional = true }'
assert_rejected "$root" "novarocks-state-store-api declared normal dependency closure contains a memory crate: novarocks-memory"

echo "memory-dependency-boundary-test: PASS"
