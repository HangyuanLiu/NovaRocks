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

# Local-only checks. Missing tools/components fail; this script never installs them.
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: tools/ci/memory-model-checks.sh --loom|--miri|--all
Runs locked, offline local memory protocol checks; not part of default CI.
MIRI_TOOLCHAIN optionally selects an already installed nightly (default: nightly).
No Rust toolchain, component or Cargo dependency is downloaded by this script.
EOF
}
fail() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}
mode="${1:-}"
if [[ "$mode" == --help || "$mode" == -h ]]; then
  usage
  exit 0
fi
if [[ $# != 1 || ( "$mode" != --loom && "$mode" != --miri && "$mode" != --all ) ]]; then
  usage >&2
  exit 2
fi
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"
command -v cargo >/dev/null || fail 'cargo is unavailable; no toolchain will be installed.'
export CARGO_NET_OFFLINE=true
# Also prevent cargo's rustup proxy from fetching a selected missing toolchain.
export RUSTUP_AUTO_INSTALL=0

# Complete all requested prerequisites before starting any potentially long model.
miri_toolchain="${MIRI_TOOLCHAIN:-nightly}"
if [[ "$mode" == --miri || "$mode" == --all ]]; then
  command -v rustup >/dev/null || fail 'rustup is unavailable; no toolchain will be installed.'
  [[ "$miri_toolchain" == nightly || "$miri_toolchain" == nightly-* ]] || \
    fail "MIRI_TOOLCHAIN must select an installed nightly: $miri_toolchain"
  installed="$(rustup toolchain list)"
  found=false
  while IFS= read -r line; do
    name="${line%% *}"
    if [[ "$name" == "$miri_toolchain" || "$name" == "$miri_toolchain-"* ]]; then
      found=true
      break
    fi
  done <<< "$installed"
  [[ "$found" == true ]] || \
    fail "Miri toolchain $miri_toolchain is not installed; obtain authorization separately."
  components="$(rustup component list --installed --toolchain "$miri_toolchain")"
  for component in miri rust-src; do
    present=false
    while IFS= read -r line; do
      name="${line%% *}"
      if [[ "$name" == "$component" || "$name" == "$component-"* ]]; then
        present=true
        break
      fi
    done <<< "$components"
    [[ "$present" == true ]] || \
      fail "Miri requires installed $component for $miri_toolchain; obtain authorization separately."
  done
  # rustup run does not install an absent toolchain without its --install option.
  rustup run "$miri_toolchain" cargo miri --version
  # cargo-miri builds its sysroot from a temporary directory, where offline
  # resolution cannot see std's crates.io dependencies. rust-src ships them
  # vendored, with a Cargo source replacement under library/.cargo; preparing
  # the sysroot from inside that tree resolves std without fetching anything.
  miri_library="$(rustup run "$miri_toolchain" rustc --print sysroot)/lib/rustlib/src/rust/library"
  [[ -f "$miri_library/.cargo/config.toml" && -d "$miri_library/vendor" ]] || \
    fail "rust-src for $miri_toolchain lacks vendored std dependencies; cannot prepare an offline Miri sysroot."
  (cd "$miri_library" && rustup run "$miri_toolchain" cargo miri setup)
fi

if [[ "$mode" == --loom || "$mode" == --all ]]; then
  for model in lane_loom owner_loom ledger_loom; do
    printf 'Running bounded Loom models: %s\n' "$model"
    RUSTFLAGS="${RUSTFLAGS:+$RUSTFLAGS }--cfg loom" \
      cargo test -p novarocks-memory --lib --release --locked --offline \
      "$model" -- --test-threads=1
  done
fi
if [[ "$mode" == --miri || "$mode" == --all ]]; then
  # Attribution readouts stamp samples with SystemTime. Isolation only gates host
  # access (clocks, environment, files); it does not relax any UB check.
  export MIRIFLAGS="${MIRIFLAGS:+$MIRIFLAGS }-Zmiri-strict-provenance -Zmiri-symbolic-alignment-check -Zmiri-disable-isolation"
  for module in lane:: attribution::; do
    printf 'Running System-backend Miri library checks: %s\n' "$module"
    rustup run "$miri_toolchain" cargo miri test -p novarocks-memory \
      --lib --locked --offline "$module" -- --test-threads=1
  done
  # Select the actual source targets, including the standalone reconcile binary.
  for source in novarocks/memory/tests/attribution_*.rs novarocks/memory/tests/allocator_observation.rs; do
    target="${source##*/}"
    target="${target%.rs}"
    printf 'Running System-backend Miri integration checks: %s\n' "$target"
    rustup run "$miri_toolchain" cargo miri test -p novarocks-memory \
      --test "$target" --locked --offline -- --test-threads=1
  done
fi
