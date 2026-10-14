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


# Build inputs are explicit; self-report does not certify final dependency settings.
ci_validate_build_environment() {
  python3 - <<'PYENV'
import os, sys
unsupported = [key for key in ("RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "RUSTC_WRAPPER", "RUSTC_WORKSPACE_WRAPPER", "CARGO_BUILD_TARGET", "CARGO_BUILD_RUSTFLAGS", "RUSTC") if os.environ.get(key)]
for key, value in os.environ.items():
    if key.startswith("CARGO_PROFILE_") and not (key == "CARGO_PROFILE_RELEASE_DEBUG_ASSERTIONS" and value == "false"):
        unsupported.append(key)
    if key.startswith("CARGO_TARGET_") and key.endswith(("_RUSTFLAGS", "_RUSTC_WRAPPER")) and value:
        unsupported.append(key)
if unsupported:
    print("error: unsupported compiler/profile overrides: " + ", ".join(sorted(set(unsupported))), file=sys.stderr)
    sys.exit(2)
PYENV
}

ci_profile_directory() {
  case "$1" in
    dev) printf '%s\n' debug ;;
    dev-opt|release) printf '%s\n' "$1" ;;
    *) echo "error: unsupported CI Cargo profile" >&2; return 2 ;;
  esac
}

ci_target_directory() {
  local kind="$1" root="${2:-${REPO_ROOT:-$PWD}}"
  local path
  case "$kind" in
    normal) path="${NOVA_CI_NORMAL_TARGET_DIR:-$root/target/normal}" ;;
    test-controls) path="${NOVA_CI_CONTROL_TARGET_DIR:-$root/target/test-controls}" ;;
    variants) path="${NOVA_CI_VARIANT_TARGET_DIR:-$root/target/variants}" ;;
    *) echo "error: unsupported CI target kind" >&2; return 2 ;;
  esac
  python3 - "$path" "$root" <<'PYPATH'
from pathlib import Path
import sys
path = Path(sys.argv[1])
print((path if path.is_absolute() else Path(sys.argv[2]) / path).resolve())
PYPATH
}

ci_validate_target_separation() {
  local normal controls variants
  normal="$(ci_target_directory normal)" || return $?
  controls="$(ci_target_directory test-controls)" || return $?
  variants="$(ci_target_directory variants)" || return $?
  python3 - "$normal" "$controls" "$variants" <<'PYTARGET'
from pathlib import Path
import sys
roots = list(map(Path, sys.argv[1:]))
for i, left in enumerate(roots):
    for right in roots[i + 1:]:
        if left == right or left in right.parents or right in left.parents:
            print("error: normal, controls and variant Cargo targets must be independent", file=sys.stderr)
            sys.exit(2)
PYTARGET
}

ci_artifact_path() {
  local directory
  directory="$(ci_profile_directory "$2")" || return $?
  python3 - "$1" "$directory" "$3" <<'PYPATH'
from pathlib import Path
import sys
print((Path(sys.argv[1]) / sys.argv[2] / sys.argv[3]).resolve())
PYPATH
}

ci_build_server() {
  local target_dir="$1" profile="$2" kind="$3"
  local variant="${4:-}" identity="${5:-}"
  local features
  ci_validate_build_environment || return $?
  ci_profile_directory "$profile" >/dev/null || return $?
  case "$kind" in
    normal-build)
      [ -z "$variant" ] || { echo "error: normal build cannot carry a control variant" >&2; return 2; }
      features="jemalloc,normal-build" ;;
    test-controls) features="jemalloc,test-controls" ;;
    *) echo "error: unsupported Server build kind" >&2; return 2 ;;
  esac
  if [ -n "$variant" ]; then features="$features,$variant"; fi
  if [ -n "$identity" ]; then
    env NOVAROCKS_BUILD_PROFILE="$profile" NOVAROCKS_NATIVE_BUILD_IDENTITY="$identity" \
      cargo build -p novarocks-server --bin novarocks --no-default-features --features "$features" \
      --profile "$profile" --locked --target-dir "$target_dir"
  else
    env -u NOVAROCKS_NATIVE_BUILD_IDENTITY NOVAROCKS_BUILD_PROFILE="$profile" \
      cargo build -p novarocks-server --bin novarocks --no-default-features --features "$features" \
      --profile "$profile" --locked --target-dir "$target_dir"
  fi
}

# Keep actual exit and streams even when parsing or semantic checks fail.
ci_capture_build_info() {
  local binary="$1" kind="$2" profile="$3" evidence="$4"
  local variant="${5:-}" identity="${6:-}" code=0
  mkdir -p "$(dirname "$evidence")" || return $?
  "$binary" --build-info >"$evidence.stdout.json" 2>"$evidence.stderr.log" || code=$?
  printf '%s\n' "$code" >"$evidence.exit"
  [ "$code" -eq 0 ] || return "$code"
  python3 - "$evidence.stdout.json" "$kind" "$profile" "$variant" "$identity" <<'PYINFO'
import json, sys
try:
    value = json.load(open(sys.argv[1]))
    kind, profile, variant, identity = sys.argv[2:]
    assert value["schema_version"] == 1
    assert value["build_kind"] == kind
    assert value["declared_profile"] == profile
    cfg = value["actual_cfg"]
    assert type(cfg["debug_assertions"]) is bool
    assert cfg["debug_assertions"] == (profile != "release")
    assert cfg["panic"] in ("unwind", "abort")
    assert cfg["target"] and cfg["pointer_width"] in (32, 64)
    assert cfg["target_os"] and cfg["target_arch"]
    owners = value["capabilities"]
    assert owners and len({entry["owner"] for entry in owners}) == len(owners)
    if kind in ("normal-build", "runner"):
        assert all(entry["compiled_capabilities"] == [] for entry in owners)
    else:
        assert kind == "test-controls" and value["package"] == "novarocks-server"
        assert all("test-controls" in entry["compiled_capabilities"] for entry in owners)
        server = next(entry for entry in owners if entry["owner"] == "novarocks-server")
        assert set(server["compiled_capabilities"]) == {"test-controls"} | ({variant} if variant else set())
    if identity:
        assert value["explicit_build_identity"] == identity
except (ValueError, KeyError, AssertionError, StopIteration, TypeError):
    print("error: binary build-info does not match selected kind, capabilities, profile or identity", file=sys.stderr)
    sys.exit(1)
PYINFO
}

ci_capture_normal_builds() {
  local target="$1" profile="$2" evidence="$3" binary kind
  for binary in novarocks novarocks-system-tests novarocks-sql-test; do
    kind=runner
    [ "$binary" != novarocks ] || kind=normal-build
    ci_capture_build_info "$(ci_artifact_path "$target" "$profile" "$binary")" "$kind" "$profile" "$evidence/$binary" || return $?
  done
}

ci_build_base_controls() {
  local target="$1" profile="$2" evidence="$3"
  ci_build_server "$target" "$profile" test-controls &&
  ci_capture_build_info "$(ci_artifact_path "$target" "$profile" novarocks)" test-controls "$profile" "$evidence"
}

ci_validate_build_inputs() {
  ci_validate_build_environment &&
  ci_validate_target_separation &&
  ci_profile_directory "$1" >/dev/null
}
