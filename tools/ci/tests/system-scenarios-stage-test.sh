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

# Behaviour tests for the System Scenario stage. A fake runner stands in for
# `novarocks-system-tests` so the stage's selection, ordering, reporting and
# fail-fast semantics are asserted without launching a real cluster.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"

source "$REPO_ROOT/tools/ci/lib/logging.sh"
source "$REPO_ROOT/tools/ci/lib/command.sh"
source "$REPO_ROOT/tools/ci/lib/system_scenarios.sh"

tmpdir="$(mktemp -d)"
tmpdir="$(cd "$tmpdir" && pwd -P)"
trap 'rm -rf "$tmpdir"' EXIT

fake_runner="$tmpdir/fake-runner.sh"
cat >"$fake_runner" <<'RUNNER'
#!/usr/bin/env bash
set -uo pipefail

if [ "${1:-}" = "--list-default" ] || [ "${1:-}" = "--list" ]; then
  if [ -n "${FAKE_RUNNER_LIST_FAILS:-}" ]; then
    echo "registry exploded" >&2
    exit 3
  fi
  if [ -n "${FAKE_RUNNER_SCENARIOS+set}" ]; then
    printf "%s" "$FAKE_RUNNER_SCENARIOS"
  else
    printf "alpha/one\nbeta/two\nbeta/three\n"
  fi
  if [ "${1:-}" = "--list" ]; then
    printf "explicit/performance\n"
  fi
  exit 0
fi

printf '%s\n' "$@" >>"$FAKE_RUNNER_CALLS.args"
only=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --only) only="$2"; shift 2 ;;
    *) shift ;;
  esac
done

echo "invoked --only $only"
echo "$only" >>"$FAKE_RUNNER_CALLS"

if [ "$only" = "${FAKE_RUNNER_FAIL_ON:-}" ]; then
  echo "scenario=$only FAILED; rerun: novarocks-system-tests --only $only" >&2
  exit 1
fi

echo "scenario=$only PASS"
exit 0
RUNNER
chmod +x "$fake_runner"

fail() {
  echo "FAIL: $1" >&2
  exit 1
}

reset_stage() {
  ci_init_summary_state
  CI_RUN_DIR="$tmpdir/run"
  CI_SUMMARY="$CI_RUN_DIR/summary.md"
  rm -rf "$CI_RUN_DIR"
  mkdir -p "$CI_RUN_DIR"
  export FAKE_RUNNER_CALLS="$tmpdir/calls.txt"
  : >"$FAKE_RUNNER_CALLS"
  unset FAKE_RUNNER_LIST_FAILS FAKE_RUNNER_FAIL_ON FAKE_RUNNER_SCENARIOS
}

primary_binary="$tmpdir/novarocks"
compatible_binary="$tmpdir/novarocks-compatible"
other_island_binary="$tmpdir/novarocks-other-island"
printf 'primary\n' >"$primary_binary"
printf 'compatible\n' >"$compatible_binary"
printf 'other-island\n' >"$other_island_binary"
chmod +x "$primary_binary" "$compatible_binary" "$other_island_binary"

# --- 1. every default scenario runs, in registry order, one --only each ----
primary_binary="$tmpdir/novarocks"
compatible_binary="$tmpdir/novarocks-compatible"
other_island_binary="$tmpdir/novarocks-other-island"
printf 'primary\n' >"$primary_binary"
printf 'compatible\n' >"$compatible_binary"
printf 'other-island\n' >"$other_island_binary"
chmod +x "$primary_binary" "$compatible_binary" "$other_island_binary"

reset_stage
all_list="$("$fake_runner" --list)"
case "$all_list" in
  *"explicit/performance"*) ;;
  *) fail "the complete registry must expose explicit stages" ;;
esac
default_list="$(ci_system_scenario_list "$fake_runner")"
case "$default_list" in
  *"explicit/performance"*) fail "CI discovery must exclude explicit stages" ;;
esac
if ! ci_run_system_scenarios "$fake_runner" "$primary_binary" \
  "$compatible_binary" "$other_island_binary" "$tmpdir/base.toml" \
  "$tmpdir/artifacts" 3 300 "$tmpdir/controls"; then
  fail "all-pass run should succeed"
fi

expected_calls="alpha/one
beta/two
beta/three"
actual_calls="$(cat "$FAKE_RUNNER_CALLS")"
[ "$actual_calls" = "$expected_calls" ] ||
  fail "expected one --only call per scenario in registry order, got: $actual_calls"

for scenario in "alpha/one" "beta/two" "beta/three"; do
  case "$CI_SYSTEM_ROWS" in
    *"| $scenario | PASS |"*) ;;
    *) fail "missing PASS summary row for $scenario" ;;
  esac
done

# --- 2. the stage forwards the frozen 1FE+NBE launch inputs -----------------
grep -q -- "--cluster-size 3" "$CI_RUN_DIR/system/alpha-one.log" ||
  fail "cluster size must reach the runner"
grep -q -- "--artifact-root" "$CI_RUN_DIR/system/alpha-one.log" ||
  fail "per-scenario artifact root must reach the runner"
grep -q -- "--compatible-binary $compatible_binary" "$CI_RUN_DIR/system/alpha-one.log" ||
  fail "compatible binary must reach every scenario invocation"
grep -q -- "--other-island-binary $other_island_binary" "$CI_RUN_DIR/system/alpha-one.log" ||
  fail "other-island binary must reach every scenario invocation"
[ -d "$tmpdir/artifacts/beta-two" ] ||
  fail "each scenario needs its own artifact directory"

# --- 3. summary renders an independent System Scenarios section --------------
ci_render_summary "PASS"
grep -q "^## System Scenarios$" "$CI_SUMMARY" ||
  fail "summary must contain a System Scenarios section"
grep -q "^| Scenario | Status | Duration | Log | Artifact |$" "$CI_SUMMARY" ||
  fail "System Scenarios table needs scenario/status/duration/log/artifact columns"
grep -q "| beta/three | PASS |" "$CI_SUMMARY" ||
  fail "summary must list each scenario row"

# --- 4. first failure stops the stage; later scenarios are not faked green ---
reset_stage
export FAKE_RUNNER_FAIL_ON="beta/two"
if ci_run_system_scenarios "$fake_runner" "$primary_binary" \
  "$compatible_binary" "$other_island_binary" "$tmpdir/base.toml" \
  "$tmpdir/artifacts" 3 300 "$tmpdir/controls"; then
  fail "a failing scenario must fail the stage"
fi

actual_calls="$(cat "$FAKE_RUNNER_CALLS")"
[ "$actual_calls" = "alpha/one
beta/two" ] || fail "stage must stop at the first failure, got: $actual_calls"

case "$CI_SYSTEM_ROWS" in
  *"| beta/two | FAIL |"*) ;;
  *) fail "failing scenario needs a FAIL row" ;;
esac
case "$CI_SYSTEM_ROWS" in
  *"beta/three"*) fail "scenarios after the failure must not be reported at all" ;;
esac
case "$CI_FAILURE_TAIL" in
  *"rerun: novarocks-system-tests --only beta/two"*) ;;
  *) fail "failure tail must preserve the runner's exact rerun command" ;;
esac

# --- 5. a broken or empty registry fails loudly ------------------------------
reset_stage
export FAKE_RUNNER_LIST_FAILS=1
if ci_run_system_scenarios "$fake_runner" "$primary_binary" \
  "$compatible_binary" "$other_island_binary" "$tmpdir/base.toml" \
  "$tmpdir/artifacts" 3 300 "$tmpdir/controls"; then
  fail "scenario discovery failure must fail the stage"
fi

reset_stage
export FAKE_RUNNER_SCENARIOS=""
if ci_run_system_scenarios "$fake_runner" "$primary_binary" \
  "$compatible_binary" "$other_island_binary" "$tmpdir/base.toml" \
  "$tmpdir/artifacts" 3 300 "$tmpdir/controls"; then
  fail "an empty registry must fail the stage rather than silently pass"
fi

# --- 6. an explicit skip is recorded as SKIP, never omitted ------------------
reset_stage
ci_record_system_scenario "alpha/one" "SKIP" "0" "" "-"
ci_render_summary "PASS"
grep -q "| alpha/one | SKIP |" "$CI_SUMMARY" ||
  fail "an explicitly skipped scenario must appear as SKIP in the summary"

# --- 7. isolated targets and actual self-report, including build failure ---
fake_bin="$tmpdir/bin"
mkdir -p "$fake_bin"
cat >"$fake_bin/cargo" <<'CARGO'
#!/usr/bin/env bash
set -euo pipefail
printf '%s|%s|%s\n' "$NOVAROCKS_BUILD_PROFILE" "${NOVAROCKS_NATIVE_BUILD_IDENTITY:-}" "$*" >>"$FAKE_CARGO_CALLS"
if [[ "$*" = *"native-compatibility-test-fixture"* ]] && [ -n "${FAKE_CARGO_FAIL_OTHER:-}" ]; then exit 7; fi
target="" profile="" features=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --target-dir) target="$2"; shift 2 ;;
    --profile) profile="$2"; shift 2 ;;
    --features) features="$2"; shift 2 ;;
    *) shift ;;
  esac
done
[ "$profile" != dev ] || profile=debug
mkdir -p "$target/$profile"
python3 - "$target/$profile/novarocks" "$NOVAROCKS_BUILD_PROFILE" "$features" "${NOVAROCKS_NATIVE_BUILD_IDENTITY:-}" <<'FAKEINFO'
from pathlib import Path
import sys, json
path, profile, features, identity = sys.argv[1:]
caps = [feature for feature in features.split(',') if feature != 'jemalloc']
kind = 'normal-build' if 'normal-build' in caps else 'test-controls'
if kind == 'normal-build': caps = []
value = dict(schema_version=1, package='novarocks-server', build_kind=kind,
 capabilities=[dict(owner='novarocks-server', compiled_capabilities=caps)],
 declared_profile=profile, explicit_build_identity=identity or None,
 actual_cfg=dict(debug_assertions=profile != 'release', panic='unwind', target='fixture-target', pointer_width=64, target_os='fixture-os', target_arch='fixture-arch'))
Path(path).write_text('#!/usr/bin/env python3\nimport json\nprint(' + repr(json.dumps(value)) + ')\n')
Path(path).chmod(0o755)
FAKEINFO
CARGO
chmod +x "$fake_bin/cargo"
export FAKE_CARGO_CALLS="$tmpdir/cargo-calls"
: >"$FAKE_CARGO_CALLS"
export NOVA_CI_NORMAL_TARGET_DIR="$tmpdir/normal"
export NOVA_CI_CONTROL_TARGET_DIR="$tmpdir/controls-target"
export NOVA_CI_VARIANT_TARGET_DIR="$tmpdir/variants"
PATH="$fake_bin:$PATH" ci_build_server "$NOVA_CI_NORMAL_TARGET_DIR" release normal-build || fail "normal fixture build failed"
fixture_binary="$(ci_artifact_path "$NOVA_CI_NORMAL_TARGET_DIR" release novarocks)"
fixture_root="$tmpdir/fixtures"
fixture_log="$tmpdir/fixture-build.log"
normal_hash="$(shasum -a 256 "$fixture_binary")"
PATH="$fake_bin:$PATH" ci_build_server "$NOVA_CI_CONTROL_TARGET_DIR" release test-controls || fail "base controls fixture build failed"
PATH="$fake_bin:$PATH" ci_prepare_system_scenario_binaries \
  "$fixture_binary" release "$fixture_root" deadbeef1234 "$fixture_log" || fail "fixture preparation should succeed"
[ "$(shasum -a 256 "$fixture_binary")" = "$normal_hash" ] || fail "normal artifact changed"
[ "$SYSTEM_SCENARIO_PRIMARY_BINARY" = "$fixture_binary" ] || fail "normal primary path changed"
[ "$SYSTEM_SCENARIO_CONTROL_BINARY" = "$tmpdir/controls-target/release/novarocks" ] || fail "controls target lost"
[ "$SYSTEM_SCENARIO_COMPATIBLE_BINARY" = "$tmpdir/variants/compatible/release/novarocks" ] || fail "compatible target lost"
[ "$SYSTEM_SCENARIO_OTHER_ISLAND_BINARY" = "$tmpdir/variants/other-island/dev-opt/novarocks" ] || fail "other-island must declare actual dev-opt profile"
grep -q -- '--no-default-features --features jemalloc,test-controls --profile release --locked --target-dir' "$FAKE_CARGO_CALLS" || fail "controls features lost"
grep -q -- 'dev-opt|ci-other-island-deadbeef1234|.*--features jemalloc,test-controls,native-compatibility-test-fixture --profile dev-opt' "$FAKE_CARGO_CALLS" || fail "other-island inputs lost"
export FAKE_CARGO_FAIL_OTHER=1
code=0
PATH="$fake_bin:$PATH" ci_prepare_system_scenario_binaries "$fixture_binary" release "$fixture_root" deadbeef1234 "$fixture_log" || code=$?
[ "$code" = 7 ] || fail "actual build failure exit lost"
[ "$(shasum -a 256 "$fixture_binary")" = "$normal_hash" ] || fail "failed fixture build changed normal"
unset FAKE_CARGO_FAIL_OTHER
if ci_capture_build_info "$fixture_binary" test-controls release "$tmpdir/wrong-kind" 2>/dev/null; then fail "normal self-report accepted as controls"; fi
if ci_capture_build_info "$fixture_binary" normal-build dev-opt "$tmpdir/wrong-profile" 2>/dev/null; then fail "wrong profile accepted"; fi
[ "$(cat "$tmpdir/wrong-profile.exit")" = 0 ] || fail "actual report exit lost"
cat >"$tmpdir/failed-info" <<'FAILINFO'
#!/usr/bin/env bash
echo failed >&2
exit 9
FAILINFO
chmod +x "$tmpdir/failed-info"
code=0
ci_capture_build_info "$tmpdir/failed-info" normal-build release "$tmpdir/failed-info-evidence" || code=$?
[ "$code" = 9 ] && [ "$(cat "$tmpdir/failed-info-evidence.exit")" = 9 ] || fail "build-info actual exit lost"
grep -qx failed "$tmpdir/failed-info-evidence.stderr.log" || fail "raw build-info stderr lost"
if RUSTFLAGS='unsupported' ci_validate_build_environment 2>/dev/null; then fail "unsupported flags accepted"; fi

# Compatibility explicitly consumes controls primary; ordinary still stays normal.
reset_stage
export FAKE_RUNNER_SCENARIOS=$'native-compatibility/mixed-build-same-island\nalpha/one\n'
: >"$FAKE_RUNNER_CALLS.args"
ci_run_system_scenarios "$fake_runner" "$primary_binary" "$compatible_binary" "$other_island_binary" "$tmpdir/base.toml" "$tmpdir/artifacts" 3 300 "$tmpdir/controls" || fail "compatibility wiring failed"
python3 - "$FAKE_RUNNER_CALLS.args" "$primary_binary" "$tmpdir/controls" <<'CHECKARGS'
import sys
args=open(sys.argv[1]).read().splitlines()
rows=[]
while args:
    i=args.index('--only')+2
    rows.append(args[:i]); args=args[i:]
for row in rows:
    get=lambda flag: row[row.index(flag)+1]
    assert get('--ordinary-binary') == sys.argv[2]
    assert get('--test-control-binary') == sys.argv[3]
    assert get('--binary') == (sys.argv[3] if get('--only').startswith('native-compatibility/') else sys.argv[2])
CHECKARGS
[ "$(ci_profile_directory dev)" = debug ] || fail "dev path mapping lost"

# Discovery retains explicit normal target/profile/lock; no product is launched.
source "$REPO_ROOT/tools/ci/lib/sql_suites.sh"
cat >"$fake_bin/cargo" <<'DISCOVERY'
#!/usr/bin/env bash
printf '%s\n' "$NOVAROCKS_BUILD_PROFILE" "$@" >"$FAKE_DISCOVERY_ARGS"
printf '%s\n' filter
DISCOVERY
chmod +x "$fake_bin/cargo"
export FAKE_DISCOVERY_ARGS="$tmpdir/discovery.args"
NOVA_CI_CARGO_PROFILE=release PATH="$fake_bin:$PATH" ci_discover_sql_suites "$REPO_ROOT" default >/dev/null
grep -qx release "$FAKE_DISCOVERY_ARGS" || fail "discovery profile lost"
grep -qx -- --locked "$FAKE_DISCOVERY_ARGS" || fail "discovery lock lost"
grep -qx "$NOVA_CI_NORMAL_TARGET_DIR" "$FAKE_DISCOVERY_ARGS" || fail "discovery normal target lost"
grep -qx -- --suite "$FAKE_DISCOVERY_ARGS" || fail "discovery default selection lost"
if NOVA_CI_CONTROL_TARGET_DIR="$NOVA_CI_NORMAL_TARGET_DIR" ci_validate_target_separation 2>/dev/null; then fail "overlapping targets accepted"; fi

echo "PASS: system scenario stage behaviour"
