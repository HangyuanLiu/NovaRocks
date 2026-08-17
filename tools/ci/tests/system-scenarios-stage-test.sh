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
trap 'rm -rf "$tmpdir"' EXIT

fake_runner="$tmpdir/fake-runner.sh"
cat >"$fake_runner" <<'RUNNER'
#!/usr/bin/env bash
set -uo pipefail

if [ "${1:-}" = "--list" ]; then
  if [ -n "${FAKE_RUNNER_LIST_FAILS:-}" ]; then
    echo "registry exploded" >&2
    exit 3
  fi
  if [ -n "${FAKE_RUNNER_SCENARIOS+set}" ]; then
    printf "%s" "$FAKE_RUNNER_SCENARIOS"
  else
    printf "alpha/one\nbeta/two\nbeta/three\n"
  fi
  exit 0
fi

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

# --- 1. every registered scenario runs, in registry order, one --only each ----
reset_stage
if ! ci_run_system_scenarios "$fake_runner" /bin/true "$tmpdir/base.toml" \
  "$tmpdir/artifacts" 3 300; then
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
if ci_run_system_scenarios "$fake_runner" /bin/true "$tmpdir/base.toml" \
  "$tmpdir/artifacts" 3 300; then
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
if ci_run_system_scenarios "$fake_runner" /bin/true "$tmpdir/base.toml" \
  "$tmpdir/artifacts" 3 300; then
  fail "scenario discovery failure must fail the stage"
fi

reset_stage
export FAKE_RUNNER_SCENARIOS=""
if ci_run_system_scenarios "$fake_runner" /bin/true "$tmpdir/base.toml" \
  "$tmpdir/artifacts" 3 300; then
  fail "an empty registry must fail the stage rather than silently pass"
fi

# --- 6. an explicit skip is recorded as SKIP, never omitted ------------------
reset_stage
ci_record_system_scenario "alpha/one" "SKIP" "0" "" "-"
ci_render_summary "PASS"
grep -q "| alpha/one | SKIP |" "$CI_SUMMARY" ||
  fail "an explicitly skipped scenario must appear as SKIP in the summary"

echo "PASS: system scenario stage behaviour"
