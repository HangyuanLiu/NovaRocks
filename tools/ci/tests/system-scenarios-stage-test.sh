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

# Behaviour tests exercise native selection and Cargo orchestration with fakes.
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
source "$REPO_ROOT/tools/ci/lib/logging.sh"
source "$REPO_ROOT/tools/ci/lib/command.sh"
source "$REPO_ROOT/tools/ci/lib/system_scenarios.sh"
source "$REPO_ROOT/tools/ci/lib/sql_suites.sh"
tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT
real_repo="$REPO_ROOT"
mkdir -p "$tmpdir/bin" "$tmpdir/repo/tools"
ln -s "$REPO_ROOT/tools/ci" "$tmpdir/repo/tools/ci"
REPO_ROOT="$tmpdir/repo"
export FAKE_CARGO_CALLS="$tmpdir/cargo.calls" FAKE_SCENARIO_CALLS="$tmpdir/scenario.calls"
export FAKE_SOURCE_ROOT="$real_repo"
cat >"$tmpdir/bin/cargo" <<'CARGO'
#!/usr/bin/env bash
set -euo pipefail
printf '%s|%s\n' "${NOVAROCKS_SERVER_BUILD_IDENTITY:-primary}" "$*" >>"$FAKE_CARGO_CALLS"
if [ -n "${FAKE_CARGO_FAILURE:-}" ]; then exit "$FAKE_CARGO_FAILURE"; fi
if [ -n "${FAKE_CARGO_FAIL_VARIANT:-}" ] && [ -n "${NOVAROCKS_SERVER_BUILD_IDENTITY:-}" ]; then exit 17; fi
python3 - "$@" <<'ARTIFACTS'
from pathlib import Path
import json, os, sys
args=sys.argv[1:]
target=Path(args[args.index('--target-dir')+1]) / 'debug'
target.mkdir(parents=True, exist_ok=True)
if args[0] != 'build':
    sys.exit(0)
names=['novarocks'] if '-p' in args else ['novarocks','novarocks-sql-test','novarocks-system-tests','novarocks-error-manifest']
for name in names:
    if name == os.environ.get('FAKE_CARGO_OMIT'): continue
    path=target/name
    if name == 'novarocks-system-tests':
        text=Path(os.environ['FAKE_RUNNER_TEMPLATE']).read_text()
    else:
        info=dict(schema_version=1, actual_cfg=dict(debug_assertions=True), capabilities=[dict(owner='fixture',compiled_capabilities=['test-controls'])], explicit_build_identity=os.environ.get('NOVAROCKS_SERVER_BUILD_IDENTITY'))
        text='#!/usr/bin/env python3\nimport json, sys\n' + ("if '--list-suites' in sys.argv:\n print('filter' if '--suite' in sys.argv else 'filter\\nssb')\n sys.exit(0)\n" if name == 'novarocks-sql-test' else '') + 'print(' + repr(json.dumps(info)) + ')\n'
    path.write_text(text); path.chmod(0o755)
    print(json.dumps(dict(reason='compiler-artifact', target=dict(name=name, kind=['bin']),profile=dict(test=False), executable=str(path))))
ARTIFACTS
CARGO
cat >"$tmpdir/runner" <<'RUNNER'
#!/usr/bin/env bash
set -euo pipefail
if [ "${1:-}" = --build-info ]; then
  echo '{"schema_version":1,"actual_cfg":{"debug_assertions":true},"capabilities":[{"owner":"runner","compiled_capabilities":["test-controls"]}]}'
  exit 0
fi
mode="${1:-}"; shift || true
case "$mode" in
  --list-default) [ -z "${FAKE_EMPTY_REGISTRY:-}" ] || exit 0; [ -z "${FAKE_REGISTRY_FAILURE:-}" ] || exit 19; printf 'alpha/one\nbeta/two\n'; exit ;;
  --list-compatibility|--list-binary-requirements)
    selection=()
    while [ "$#" -gt 0 ]; do
      [ "$1" = --only ] && [ "$#" -ge 2 ] || exit 2
      case "$2" in native-compatibility/raw|native-compatibility/mixed|native-compatibility/cutover) selection+=("$2"); shift 2 ;; alpha/one|beta/two) [ "$mode" != --list-compatibility ] || exit 2; selection+=("$2"); shift 2 ;; *) exit 2 ;; esac
    done
    if [ "$mode" = --list-compatibility ]; then
      if [ "${#selection[@]}" = 0 ]; then printf 'native-compatibility/raw\nnative-compatibility/mixed\nnative-compatibility/cutover\n'; else printf '%s\n' "${selection[@]}"; fi
    else
      echo primary
      for scenario in "${selection[@]}"; do
        case "$scenario" in *mixed) echo compatible ;; *cutover) printf 'compatible\nother-island\n' ;; esac
      done
    fi
    exit ;;
esac
set -- "$mode" "$@"
while [ "$#" -gt 0 ]; do
  case "$1" in
    --only) scenario="$2"; shift 2 ;;
    --binary|--compatible-binary|--other-island-binary) [ -x "$2" ] || exit 4; shift 2 ;;
    *) shift ;;
  esac
done
printf '%s\n' "$scenario" >>"$FAKE_SCENARIO_CALLS"
if [ "$scenario" = "${FAKE_SCENARIO_FAILURE:-}" ]; then echo 'original scenario error' >&2; exit 23; fi
RUNNER
chmod +x "$tmpdir/bin/cargo" "$tmpdir/runner"
export FAKE_RUNNER_TEMPLATE="$tmpdir/runner"
export PATH="$tmpdir/bin:$PATH"
export NOVA_CI_ARTIFACT_RECEIPT="$tmpdir/artifacts.json"
ci_prepare_main "$NOVA_CI_ARTIFACT_RECEIPT"
[ "$(wc -l <"$FAKE_CARGO_CALLS" | tr -d ' ')" = 1 ]
grep -F -- 'build --workspace --profile dev --locked' "$FAKE_CARGO_CALLS" >/dev/null
# Cargo options remain before libtest's separator.
ci_main_cargo test --tests narrowed -- --exact
python3 - "$FAKE_CARGO_CALLS" <<'ARGS'
import sys
args=open(sys.argv[1]).read().splitlines()[-1].split('|',1)[1].split()
assert args[:4] == ['test','--workspace','--profile','dev']
assert args[args.index('--')+1:] == ['--exact']
ARGS
: >"$FAKE_CARGO_CALLS"
printf 'primary|joint preparation\n' >"$FAKE_CARGO_CALLS"
binary="$(ci_bound_binary novarocks)"
runner="$(ci_bound_binary novarocks-system-tests)"
ci_capture_main_builds "$NOVA_CI_ARTIFACT_RECEIPT" "$tmpdir/info"
[ "$(ci_discover_sql_suites "$REPO_ROOT" default)" = filter ]
[ "$(ci_discover_sql_suites "$REPO_ROOT")" = $'filter\nssb' ]
CI_RUN_DIR="$tmpdir/run"; mkdir -p "$CI_RUN_DIR"
CI_SUMMARY="$CI_RUN_DIR/summary.md"
ci_init_summary_state
ci_select_system_scenarios "$runner" functional "$tmpdir/default" "$tmpdir/requirements"
ci_prepare_system_scenario_binaries "$binary" dev "$tmpdir/fixtures" test "$tmpdir/build.log" "$tmpdir/requirements"
[ "$(wc -l <"$FAKE_CARGO_CALLS" | tr -d ' ')" = 1 ]
ci_run_system_scenarios "$runner" "$binary" '' '' "$tmpdir/base" "$tmpdir/runtime" 3 30 "$tmpdir/default"
[ "$(cat "$FAKE_SCENARIO_CALLS")" = $'alpha/one\nbeta/two' ]

for choice in raw mixed cutover; do
  : >"$FAKE_CARGO_CALLS"
  ci_select_system_scenarios "$runner" compatibility "$tmpdir/selected" "$tmpdir/requirements" "native-compatibility/$choice"
  ci_prepare_system_scenario_binaries "$binary" dev "$tmpdir/fixtures-$choice" test "$tmpdir/build.log" "$tmpdir/requirements"
  case "$choice" in
    raw) expected=0 ;; mixed) expected=2 ;; cutover) expected=3 ;;
  esac
  [ "$(wc -l <"$FAKE_CARGO_CALLS" | tr -d ' ')" = "$expected" ]
  [ "$choice" != cutover ] || [ "$(grep -c '^ci-other-island-' "$FAKE_CARGO_CALLS")" = 1 ]
  ci_run_system_scenarios "$runner" "$SYSTEM_SCENARIO_PRIMARY_BINARY" "$SYSTEM_SCENARIO_COMPATIBLE_BINARY" "$SYSTEM_SCENARIO_OTHER_ISLAND_BINARY" "$tmpdir/base" "$tmpdir/runtime" 3 30 "$tmpdir/selected"
done
# Duplicate needs across selected initial and later phases prepare each variant once.
: >"$FAKE_CARGO_CALLS"
ci_select_system_scenarios "$runner" compatibility "$tmpdir/selected" "$tmpdir/requirements" native-compatibility/mixed native-compatibility/cutover
ci_prepare_system_scenario_binaries "$binary" dev "$tmpdir/union" test "$tmpdir/build.log" "$tmpdir/requirements"
[ "$(wc -l <"$FAKE_CARGO_CALLS" | tr -d ' ')" = 3 ]
for invalid in unknown alpha/one ''; do
  code=0
  ci_select_system_scenarios "$runner" compatibility "$tmpdir/selected" "$tmpdir/requirements" "$invalid" || code=$?
  [ "$code" = 2 ]
done
export FAKE_SCENARIO_FAILURE=beta/two
code=0
ci_run_system_scenarios "$runner" "$binary" '' '' "$tmpdir/base" "$tmpdir/failure" 3 30 "$tmpdir/default" || code=$?
[ "$code" = 23 ]
grep -q 'original scenario error' "$CI_RUN_DIR/system/beta-two.log"
unset FAKE_SCENARIO_FAILURE

# Registry failures and empty sets must not disappear as successful work.
export FAKE_REGISTRY_FAILURE=1
code=0; ci_select_system_scenarios "$runner" functional "$tmpdir/selected" "$tmpdir/requirements" || code=$?
[ "$code" = 19 ]
unset FAKE_REGISTRY_FAILURE
export FAKE_EMPTY_REGISTRY=1
code=0; ci_select_system_scenarios "$runner" functional "$tmpdir/selected" "$tmpdir/requirements" || code=$?
[ "$code" = 2 ]
unset FAKE_EMPTY_REGISTRY
# Required variants cannot be silently omitted or replaced by an old file.
ci_select_system_scenarios "$runner" compatibility "$tmpdir/selected" "$tmpdir/requirements" native-compatibility/mixed
if ci_run_system_scenarios "$runner" "$binary" '' '' "$tmpdir/base" "$tmpdir/missing-variant" 3 30 "$tmpdir/selected"; then exit 1; fi
export FAKE_CARGO_FAIL_VARIANT=1
code=0
ci_prepare_system_scenario_binaries "$binary" dev "$tmpdir/failed-variant" test "$tmpdir/failed-variant.log" "$tmpdir/requirements" || code=$?
[ "$code" = 17 ] && [ "$(cat "$tmpdir/failed-variant/main-restore.exit")" = 0 ]
ci_bound_binary novarocks >/dev/null
unset FAKE_CARGO_FAIL_VARIANT
# A later destination failure must restore the main graph after Compatible built.
(
  : >"$FAKE_CARGO_CALLS"
  ci_select_system_scenarios "$runner" compatibility "$tmpdir/mkdir-selected" "$tmpdir/mkdir-requirements" native-compatibility/cutover
  before_binding="$(python3 - "$NOVA_CI_ARTIFACT_RECEIPT" <<'BINDING'
import json, sys
print(json.load(open(sys.argv[1]))["novarocks"]["sha256"])
BINDING
)"
  failed_destination="$(ci_target_directory variants)/other-island"
  mkdir() {
    if [ "$#" -eq 2 ] && [ "$1" = -p ] && [ "$2" = "$failed_destination" ]; then
      echo 'synthetic later variant directory failure' >&2
      return 28
    fi
    command mkdir "$@"
  }
  code=0
  ci_prepare_system_scenario_binaries "$binary" dev "$tmpdir/mkdir-failure" test "$tmpdir/mkdir-failure.log" "$tmpdir/mkdir-requirements" || code=$?
  [ "$code" = 28 ]
  [ "$(cat "$tmpdir/mkdir-failure/main-restore.exit")" = 0 ]
  [ "$(wc -l <"$FAKE_CARGO_CALLS" | tr -d ' ')" = 2 ]
  grep -q '^ci-compatible-' "$FAKE_CARGO_CALLS"
  if grep -q '^ci-other-island-' "$FAKE_CARGO_CALLS"; then exit 1; fi
  ci_bound_binary novarocks >/dev/null
  "$binary" --build-info >"$tmpdir/mkdir-failure/restored-info.json"
  python3 - "$NOVA_CI_ARTIFACT_RECEIPT" "$before_binding" "$tmpdir/mkdir-failure/restored-info.json" <<'RESTORED'
import json, sys
assert json.load(open(sys.argv[1]))["novarocks"]["sha256"] == sys.argv[2]
assert json.load(open(sys.argv[3]))["explicit_build_identity"] is None
RESTORED
)
cat >"$tmpdir/failed-info" <<'INFO'
#!/usr/bin/env bash
echo 'actual report failure' >&2
exit 9
INFO
chmod +x "$tmpdir/failed-info"
code=0; ci_capture_build_info "$tmpdir/failed-info" "$tmpdir/failed-report" || code=$?
[ "$code" = 9 ] && [ "$(cat "$tmpdir/failed-report.exit")" = 9 ]
grep -qx 'actual report failure' "$tmpdir/failed-report.stderr.log"
cat >"$tmpdir/false-capability" <<'INFO'
#!/usr/bin/env bash
echo '{"schema_version":1,"actual_cfg":{"debug_assertions":true},"capabilities":[{"owner":"runner","compiled_capabilities":[]}]}'
INFO
chmod +x "$tmpdir/false-capability"
if ci_capture_build_info "$tmpdir/false-capability" "$tmpdir/false-report"; then exit 1; fi

# Missing programs, changed content and original Cargo exits remain failures.
export FAKE_CARGO_OMIT=novarocks-sql-test
if ci_prepare_main "$tmpdir/missing" 2>/dev/null; then exit 1; fi
unset FAKE_CARGO_OMIT
export FAKE_CARGO_FAILURE=7
code=0; ci_prepare_main "$tmpdir/failed" || code=$?
[ "$code" = 7 ] && [ "$(cat "$tmpdir/failed.cargo.exit")" = 7 ]
unset FAKE_CARGO_FAILURE
ci_prepare_main "$NOVA_CI_ARTIFACT_RECEIPT"
printf '# changed\n' >>"$binary"
if ci_bound_binary novarocks >/dev/null 2>&1; then exit 1; fi
if RUSTFLAGS=invalid ci_validate_build_environment 2>/dev/null; then exit 1; fi

# The standalone entry only prepares programs, selects, binds and runs its scope.
source "$real_repo/tools/ci/native-compatibility-ci.sh" --source-only
REPO_ROOT="$tmpdir/repo"; SCRIPT_DIR="$real_repo/tools/ci"
mkdir -p "$REPO_ROOT/logs"
git -C "$REPO_ROOT" init -q
git -C "$REPO_ROOT" -c user.name=Fixture -c user.email=fixture@example.invalid commit --allow-empty -qm fixture
: >"$FAKE_CARGO_CALLS"; : >"$FAKE_SCENARIO_CALLS"
main --only native-compatibility/raw >/dev/null
[ "$(wc -l <"$FAKE_CARGO_CALLS" | tr -d ' ')" = 1 ]
[ "$(cat "$FAKE_SCENARIO_CALLS")" = native-compatibility/raw ]
if grep -E '^.*\|(test|check|clippy) ' "$FAKE_CARGO_CALLS"; then exit 1; fi

# Invalid and failing standalone runs retain their original exit and scope.
: >"$FAKE_CARGO_CALLS"; : >"$FAKE_SCENARIO_CALLS"
code=0; main --only alpha/one >/dev/null || code=$?
[ "$code" = 2 ] && [ ! -s "$FAKE_SCENARIO_CALLS" ]
[ "$(wc -l <"$FAKE_CARGO_CALLS" | tr -d ' ')" = 1 ]
export FAKE_SCENARIO_FAILURE=native-compatibility/raw
code=0; main --only native-compatibility/raw >/dev/null || code=$?
[ "$code" = 23 ] && [ "$(cat "$CI_RUN_DIR/exit")" = 23 ]
unset FAKE_SCENARIO_FAILURE

echo 'system-scenarios-stage-test: PASS'
