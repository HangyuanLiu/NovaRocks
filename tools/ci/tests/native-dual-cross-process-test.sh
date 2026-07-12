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

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
GATE="$REPO_ROOT/tools/ci/native-dual-cross-process.sh"
EXPECTED_HEAD="$(git -C "$REPO_ROOT" rev-parse HEAD)"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/novarocks-native-dual-contract.XXXXXX")"
TMP_DIR="$(cd "$TMP_DIR" && pwd -P)"
trap 'rm -rf "$TMP_DIR"' EXIT

BUILD_HOOK="$TMP_DIR/fake-build.sh"
RUNNER_HOOK="$TMP_DIR/fake-runner.sh"
CONTRACT_HOOK="$TMP_DIR/fake-contract.sh"
IDENTITY_HOOK="$TMP_DIR/fake-identity.sh"
CALLS="$TMP_DIR/runner-calls.tsv"
CONTRACT_CALLS="$TMP_DIR/contract-calls.txt"
IDENTITY_CALLS="$TMP_DIR/identity-calls.txt"
RUNTIME_DIR="$TMP_DIR/runtime"
SQL_CONFIG="$RUNTIME_DIR/sql-test.conf"
STANDALONE_CONFIG="$RUNTIME_DIR/standalone.toml"
RUNTIME_MANIFEST="$RUNTIME_DIR/manifest.json"

mkdir -p "$RUNTIME_DIR"
printf 'fixture=true\n' >"$SQL_CONFIG"
printf 'fixture=true\n' >"$STANDALONE_CONFIG"

write_manifest() {
  local path="$1"
  local workspace_root="$2"
  local runtime_dir="$3"
  local sql_config="$4"
  local standalone_config="$5"
  printf '{"workspace_root":"%s","runtime_dir":"%s","novarocks":{"sql_test_config":"%s","standalone_config":"%s"}}\n' \
    "$workspace_root" "$runtime_dir" "$sql_config" "$standalone_config" >"$path"
}
write_manifest "$RUNTIME_MANIFEST" "$REPO_ROOT" "$RUNTIME_DIR" "$SQL_CONFIG" "$STANDALONE_CONFIG"

printf '%s\n' \
  '#!/usr/bin/env bash' \
  'set -euo pipefail' \
  'label="$1"' \
  'output="$2"' \
  'if [ "${NFE4_FAKE_DUPLICATE_ARTIFACTS:-0}" = "1" ]; then label=duplicate; fi' \
  'printf "fake-%s-binary\n" "$label" >"$output"' \
  'chmod +x "$output"' >"$BUILD_HOOK"
chmod +x "$BUILD_HOOK"

printf '%s\n' \
  '#!/usr/bin/env bash' \
  'set -euo pipefail' \
  'label="$1"' \
  'binary="$2"' \
  'suite="$3"' \
  'case_id="$4"' \
  'printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\n" "$label" "$binary" "$suite" "$case_id" "${NOVAROCKS_BIN:-}" "${NFE4_CLUSTER_ARGS:-}" "${NOVAROCKS_STANDALONE_CONFIG:-}" >>"$NFE4_FAKE_CALLS"' \
  'if [ "${NFE4_FAKE_FAIL_CASE:-}" = "$label/$suite/$case_id" ]; then exit 42; fi' \
  'if [ "${NFE4_FAKE_ZERO_CASE:-}" = "$label/$suite/$case_id" ]; then' \
  '  printf "cases=0\ntotal=0\npass=0\nfail=0\n"' \
  '  exit 0' \
  'fi' \
  'barrier_target="$label/$suite/$case_id"' \
  'if [ "${NFE4_FAKE_MUTATE_CASE:-}" = "$barrier_target" ] && [ -n "${NFE4_FAKE_MUTATE_PATH:-}" ]; then' \
  '  printf "\n " >>"$NFE4_FAKE_MUTATE_PATH"' \
  'fi' \
  'if [ "${NFE4_FAKE_BARRIER_CASE:-}" = "$barrier_target" ]; then' \
  '  case "${NFE4_FAKE_BARRIER_MODE:-missing}" in' \
  '    missing) ;;' \
  '    wrong) printf "cross-process topology barrier PASS: SHOW BACKENDS 2/3 Live; fake=true\n" ;;' \
  '    duplicate) printf "cross-process topology barrier PASS: SHOW BACKENDS 3/3 Live; fake=true\ncross-process topology barrier PASS: SHOW BACKENDS 3/3 Live; fake=duplicate\n" ;;' \
  '  esac' \
  'else' \
  '  printf "cross-process topology barrier PASS: SHOW BACKENDS 3/3 Live; fake=true\n"' \
  'fi' \
  'printf "cases=1\ntotal=1\npass=1\nfail=0\n"' >"$RUNNER_HOOK"
chmod +x "$RUNNER_HOOK"

printf '%s\n' \
  '#!/usr/bin/env bash' \
  'set -euo pipefail' \
  'contract="$1"' \
  'printf "%s\n" "$contract" >>"$NFE4_FAKE_CONTRACT_CALLS"' \
  'if [ "${NFE4_FAKE_FAIL_CONTRACT:-}" = "$contract" ]; then exit 43; fi' \
  'printf "%s current-head contract: PASS\n" "$contract"' >"$CONTRACT_HOOK"
chmod +x "$CONTRACT_HOOK"

printf '%s\n' \
  '#!/usr/bin/env bash' \
  'set -euo pipefail' \
  'checkpoint="$1"' \
  'expected_head="$2"' \
  'printf "%s\t%s\n" "$checkpoint" "$expected_head" >>"$NFE4_FAKE_IDENTITY_CALLS"' \
  'if [ "${NFE4_FAKE_FAIL_IDENTITY:-}" = "$checkpoint" ]; then exit 44; fi' \
  'printf "%s\n" "$expected_head"' >"$IDENTITY_HOOK"
chmod +x "$IDENTITY_HOOK"

run_gate() {
  local run_dir="$1"
  shift
  env \
    NFE4_TEST_MODE=1 \
    NFE4_RUN_DIR="$run_dir" \
    NFE4_BUILD_HOOK="$BUILD_HOOK" \
    NFE4_RUNNER_HOOK="$RUNNER_HOOK" \
    NFE4_CONTRACT_HOOK="$CONTRACT_HOOK" \
    NFE4_IDENTITY_HOOK="$IDENTITY_HOOK" \
    NFE4_FAKE_CALLS="$CALLS" \
    NFE4_FAKE_CONTRACT_CALLS="$CONTRACT_CALLS" \
    NFE4_FAKE_IDENTITY_CALLS="$IDENTITY_CALLS" \
    NOVAROCKS_SQL_TEST_CONFIG="$SQL_CONFIG" \
    NOVAROCKS_STANDALONE_CONFIG="$STANDALONE_CONFIG" \
    NOVAROCKS_RUNTIME_MANIFEST="$RUNTIME_MANIFEST" \
    "$@" \
    "$GATE"
}

rm -f "$CALLS" "$CONTRACT_CALLS" "$IDENTITY_CALLS"
SUCCESS_DIR="$TMP_DIR/success"
run_gate "$SUCCESS_DIR"

SUMMARY="$SUCCESS_DIR/summary.txt"
test -f "$SUMMARY"
grep -qx 'summary_format=novarocks-native-dual-kv-v1' "$SUMMARY"
grep -qx 'status=CONTRACT_TEST_PASS' "$SUMMARY"
grep -qx 'acceptance_valid=false' "$SUMMARY"
if grep -qx 'status=PASS' "$SUMMARY"; then
  echo "test mode must never produce an acceptance PASS status" >&2
  exit 1
fi
grep -qx "git_head_start=$EXPECTED_HEAD" "$SUMMARY"
grep -qx "git_head_end=$EXPECTED_HEAD" "$SUMMARY"
grep -qx 'topology=1FE+3BE' "$SUMMARY"
grep -qx 'cluster_mode=cross-process' "$SUMMARY"
grep -qx 'cluster_size=3' "$SUMMARY"
grep -qx 'test_mode=1' "$SUMMARY"
grep -qx 'default_build=PASS' "$SUMMARY"
grep -qx 'compat_build=PASS' "$SUMMARY"
grep -qx 'default_cases=7/7_PASS' "$SUMMARY"
grep -qx 'compat_cases=7/7_PASS' "$SUMMARY"
grep -qx 'default_barriers=7/7_PASS' "$SUMMARY"
grep -qx 'compat_barriers=7/7_PASS' "$SUMMARY"
grep -qx "runtime_manifest=$RUNTIME_MANIFEST" "$SUMMARY"
grep -q '^runtime_manifest_sha256=[0-9a-f]\{64\}$' "$SUMMARY"
grep -q '^sql_test_config_sha256=[0-9a-f]\{64\}$' "$SUMMARY"
grep -q '^standalone_config_sha256=[0-9a-f]\{64\}$' "$SUMMARY"
grep -qx "standalone_config=$STANDALONE_CONFIG" "$SUMMARY"
grep -qx "case_results_tsv=$SUCCESS_DIR/case-results.tsv" "$SUMMARY"
grep -q '^default_binary=/' "$SUMMARY"
grep -q '^compat_binary=/' "$SUMMARY"
grep -q '^default_sha256=[0-9a-f]\{64\}$' "$SUMMARY"
grep -q '^compat_sha256=[0-9a-f]\{64\}$' "$SUMMARY"
grep -qx 'native_proto_contract=PASS' "$SUMMARY"
grep -qx 'raw_source_guard=PASS' "$SUMMARY"
grep -qx "logs=$SUCCESS_DIR/logs" "$SUMMARY"

DEFAULT_BINARY="$(sed -n 's/^default_binary=//p' "$SUMMARY")"
COMPAT_BINARY="$(sed -n 's/^compat_binary=//p' "$SUMMARY")"
test "$DEFAULT_BINARY" != "$COMPAT_BINARY"
test -x "$DEFAULT_BINARY"
test -x "$COMPAT_BINARY"
test "$(sed -n 's/^default_sha256=//p' "$SUMMARY")" != "$(sed -n 's/^compat_sha256=//p' "$SUMMARY")"
printf '%s\n' native_proto_contract raw_source_guard >"$TMP_DIR/expected-contracts.txt"
diff -u "$TMP_DIR/expected-contracts.txt" "$CONTRACT_CALLS"

EXPECTED_IDENTITY_CALLS="$TMP_DIR/expected-identity-calls.txt"
for checkpoint in \
  before_static_contracts \
  before_build_default after_build_default \
  before_build_compat after_build_compat \
  before_sql_matrix after_sql_matrix before_final_pass; do
  printf '%s\t%s\n' "$checkpoint" "$EXPECTED_HEAD"
done >"$EXPECTED_IDENTITY_CALLS"
diff -u "$EXPECTED_IDENTITY_CALLS" "$IDENTITY_CALLS"

EXPECTED_CASES="$TMP_DIR/expected-cases.tsv"
printf '%s\n' \
  $'filter\tfilter_basic_comparison' \
  $'runtime-filter-distributed\truntime_filter_distributed_partitioned_probe' \
  $'aggregate\tdistinct_group_by_multi_phase' \
  $'cte\tcte_multi_alias' \
  $'iceberg-rest\ticeberg_rest_distributed_insert_append' \
  $'iceberg-rest\ticeberg_rest_distributed_delete' \
  $'iceberg-rest\ticeberg_rest_ivm_change_op_delta_source' >"$EXPECTED_CASES"

test "$(wc -l <"$CALLS" | tr -d ' ')" -eq 14
for label in default compat; do
  awk -F '\t' -v label="$label" '$1 == label { print $3 "\t" $4 }' "$CALLS" >"$TMP_DIR/$label.cases"
  diff -u "$EXPECTED_CASES" "$TMP_DIR/$label.cases"
done

CASE_RESULTS="$SUCCESS_DIR/case-results.tsv"
test -f "$CASE_RESULTS"
grep -qx $'label\tsuite\tcase_id\texit_code\tcase_pass\tbarrier_exit_code\tbarrier_marker_count\tbarrier_pass\tlog' "$CASE_RESULTS"
test "$(wc -l <"$CASE_RESULTS" | tr -d ' ')" -eq 15

while IFS=$'\t' read -r label binary suite case_id env_binary cluster_args env_standalone; do
  test "$binary" = "$env_binary"
  test "$cluster_args" = '--cluster-mode cross-process --cluster-size 3'
  test "$env_standalone" = "$STANDALONE_CONFIG"
  awk -F '\t' -v l="$label" -v s="$suite" -v c="$case_id" \
    '$1 == l && $2 == s && $3 == c && $4 == 0 && $5 == 1 && $6 == 0 && $7 == 1 && $8 == 1 { found=1 } END { exit(found ? 0 : 1) }' \
    "$CASE_RESULTS"
done <"$CALLS"

if rg -n 'DISCOVERY_FAIL|KNOWN_FAIL|known.fail' "$GATE" "$SUMMARY" >/dev/null; then
  echo "dedicated gate must not contain discovery or known-failure downgrade paths" >&2
  exit 1
fi
if ! grep -q 'status --porcelain --untracked-files=normal' "$GATE"; then
  echo "real current-head gate must reject a dirty worktree" >&2
  exit 1
fi

FAIL_CASE='compat/cte/cte_multi_alias'
if run_gate "$TMP_DIR/failing-runner" NFE4_FAKE_FAIL_CASE="$FAIL_CASE"; then
  echo "runner failure must fail the hard gate even through tee" >&2
  exit 1
fi

ZERO_CASE='default/aggregate/distinct_group_by_multi_phase'
if run_gate "$TMP_DIR/zero-case" NFE4_FAKE_ZERO_CASE="$ZERO_CASE"; then
  echo "a successful runner invocation selecting zero cases must fail the gate" >&2
  exit 1
fi

BARRIER_CASE='default/filter/filter_basic_comparison'
for mode in missing wrong duplicate; do
  if run_gate "$TMP_DIR/barrier-$mode" \
    NFE4_FAKE_BARRIER_CASE="$BARRIER_CASE" NFE4_FAKE_BARRIER_MODE="$mode"; then
    echo "$mode 3/3 Live barrier evidence must fail the hard gate" >&2
    exit 1
  fi
done

for drift in manifest sql standalone; do
  drift_dir="$TMP_DIR/drift-$drift"
  mkdir -p "$drift_dir"
  drift_sql="$drift_dir/sql-test.conf"
  drift_standalone="$drift_dir/standalone.toml"
  drift_manifest="$drift_dir/manifest.json"
  printf 'fixture=true\n' >"$drift_sql"
  printf 'fixture=true\n' >"$drift_standalone"
  write_manifest "$drift_manifest" "$REPO_ROOT" "$drift_dir" "$drift_sql" "$drift_standalone"
  case "$drift" in
    manifest) mutate_path="$drift_manifest" ;;
    sql) mutate_path="$drift_sql" ;;
    standalone) mutate_path="$drift_standalone" ;;
  esac
  if run_gate "$TMP_DIR/failing-drift-$drift" \
    NOVAROCKS_RUNTIME_MANIFEST="$drift_manifest" \
    NOVAROCKS_SQL_TEST_CONFIG="$drift_sql" \
    NOVAROCKS_STANDALONE_CONFIG="$drift_standalone" \
    NFE4_FAKE_MUTATE_CASE=default/filter/filter_basic_comparison \
    NFE4_FAKE_MUTATE_PATH="$mutate_path"; then
    echo "$drift drift during the SQL matrix must fail final evidence validation" >&2
    exit 1
  fi
done

if run_gate "$TMP_DIR/duplicate-artifacts" NFE4_FAKE_DUPLICATE_ARTIFACTS=1; then
  echo "duplicate default/compat artifacts must fail the gate" >&2
  exit 1
fi

for contract in native_proto_contract raw_source_guard; do
  if run_gate "$TMP_DIR/failing-$contract" NFE4_FAKE_FAIL_CONTRACT="$contract"; then
    echo "$contract failure must fail the hard gate" >&2
    exit 1
  fi
done

if run_gate "$TMP_DIR/failing-identity" NFE4_FAKE_FAIL_IDENTITY=after_build_compat; then
  echo "source identity drift at any checkpoint must fail the hard gate" >&2
  exit 1
fi

MISMATCH_DIR="$TMP_DIR/manifest-mismatch"
mkdir -p "$MISMATCH_DIR" "$TMP_DIR/not-the-repo" "$TMP_DIR/not-the-manifest-parent"
printf 'fixture=true\n' >"$TMP_DIR/other-sql.conf"
printf 'fixture=true\n' >"$TMP_DIR/other-standalone.toml"
for mismatch in workspace runtime sql standalone; do
  manifest="$MISMATCH_DIR/$mismatch.json"
  workspace="$REPO_ROOT"
  runtime="$MISMATCH_DIR"
  sql="$SQL_CONFIG"
  standalone="$STANDALONE_CONFIG"
  case "$mismatch" in
    workspace) workspace="$TMP_DIR/not-the-repo" ;;
    runtime) runtime="$TMP_DIR/not-the-manifest-parent" ;;
    sql) sql="$TMP_DIR/other-sql.conf" ;;
    standalone) standalone="$TMP_DIR/other-standalone.toml" ;;
  esac
  write_manifest "$manifest" "$workspace" "$runtime" "$sql" "$standalone"
  if run_gate "$TMP_DIR/failing-manifest-$mismatch" NOVAROCKS_RUNTIME_MANIFEST="$manifest"; then
    echo "runtime manifest $mismatch mismatch must fail the hard gate" >&2
    exit 1
  fi
done

if env \
  NFE4_RUN_DIR="$TMP_DIR/hooks-without-test-mode" \
  NFE4_BUILD_HOOK="$BUILD_HOOK" \
  NFE4_RUNNER_HOOK="$RUNNER_HOOK" \
  NFE4_CONTRACT_HOOK="$CONTRACT_HOOK" \
  NFE4_IDENTITY_HOOK="$IDENTITY_HOOK" \
  NFE4_FAKE_CALLS="$CALLS" \
  NFE4_FAKE_CONTRACT_CALLS="$CONTRACT_CALLS" \
  NFE4_FAKE_IDENTITY_CALLS="$IDENTITY_CALLS" \
  NOVAROCKS_SQL_TEST_CONFIG="$SQL_CONFIG" \
  NOVAROCKS_STANDALONE_CONFIG="$STANDALONE_CONFIG" \
  NOVAROCKS_RUNTIME_MANIFEST="$RUNTIME_MANIFEST" \
  "$GATE"; then
  echo "test hooks must be rejected unless NFE4_TEST_MODE=1" >&2
  exit 1
fi

echo "native dual cross-process gate contract: PASS"
