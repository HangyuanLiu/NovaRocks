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
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd -P)"
PROFILE="${NFE4_CARGO_PROFILE:-dev-opt}"
TIMESTAMP="$(date +%Y%m%d-%H%M%S)"
RUN_DIR="${NFE4_RUN_DIR:-$REPO_ROOT/logs/nfe4-native-e2e/$TIMESTAMP}"
BUILD_HOOK="${NFE4_BUILD_HOOK:-}"
RUNNER_HOOK="${NFE4_RUNNER_HOOK:-}"
CONTRACT_HOOK="${NFE4_CONTRACT_HOOK:-}"
IDENTITY_HOOK="${NFE4_IDENTITY_HOOK:-}"
TEST_MODE="${NFE4_TEST_MODE:-0}"
SQL_CONFIG="${NOVAROCKS_SQL_TEST_CONFIG:-}"
EXPECTED_STANDALONE_CONFIG="${NOVAROCKS_STANDALONE_CONFIG:-}"
RUNTIME_MANIFEST="${NOVAROCKS_RUNTIME_MANIFEST:-${NOVA_ENV_RUNTIME_DIR:-}/manifest.json}"

mkdir -p "$RUN_DIR/bin" "$RUN_DIR/logs"
RUN_DIR="$(cd "$RUN_DIR" && pwd -P)"
SUMMARY="$RUN_DIR/summary.txt"
CASE_RESULTS="$RUN_DIR/case-results.tsv"
GIT_HEAD_START="$(git -C "$REPO_ROOT" rev-parse HEAD)"
GIT_HEAD_END="NOT_VERIFIED"
DEFAULT_BINARY="$RUN_DIR/bin/novarocks-default"
COMPAT_BINARY="$RUN_DIR/bin/novarocks-compat"
DEFAULT_BUILD="NOT_RUN"
COMPAT_BUILD="NOT_RUN"
DEFAULT_CASE_COUNT=0
COMPAT_CASE_COUNT=0
DEFAULT_BARRIER_COUNT=0
COMPAT_BARRIER_COUNT=0
STATUS="FAIL"
ACCEPTANCE_VALID="false"
NATIVE_PROTO_CONTRACT="NOT_RUN"
RAW_SOURCE_GUARD="NOT_RUN"
RUNTIME_MANIFEST_SHA256="NOT_VALIDATED"
SQL_CONFIG_SHA256="NOT_VALIDATED"
STANDALONE_CONFIG_SHA256="NOT_VALIDATED"
RUNTIME_DIR="NOT_VALIDATED"
STANDALONE_CONFIG="NOT_VALIDATED"

printf 'label\tsuite\tcase_id\texit_code\tcase_pass\tbarrier_exit_code\tbarrier_marker_count\tbarrier_pass\tlog\n' >"$CASE_RESULTS"

absolute_existing_path() {
  local path="$1"
  local dir
  dir="$(cd "$(dirname "$path")" && pwd -P)"
  printf '%s/%s\n' "$dir" "$(basename "$path")"
}

sha256_file() {
  local path="$1"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$path" | awk '{print $1}'
  else
    shasum -a 256 "$path" | awk '{print $1}'
  fi
}

rollup() {
  local count="$1"
  if [ "$count" -eq 7 ]; then
    printf '7/7_PASS\n'
  else
    printf '%s/7_FAIL\n' "$count"
  fi
}

render_summary() {
  local default_sha="NOT_BUILT"
  local compat_sha="NOT_BUILT"
  local default_path="$DEFAULT_BINARY"
  local compat_path="$COMPAT_BINARY"

  if [ -f "$DEFAULT_BINARY" ]; then
    default_path="$(absolute_existing_path "$DEFAULT_BINARY")"
    default_sha="$(sha256_file "$DEFAULT_BINARY")"
  fi
  if [ -f "$COMPAT_BINARY" ]; then
    compat_path="$(absolute_existing_path "$COMPAT_BINARY")"
    compat_sha="$(sha256_file "$COMPAT_BINARY")"
  fi

  {
    echo "summary_format=novarocks-native-dual-kv-v1"
    echo "status=$STATUS"
    echo "acceptance_valid=$ACCEPTANCE_VALID"
    echo "test_mode=$TEST_MODE"
    echo "git_head_start=$GIT_HEAD_START"
    echo "git_head_end=$GIT_HEAD_END"
    echo "topology=1FE+3BE"
    echo "cluster_mode=cross-process"
    echo "cluster_size=3"
    echo "default_binary=$default_path"
    echo "default_sha256=$default_sha"
    echo "compat_binary=$compat_path"
    echo "compat_sha256=$compat_sha"
    echo "default_build=$DEFAULT_BUILD"
    echo "compat_build=$COMPAT_BUILD"
    echo "default_cases=$(rollup "$DEFAULT_CASE_COUNT")"
    echo "compat_cases=$(rollup "$COMPAT_CASE_COUNT")"
    echo "default_barriers=$(rollup "$DEFAULT_BARRIER_COUNT")"
    echo "compat_barriers=$(rollup "$COMPAT_BARRIER_COUNT")"
    echo "native_proto_contract=$NATIVE_PROTO_CONTRACT"
    echo "raw_source_guard=$RAW_SOURCE_GUARD"
    echo "runtime_manifest=$RUNTIME_MANIFEST"
    echo "runtime_manifest_sha256=$RUNTIME_MANIFEST_SHA256"
    echo "runtime_dir=$RUNTIME_DIR"
    echo "sql_test_config=$SQL_CONFIG"
    echo "sql_test_config_sha256=$SQL_CONFIG_SHA256"
    echo "standalone_config=$STANDALONE_CONFIG"
    echo "standalone_config_sha256=$STANDALONE_CONFIG_SHA256"
    echo "case_results_tsv=$CASE_RESULTS"
    echo "logs=$RUN_DIR/logs"
  } >"$SUMMARY"
}

finish() {
  local code=$?
  trap - EXIT
  render_summary
  echo "native dual cross-process summary: $SUMMARY"
  exit "$code"
}
trap finish EXIT

case "$TEST_MODE" in
  0|1) ;;
  *)
    echo "error: NFE4_TEST_MODE must be 0 or 1" >&2
    exit 2
    ;;
esac

if [ "$TEST_MODE" = "0" ]; then
  if [ -n "$BUILD_HOOK" ] || [ -n "$RUNNER_HOOK" ] || [ -n "$CONTRACT_HOOK" ] || [ -n "$IDENTITY_HOOK" ]; then
    echo "error: NFE4 hooks are test-only and require NFE4_TEST_MODE=1" >&2
    exit 2
  fi
elif [ -z "$BUILD_HOOK" ] || [ -z "$RUNNER_HOOK" ] || [ -z "$CONTRACT_HOOK" ] || [ -z "$IDENTITY_HOOK" ]; then
  echo "error: NFE4_TEST_MODE=1 requires build, runner, contract, and identity hooks" >&2
  exit 2
fi

verify_source_identity() {
  local checkpoint="$1"
  local current_head

  if [ "$TEST_MODE" = "1" ]; then
    current_head="$("$IDENTITY_HOOK" "$checkpoint" "$GIT_HEAD_START")"
    current_head="$(printf '%s\n' "$current_head" | tail -1)"
  else
    current_head="$(git -C "$REPO_ROOT" rev-parse HEAD)"
    if [ -n "$(git -C "$REPO_ROOT" status --porcelain --untracked-files=normal)" ]; then
      echo "error: source identity checkpoint $checkpoint found a dirty worktree" >&2
      return 1
    fi
  fi

  if [ "$current_head" != "$GIT_HEAD_START" ]; then
    echo "error: source identity changed at $checkpoint: start=$GIT_HEAD_START current=$current_head" >&2
    return 1
  fi
  GIT_HEAD_END="$current_head"
}

validate_runtime_manifest() {
  local output="$RUN_DIR/manifest-validation.txt"
  python3 - "$RUNTIME_MANIFEST" "$REPO_ROOT" "$SQL_CONFIG" "$EXPECTED_STANDALONE_CONFIG" >"$output" <<'PY'
import json
import sys
from pathlib import Path

manifest = Path(sys.argv[1]).resolve(strict=True)
repo = Path(sys.argv[2]).resolve(strict=True)
sql_config = Path(sys.argv[3]).resolve(strict=True)
expected_standalone_raw = sys.argv[4]

with manifest.open("r", encoding="utf-8") as handle:
    data = json.load(handle)

workspace = Path(data["workspace_root"]).resolve(strict=True)
runtime_dir = Path(data["runtime_dir"]).resolve(strict=True)
manifest_sql = Path(data["novarocks"]["sql_test_config"]).resolve(strict=True)
standalone = Path(data["novarocks"]["standalone_config"]).resolve(strict=True)

if workspace != repo:
    raise SystemExit(f"manifest workspace_root mismatch: manifest={workspace} repo={repo}")
if runtime_dir != manifest.parent:
    raise SystemExit(
        f"manifest runtime_dir mismatch: manifest={runtime_dir} parent={manifest.parent}"
    )
if manifest_sql != sql_config:
    raise SystemExit(
        f"manifest sql_test_config mismatch: manifest={manifest_sql} requested={sql_config}"
    )
if not standalone.is_file():
    raise SystemExit(f"manifest standalone_config is not a file: {standalone}")
if expected_standalone_raw:
    expected_standalone = Path(expected_standalone_raw).resolve(strict=True)
    if standalone != expected_standalone:
        raise SystemExit(
            f"manifest standalone_config mismatch: manifest={standalone} requested={expected_standalone}"
        )

print(manifest)
print(runtime_dir)
print(sql_config)
print(standalone)
PY

  test "$(wc -l <"$output" | tr -d ' ')" -eq 4
  RUNTIME_MANIFEST="$(sed -n '1p' "$output")"
  RUNTIME_DIR="$(sed -n '2p' "$output")"
  SQL_CONFIG="$(sed -n '3p' "$output")"
  STANDALONE_CONFIG="$(sed -n '4p' "$output")"
  RUNTIME_MANIFEST_SHA256="$(sha256_file "$RUNTIME_MANIFEST")"
  SQL_CONFIG_SHA256="$(sha256_file "$SQL_CONFIG")"
  STANDALONE_CONFIG_SHA256="$(sha256_file "$STANDALONE_CONFIG")"
}

revalidate_runtime_evidence() {
  local expected_manifest="$RUNTIME_MANIFEST"
  local expected_runtime_dir="$RUNTIME_DIR"
  local expected_sql_config="$SQL_CONFIG"
  local expected_standalone_config="$STANDALONE_CONFIG"
  local expected_manifest_sha="$RUNTIME_MANIFEST_SHA256"
  local expected_sql_sha="$SQL_CONFIG_SHA256"
  local expected_standalone_sha="$STANDALONE_CONFIG_SHA256"

  validate_runtime_manifest
  if [ "$RUNTIME_MANIFEST" != "$expected_manifest" ] \
    || [ "$RUNTIME_DIR" != "$expected_runtime_dir" ] \
    || [ "$SQL_CONFIG" != "$expected_sql_config" ] \
    || [ "$STANDALONE_CONFIG" != "$expected_standalone_config" ] \
    || [ "$RUNTIME_MANIFEST_SHA256" != "$expected_manifest_sha" ] \
    || [ "$SQL_CONFIG_SHA256" != "$expected_sql_sha" ] \
    || [ "$STANDALONE_CONFIG_SHA256" != "$expected_standalone_sha" ]; then
    echo "error: validated runtime manifest or config evidence changed during the native SQL matrix" >&2
    return 1
  fi
}

if [ -z "$SQL_CONFIG" ] || [ ! -f "$SQL_CONFIG" ]; then
  echo "error: NOVAROCKS_SQL_TEST_CONFIG must name an existing runner config" >&2
  exit 2
fi
SQL_CONFIG="$(absolute_existing_path "$SQL_CONFIG")"

if [ -z "$RUNTIME_MANIFEST" ] || [ ! -f "$RUNTIME_MANIFEST" ]; then
  echo "error: NOVAROCKS_RUNTIME_MANIFEST must name the current runtime manifest" >&2
  exit 2
fi
RUNTIME_MANIFEST="$(absolute_existing_path "$RUNTIME_MANIFEST")"

for hook_name in BUILD RUNNER CONTRACT IDENTITY; do
  eval "hook_path=\${NFE4_${hook_name}_HOOK:-}"
  if [ -n "$hook_path" ] && [ ! -x "$hook_path" ]; then
    echo "error: NFE4_${hook_name}_HOOK is not executable: $hook_path" >&2
    exit 2
  fi
done

verify_source_identity before_static_contracts
validate_runtime_manifest

run_contract() {
  local contract="$1"
  local log="$RUN_DIR/logs/$contract.log"

  if [ "$TEST_MODE" = "1" ]; then
    "$CONTRACT_HOOK" "$contract" 2>&1 | tee "$log"
    return
  fi

  case "$contract" in
    native_proto_contract)
      (
        cd "$REPO_ROOT"
        cargo test --lib coordinator::dispatch::tests::fragment_submission_requires_native_plan_and_instance_params -- --nocapture
        cargo test --lib coordinator::dispatch::tests::fragment_submission_requires_native_plan_and_instance_params --features compat -- --nocapture
      ) 2>&1 | tee "$log"
      ;;
    raw_source_guard)
      (
        cd "$REPO_ROOT"
        cargo test --test architecture_guard nfe_4 -- --nocapture
        python3 tools/dev/audit_thrift_boundaries.py --strict --summary
      ) 2>&1 | tee "$log"
      ;;
    *)
      echo "error: unknown current-head contract: $contract" >&2
      return 2
      ;;
  esac
  if grep -Eq 'test result: ok\. 0 passed' "$log"; then
    echo "error: $contract filter executed zero tests" >&2
    return 1
  fi
}

run_contract native_proto_contract
NATIVE_PROTO_CONTRACT="PASS"
run_contract raw_source_guard
RAW_SOURCE_GUARD="PASS"

build_binary() {
  local label="$1"
  local output="$2"
  local log="$RUN_DIR/logs/build-$label.log"

  if [ "$TEST_MODE" = "1" ]; then
    "$BUILD_HOOK" "$label" "$output" 2>&1 | tee "$log"
  elif [ "$label" = "default" ]; then
    (
      cd "$REPO_ROOT"
      cargo build --profile "$PROFILE"
      cp "target/$PROFILE/novarocks" "$output"
    ) 2>&1 | tee "$log"
  else
    (
      cd "$REPO_ROOT"
      cargo build --profile "$PROFILE" --features compat
      cp "target/$PROFILE/novarocks" "$output"
    ) 2>&1 | tee "$log"
  fi

  if [ ! -x "$output" ]; then
    echo "error: $label build did not preserve an executable artifact at $output" >&2
    return 1
  fi
}

verify_source_identity before_build_default
build_binary default "$DEFAULT_BINARY"
DEFAULT_BUILD="PASS"
verify_source_identity after_build_default

verify_source_identity before_build_compat
build_binary compat "$COMPAT_BINARY"
COMPAT_BUILD="PASS"
verify_source_identity after_build_compat

DEFAULT_BINARY="$(absolute_existing_path "$DEFAULT_BINARY")"
COMPAT_BINARY="$(absolute_existing_path "$COMPAT_BINARY")"
if [ "$DEFAULT_BINARY" = "$COMPAT_BINARY" ]; then
  echo "error: default and compat artifact paths are identical" >&2
  exit 1
fi
if [ "$(sha256_file "$DEFAULT_BINARY")" = "$(sha256_file "$COMPAT_BINARY")" ]; then
  echo "error: default and compat artifacts have identical SHA-256 identities" >&2
  exit 1
fi

CASES=(
  "filter/filter_basic_comparison"
  "runtime-filter-distributed/runtime_filter_distributed_partitioned_probe"
  "aggregate/distinct_group_by_multi_phase"
  "cte/cte_multi_alias"
  "iceberg-rest/iceberg_rest_distributed_insert_append"
  "iceberg-rest/iceberg_rest_distributed_delete"
  "iceberg-rest/iceberg_rest_ivm_change_op_delta_source"
)
TOPOLOGY_MARKER="cross-process topology barrier PASS: SHOW BACKENDS 3/3 Live"

run_case() {
  local label="$1"
  local binary="$2"
  local suite="$3"
  local case_id="$4"
  local log="$RUN_DIR/logs/$label-$suite-$case_id.log"
  local code
  local case_pass=0
  local barrier_exit_code=1
  local barrier_count
  local barrier_pass=0

  set +e
  if [ "$TEST_MODE" = "1" ]; then
    NO_PROXY=127.0.0.1,localhost \
      NOVAROCKS_BIN="$binary" \
      NOVAROCKS_STANDALONE_CONFIG="$STANDALONE_CONFIG" \
      NFE4_CLUSTER_ARGS="--cluster-mode cross-process --cluster-size 3" \
      "$RUNNER_HOOK" "$label" "$binary" "$suite" "$case_id" 2>&1 | tee "$log"
  else
    (
      cd "$REPO_ROOT"
      NO_PROXY=127.0.0.1,localhost \
        NOVAROCKS_BIN="$binary" \
        NOVAROCKS_STANDALONE_CONFIG="$STANDALONE_CONFIG" \
        cargo run --manifest-path tests/sql-test-runner/Cargo.toml \
          --bin sql-tests --profile "$PROFILE" -- \
          --config "$SQL_CONFIG" \
          --suite "$suite" \
          --only "$case_id" \
          --mode verify \
          --query-timeout 300 \
          --cluster-mode cross-process \
          --cluster-size 3 \
          -j 1
    ) 2>&1 | tee "$log"
  fi
  code=$?
  set -e

  barrier_count="$(awk -v marker="$TOPOLOGY_MARKER" 'index($0, marker) == 1 { count++ } END { print count + 0 }' "$log")"
  if [ "$code" -eq 0 ] \
    && grep -Eq '^cases=1([[:space:](]|$)' "$log" \
    && grep -qx 'total=1' "$log" \
    && grep -qx 'pass=1' "$log" \
    && grep -qx 'fail=0' "$log"; then
    case_pass=1
  fi
  if [ "$barrier_count" -eq 1 ]; then
    barrier_exit_code=0
    barrier_pass=1
  fi

  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$label" "$suite" "$case_id" "$code" "$case_pass" "$barrier_exit_code" "$barrier_count" "$barrier_pass" "$log" \
    >>"$CASE_RESULTS"

  if [ "$code" -ne 0 ]; then
    echo "error: $label $suite/$case_id failed with exit code $code" >&2
    return "$code"
  fi
  if [ "$case_pass" -ne 1 ]; then
    echo "error: $label $suite/$case_id did not execute exactly one passing case" >&2
    return 1
  fi
  if [ "$barrier_pass" -ne 1 ]; then
    echo "error: $label $suite/$case_id requires exactly one 3/3 Live barrier marker; found $barrier_count" >&2
    return 1
  fi
}

verify_source_identity before_sql_matrix
for label in default compat; do
  if [ "$label" = "default" ]; then
    binary="$DEFAULT_BINARY"
  else
    binary="$COMPAT_BINARY"
  fi
  for entry in "${CASES[@]}"; do
    suite="${entry%%/*}"
    case_id="${entry#*/}"
    run_case "$label" "$binary" "$suite" "$case_id"
    if [ "$label" = "default" ]; then
      DEFAULT_CASE_COUNT=$((DEFAULT_CASE_COUNT + 1))
      DEFAULT_BARRIER_COUNT=$((DEFAULT_BARRIER_COUNT + 1))
    else
      COMPAT_CASE_COUNT=$((COMPAT_CASE_COUNT + 1))
      COMPAT_BARRIER_COUNT=$((COMPAT_BARRIER_COUNT + 1))
    fi
  done
done
verify_source_identity after_sql_matrix
revalidate_runtime_evidence
verify_source_identity before_final_pass

if [ "$TEST_MODE" = "1" ]; then
  STATUS="CONTRACT_TEST_PASS"
else
  STATUS="PASS"
  ACCEPTANCE_VALID="true"
fi
