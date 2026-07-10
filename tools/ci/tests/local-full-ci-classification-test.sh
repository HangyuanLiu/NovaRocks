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

source "$REPO_ROOT/tools/ci/local-full-ci.sh" --source-only

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

baseline="$tmpdir/known-failures.toml"
run_dir="$tmpdir/run"
mkdir -p "$run_dir/sql"

cat >"$baseline" <<'EOF'
[[failure]]
tier = "full"
suite = "tpc-ds"
case = "q93"
error_code = "QueryTimeout"
reason = "synthetic timeout"

[[failure]]
tier = "full"
suite = "tpc-ds"
case = "q94"
error_code = "CommitUnknown"
reason = "synthetic commit unknown"
EOF

cat >"$run_dir/sql/tpc-ds.log" <<'EOF'
[sql-tests] suite=tpc-ds mode=verify
  [tpc-ds] q94 (steps=1)
    engine_error_code=CommitUnknown target execute failed: ERROR 1105 (HY000): [CommitUnknown] commit outcome unavailable
case timings (all):
  [tpc-ds] q93 PASS 0.01s
  [tpc-ds] q94 FAIL 0.01s
FAIL: total=2 pass=1 fail=1
EOF

CI_TIER="full"
KNOWN_FAILURES_FILE="$baseline"
CI_KNOWN_FAILURE_ROWS=""
CI_FAILURE_TAIL=""

if ci_classify_unexpected_passes "tpc-ds" "$run_dir/sql/tpc-ds.log"; then
  echo "expected mixed pass/fail known-failure log to report an unexpected pass" >&2
  exit 1
fi

grep -q "UNEXPECTED_PASS" <<<"$CI_KNOWN_FAILURE_ROWS"

if (
  CI_FROM_RUN_DIR="$run_dir"
  CI_TIER="full"
  KNOWN_FAILURES_FILE="$baseline"
  reclassify_existing_run >/dev/null
); then
  echo "expected --from reclassification to fail on mixed unexpected pass" >&2
  exit 1
fi

grep -q "UNEXPECTED_PASS" "$run_dir/summary.md"

targeted_suites="$(ci_tier_suites targeted "$REPO_ROOT/tools/ci/suites/stable-sql-suites.txt")"
grep -qx "optimizer-dist" <<<"$targeted_suites"

SQL_CLUSTER_MODE="all-in-one"
SQL_CLUSTER_SIZE="1"
if [ "$(ci_suite_cluster_mode optimizer-dist)" != "cross-process" ]; then
  echo "optimizer-dist must force cross-process cluster mode" >&2
  exit 1
fi
if [ "$(ci_suite_cluster_size optimizer-dist)" != "3" ]; then
  echo "optimizer-dist must force a 3-BE cluster" >&2
  exit 1
fi
if [ "$(ci_suite_cluster_mode optimizer)" != "all-in-one" ]; then
  echo "ordinary suites should keep the global cluster mode" >&2
  exit 1
fi
if [ "$(ci_suite_cluster_size optimizer)" != "1" ]; then
  echo "ordinary suites should keep the global cluster size" >&2
  exit 1
fi

native_cross_process_core_suites="$(ci_native_cross_process_core_suites)"
expected_native_cross_process_core_suites="$(printf "%s\n" join filter sort aggregate cte subquery iceberg-rest runtime-filter-distributed)"
if [ "$native_cross_process_core_suites" != "$expected_native_cross_process_core_suites" ]; then
  echo "native cross-process core suites do not match the required matrix" >&2
  printf "expected:\n%s\nactual:\n%s\n" "$expected_native_cross_process_core_suites" "$native_cross_process_core_suites" >&2
  exit 1
fi

if ! ci_native_cross_process_enabled; then
  echo "native cross-process core matrix should be enabled by default" >&2
  exit 1
fi

if [ "$(ci_native_cross_process_suites | tr '\n' ' ')" != "$(printf "%s " join filter sort aggregate cte subquery iceberg-rest runtime-filter-distributed)" ]; then
  echo "default native cross-process matrix should use the core suites" >&2
  exit 1
fi

NOVA_CI_NATIVE_CROSS_PROCESS_CORE="0"
NOVA_CI_NATIVE_CROSS_PROCESS_FULL="0"
if ci_native_cross_process_enabled; then
  echo "explicit NOVA_CI_NATIVE_CROSS_PROCESS_CORE=0 should disable the native cross-process matrix when full coverage is off" >&2
  exit 1
fi

NOVA_CI_NATIVE_CROSS_PROCESS_FULL="1"
if ! ci_native_cross_process_enabled; then
  echo "NOVA_CI_NATIVE_CROSS_PROCESS_FULL=1 should enable the matrix even when core coverage is off" >&2
  exit 1
fi
if ! ci_native_cross_process_suites | grep -qx "optimizer-dist"; then
  echo "native cross-process full matrix should include stable full suites" >&2
  exit 1
fi

SQL_CLUSTER_MODE="all-in-one"
SQL_CLUSTER_SIZE="1"
if [ "$(ci_native_cross_process_suite_cluster_mode join)" != "cross-process" ]; then
  echo "native cross-process suites must force cross-process cluster mode" >&2
  exit 1
fi
if [ "$(ci_native_cross_process_suite_cluster_size join)" != "3" ]; then
  echo "native cross-process suites must force a 3-BE cluster" >&2
  exit 1
fi

local_full_ci_text="$(cat "$REPO_ROOT/tools/ci/local-full-ci.sh")"
if ! grep -q 'stop_server_for_native_cross_process_stage' <<<"$local_full_ci_text"; then
  echo "local-full-ci must use the native cross-process stop-stage name" >&2
  exit 1
fi
if ! grep -q 'run_native_cross_process_sql_suites' <<<"$local_full_ci_text"; then
  echo "local-full-ci must use the native cross-process runner name" >&2
  exit 1
fi
if ! grep -q 'sql-native-cross-process' <<<"$local_full_ci_text"; then
  echo "native cross-process logs must use the sql-native-cross-process directory" >&2
  exit 1
fi
if ! grep -q 'native-cross-process:$suite' <<<"$local_full_ci_text"; then
  echo "native cross-process suite status keys must use native-cross-process:<suite>" >&2
  exit 1
fi
if ! grep -q 'standalone-server stop for native cross-process' <<<"$local_full_ci_text"; then
  echo "native cross-process stop stage must use the native cross-process stage name" >&2
  exit 1
fi
if ! grep -q 'native 1FE+3BE cross-process' <<<"$local_full_ci_text"; then
  echo "local-full-ci help must describe native 1FE+3BE cross-process coverage" >&2
  exit 1
fi
if ! grep -q 'NOVA_CI_NATIVE_CROSS_PROCESS_REQUIRED' <<<"$local_full_ci_text"; then
  echo "native cross-process required failures must be controlled by the renamed env" >&2
  exit 1
fi
if ! grep -q -- '--cluster-mode "$suite_cluster_mode"' <<<"$local_full_ci_text" \
  || ! grep -q -- '--cluster-size "$suite_cluster_size"' <<<"$local_full_ci_text"; then
  echo "native cross-process suites must pass cluster mode and cluster size explicitly" >&2
  exit 1
fi

retired_plan_wire_flag="--plan-wire""-format"
retired_proto_env="NOVA_CI_""PROTO_"
if rg -n -- "$retired_plan_wire_flag|$retired_proto_env" "$REPO_ROOT/tools/ci" >/dev/null; then
  echo "tools/ci must not contain the retired plan-wire flag or retired Proto matrix variables" >&2
  exit 1
fi
if ! grep -q 'run_fail_fast_stage "cargo clippy compat"' <<<"$local_full_ci_text"; then
  echo "local-full-ci must run a compat clippy stage" >&2
  exit 1
fi
if ! grep -q -- 'cargo clippy --all-targets --features compat' <<<"$local_full_ci_text"; then
  echo "compat clippy stage must pass --features compat" >&2
  exit 1
fi
if ! grep -q 'run_fail_fast_stage "cargo build compat"' <<<"$local_full_ci_text"; then
  echo "local-full-ci must run a compat build stage" >&2
  exit 1
fi
if ! grep -q -- 'cargo build --profile "$NOVA_CI_CARGO_PROFILE" --features compat' <<<"$local_full_ci_text"; then
  echo "compat build stage must pass --features compat" >&2
  exit 1
fi
if ! grep -q 'run_fail_fast_stage "cargo test compat"' <<<"$local_full_ci_text"; then
  echo "local-full-ci must run a compat test stage" >&2
  exit 1
fi
if ! grep -q -- 'cargo test --profile "$NOVA_CI_CARGO_PROFILE" --features compat' <<<"$local_full_ci_text"; then
  echo "compat test stage must pass --features compat" >&2
  exit 1
fi

server_lib_text="$(cat "$REPO_ROOT/tools/ci/lib/server.sh")"
if ! grep -q -- 'NOVAROCKS_ENABLE_TEST_IMV_STATELESS_REBUILD=1' <<<"$server_lib_text"; then
  echo "local full CI standalone-server must enable test-only IMV stateless rebuild procedure" >&2
  exit 1
fi
