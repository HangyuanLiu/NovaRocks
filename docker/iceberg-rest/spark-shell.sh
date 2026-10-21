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

# Run a Scala script through the fixture's Spark shell with the same catalog
# configuration as spark-sql.sh.  This is intentionally a test-fixture helper:
# Iceberg's Table API exposes metadata (including StatisticsFile/Puffin) that
# is not available as a stable Spark SQL metadata table.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "${NOVAROCKS_WORKSPACE_ROOT:-$SCRIPT_DIR/../..}" && pwd)"
# See spark-sql.sh: CI supplies its already-resolved generated environment so
# this helper does not depend on the mutable interactive `current` link.
input_file=""
while (($#)); do
  case "$1" in
    --fixture-budget-secs) [[ $# -ge 2 ]] || { echo 'missing fixture budget' >&2; exit 2; }; export NOVA_FIXTURE_BUDGET_SECS="$2"; shift 2 ;;
    --fixture-budget-secs=*) export NOVA_FIXTURE_BUDGET_SECS="${1#*=}"; shift ;;
    --*) echo "unknown option: $1" >&2; exit 2 ;;
    *) [[ -z "$input_file" ]] || { echo 'only one input file is accepted' >&2; exit 2; }; input_file="$1"; shift ;;
  esac
done
set -- "$input_file"
CURRENT_ENV="${NOVA_ENV_REST_ENV_FILE:-$WORKSPACE_ROOT/docker/iceberg-rest/runtime/current/env.sh}"
CURRENT_ENV="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "$CURRENT_ENV")"

if [[ ! -f "$CURRENT_ENV" ]]; then
  echo "environment is not initialized: $CURRENT_ENV" >&2
  echo "run docker/iceberg-rest/up.sh first" >&2
  exit 1
fi

# shellcheck disable=SC1090
source "$CURRENT_ENV"
[[ "${NOVA_ENV_READY:-false}" == "true" ]] || { echo "fixture is not ready; run docker/iceberg-rest/up.sh" >&2; exit 1; }
NOVA_FIXTURE_RUN_CONTEXT="$(python3 "$SCRIPT_DIR/fixture_run.py" context --env-file "$CURRENT_ENV")"
export NOVA_FIXTURE_RUN_CONTEXT


scala_file="${1:-}"
if [[ -z "$scala_file" ]]; then
  echo "usage: $0 <scala-file>" >&2
  exit 2
fi
if [[ ! -f "$scala_file" ]]; then
  echo "Scala file not found: $scala_file" >&2
  exit 1
fi
if [[ ! -f "$NOVAROCKS_SPARK_DEFAULTS" ]]; then
  echo "Spark defaults file not found: $NOVAROCKS_SPARK_DEFAULTS" >&2
  exit 1
fi

compose_args=(
  python3 "$SCRIPT_DIR/fixture_run.py" compose --env-file "$CURRENT_ENV" --
  --env-file "$NOVA_ENV_COMPOSE_ENV"
  -p "$NOVA_ENV_COMPOSE_PROJECT"
  -f "$NOVA_ENV_COMPOSE_FILE"
)

python3 "$SCRIPT_DIR/fixture_run.py" sweep-start --env-file "$CURRENT_ENV" >&2

tmp_dir="$(python3 "$SCRIPT_DIR/fixture_run.py" temp-dir --env-file "$CURRENT_ENV")"
tmp_scala="$tmp_dir/query.scala"
tmp_defaults="$tmp_dir/spark-defaults.conf"
cleanup_local_run() {
  local primary=$? cleanup=0
  trap - EXIT INT TERM
  python3 "$SCRIPT_DIR/fixture_run.py" cleanup-dir --env-file "$CURRENT_ENV" --path "$tmp_dir" || cleanup=$?
  (( primary == 0 )) || exit "$primary"
  exit "$cleanup"
}
trap cleanup_local_run EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
cd "$WORKSPACE_ROOT"
{
  cat "$NOVAROCKS_SPARK_DEFAULTS"
  if [[ "$NOVA_FIXTURE_RUN_CONTEXT" != null ]]; then
    python3 - "$NOVA_FIXTURE_RUN_CONTEXT" <<'PYRUN'
import json,sys
value=json.loads(sys.argv[1])
print("\nspark.novarocks.fixture.scopeRoot " + value["identity"]["scope_root"])
print("spark.novarocks.fixture.runId " + value["identity"]["run_id"])
PYRUN
  fi
  if [[ -n "${NOVAROCKS_SPARK_EXTRA_DEFAULTS:-}" ]]; then
    IFS=':' read -r -a extra_defaults_files <<< "$NOVAROCKS_SPARK_EXTRA_DEFAULTS"
    for defaults_file in "${extra_defaults_files[@]}"; do
      if [[ ! -f "$defaults_file" ]]; then
        echo "Spark extra defaults file not found: $defaults_file" >&2
        exit 1
      fi
      printf '\n'
      cat "$defaults_file"
    done
  fi
} | "${compose_args[@]}" exec -T spark /bin/bash -lc "cat > '$tmp_defaults'"
"${compose_args[@]}" exec -T spark /bin/bash -lc "cat > '$tmp_scala'" < "$scala_file"
"${compose_args[@]}" exec -T spark /bin/bash -lc "
  set -euo pipefail
  spark_shell_bin=\"\${SPARK_SHELL_BIN:-}\"
  if [[ -z \"\$spark_shell_bin\" ]]; then
    spark_shell_bin=\"\$(command -v spark-shell || true)\"
  fi
  if [[ -z \"\$spark_shell_bin\" && -x /opt/spark/bin/spark-shell ]]; then
    spark_shell_bin=/opt/spark/bin/spark-shell
  fi
  if [[ -z \"\$spark_shell_bin\" ]]; then
    echo 'spark-shell binary not found' >&2
    exit 127
  fi
  printf ':quit\\n' | \"\$spark_shell_bin\" --properties-file '$tmp_defaults' -i '$tmp_scala'
"
