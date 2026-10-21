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
WORKSPACE_ROOT="$(cd "${NOVAROCKS_WORKSPACE_ROOT:-$SCRIPT_DIR/../..}" && pwd)"
# `current` is a convenience link for interactive use. CI passes the exact
# generated entry it prepared so isolated system fixtures cannot redirect or
# remove the environment required by subsequent SQL helpers.
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


sql_file="${1:-${NOVAROCKS_SPARK_V3_SMOKE_SQL:-}}"
if [[ -z "$sql_file" ]]; then
  echo "usage: $0 [sql-file]" >&2
  exit 2
fi

if [[ ! -f "$sql_file" ]]; then
  echo "SQL file not found: $sql_file" >&2
  exit 1
fi

if [[ ! -f "$NOVAROCKS_SPARK_DEFAULTS" ]]; then
  echo "Spark defaults file not found: $NOVAROCKS_SPARK_DEFAULTS" >&2
  exit 1
fi

# Only this pinned publication's generated smoke template can be expanded.
# Caller-provided SQL retains its own semantics and is copied byte for byte.
generated_smoke_sql() {
  python3 - "$SCRIPT_DIR" "$CURRENT_ENV" "$sql_file" "$1" <<'PYSMOKE'
import json, os, sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
import fixture_run
import fixture_runtime as runtime
try:
    env_file, source, operation = Path(sys.argv[2]), Path(sys.argv[3]), sys.argv[4]
    _, manifest, budget = fixture_run.admit(env_file)
    generated = bool(manifest.get("runtime", {}).get("machine")) and source.resolve(strict=True) == Path(manifest["spark"]["v3_smoke_sql"]).resolve(strict=True)
    raw = source.read_bytes()
    if generated:
        text = raw.decode("utf-8")
        publication = env_file.resolve(strict=True).parent
        if Path(manifest["runtime"]["publication_dir"]).resolve(strict=True) != publication or source.resolve(strict=True).parent != publication:
            raise runtime.RuntimeFailure("InvalidScratchScope", "generated smoke differs from its pinned publication")
        if not text.startswith("-- novarocks-generated-shared-spark-v3-smoke-v1\n") or budget is None:
            raise runtime.RuntimeFailure("InvalidScratchScope", "shared generated smoke has no scoped template")
        namespace = budget.identity.resource_name("smoke")
        location = budget.identity.scope_root + "/rest/" + namespace
        replacements = {"__NOVA_SMOKE_NAMESPACE__": namespace,
            "__NOVA_SMOKE_NAMESPACE_LOCATION__": location,
            "__NOVA_SMOKE_TABLE_LOCATION__": location + "/spark_v3_smoke"}
        if any(key not in text for key in replacements):
            raise runtime.RuntimeFailure("InvalidScratchScope", "generated smoke template is incomplete")
        for key, value in replacements.items():
            text = text.replace(key, value.replace("'", "''"))
    if operation == "classify":
        print("true" if generated else "false")
    elif generated:
        sys.stdout.write(text)
    else:
        sys.stdout.buffer.write(raw)
except runtime.RuntimeFailure as error:
    print(str(error), file=sys.stderr)
    raise SystemExit(75 if error.code in {"RunBudgetRejected", "MachineUnavailable"} else 1)
PYSMOKE
}
generated_smoke="$(generated_smoke_sql classify)"

extra_defaults_files=()
if [[ -n "${NOVAROCKS_SPARK_EXTRA_DEFAULTS:-}" ]]; then
  IFS=':' read -r -a extra_defaults_files <<< "$NOVAROCKS_SPARK_EXTRA_DEFAULTS"
  for defaults_file in "${extra_defaults_files[@]}"; do
    if [[ ! -f "$defaults_file" ]]; then
      echo "Spark extra defaults file not found: $defaults_file" >&2
      exit 1
    fi
  done
fi

compose_args=(
  python3 "$SCRIPT_DIR/fixture_run.py" compose --env-file "$CURRENT_ENV" --
  --env-file "$NOVA_ENV_COMPOSE_ENV"
  -p "$NOVA_ENV_COMPOSE_PROJECT"
  -f "$NOVA_ENV_COMPOSE_FILE"
)

python3 "$SCRIPT_DIR/fixture_run.py" sweep-start --env-file "$CURRENT_ENV" >&2

tmp_dir="$(python3 "$SCRIPT_DIR/fixture_run.py" temp-dir --env-file "$CURRENT_ENV")"
tmp_sql="$tmp_dir/query.sql"
tmp_defaults="$tmp_dir/spark-defaults.conf"
cleanup_local_run() {
  local primary=$? cleanup=0
  trap - EXIT INT TERM
  if [[ "$generated_smoke" == true && "$primary" != 0 ]]; then
    # Keep the exact run's query and context marker for diagnosis. The normal
    # SQL footer did not prove both remote drops; TTL remains the reclaim owner.
    python3 - "$NOVA_FIXTURE_RUN_CONTEXT" "$primary" "$tmp_sql" <<'PYFAIL'
import json,sys
identity=json.loads(sys.argv[1])["identity"]
print("SharedSmokeFailed: status="+sys.argv[2]+" namespace="+identity["run_id"]+"_smoke remote_cleanup=NOT_CONFIRMED diagnostics="+sys.argv[3],file=sys.stderr)
PYFAIL
  else
    python3 "$SCRIPT_DIR/fixture_run.py" cleanup-dir --env-file "$CURRENT_ENV" --path "$tmp_dir" || cleanup=$?
    if (( cleanup != 0 )); then
      echo "ContainerCleanupFailed: status=$cleanup diagnostics=$tmp_sql" >&2
    fi
  fi
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
  if [[ ${#extra_defaults_files[@]} -gt 0 ]]; then
    for defaults_file in "${extra_defaults_files[@]}"; do
      printf '\n'
      cat "$defaults_file"
    done
  fi
} | "${compose_args[@]}" exec -T spark /bin/bash -lc "cat > '$tmp_defaults'"
generated_smoke_sql render | "${compose_args[@]}" exec -T spark /bin/bash -lc "cat > '$tmp_sql'"
"${compose_args[@]}" exec -T spark /bin/bash -lc "
  set -euo pipefail
  spark_sql_bin=\"\${SPARK_SQL_BIN:-}\"
  if [[ -z \"\$spark_sql_bin\" ]]; then
    spark_sql_bin=\"\$(command -v spark-sql || true)\"
  fi
  if [[ -z \"\$spark_sql_bin\" && -x /opt/spark/bin/spark-sql ]]; then
    spark_sql_bin=/opt/spark/bin/spark-sql
  fi
  if [[ -z \"\$spark_sql_bin\" ]]; then
    echo 'spark-sql binary not found' >&2
    exit 127
  fi
  \"\$spark_sql_bin\" --properties-file '$tmp_defaults' -f '$tmp_sql'
"
