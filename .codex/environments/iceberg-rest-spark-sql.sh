#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "${NOVAROCKS_WORKSPACE_ROOT:-$SCRIPT_DIR/../..}" && pwd)"
CURRENT_ENV="$SCRIPT_DIR/runtime/current/env.sh"

if [[ ! -f "$CURRENT_ENV" ]]; then
  echo "environment is not initialized: $CURRENT_ENV" >&2
  echo "run .codex/environments/iceberg-rest-up.sh first" >&2
  exit 1
fi

# shellcheck disable=SC1090
source "$CURRENT_ENV"

sql_file="${1:-${NOVAROCKS_SPARK_V3_SMOKE_SQL:-}}"
if [[ -z "$sql_file" ]]; then
  echo "usage: $0 [sql-file]" >&2
  exit 2
fi

if [[ ! -f "$sql_file" ]]; then
  echo "SQL file not found: $sql_file" >&2
  exit 1
fi

compose_args=(
  docker compose
  --env-file "$NOVA_ENV_COMPOSE_ENV"
  -p "$NOVA_ENV_COMPOSE_PROJECT"
  -f "$NOVA_ENV_COMPOSE_FILE"
)

tmp_sql="/tmp/novarocks-spark-sql-$$.sql"

cd "$WORKSPACE_ROOT"
"${compose_args[@]}" exec -T spark /bin/bash -lc "
  set -euo pipefail
  trap 'rm -f $tmp_sql' EXIT
  cat > '$tmp_sql'
  spark-sql --properties-file /opt/novarocks-env/spark-defaults.conf -f '$tmp_sql'
" < "$sql_file"
