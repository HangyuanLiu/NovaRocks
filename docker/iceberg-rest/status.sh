#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "${NOVAROCKS_WORKSPACE_ROOT:-$SCRIPT_DIR/../..}" && pwd)"

slug="$(basename "$WORKSPACE_ROOT" | tr '[:upper:]' '[:lower:]' | tr -c 'a-z0-9' '-' | sed 's/^-*//;s/-*$//;s/--*/-/g')"
if [[ -z "$slug" ]]; then
  slug="novarocks"
fi
slug="$(printf '%s' "$slug" | cut -c1-24)"
hash="$(printf '%s' "$WORKSPACE_ROOT" | shasum -a 1 | awk '{print substr($1, 1, 8)}')"
env_id="${slug}-${hash}"
runtime_base="$SCRIPT_DIR/runtime"
runtime_dir="$runtime_base/$env_id"
current_link="$runtime_base/current"
compose_file="$SCRIPT_DIR/compose.yml"
compose_env="$runtime_dir/compose.env"
exports_file="$runtime_dir/env.sh"
compose_project="nr-${env_id}"

if [[ -f "$exports_file" ]]; then
  # shellcheck disable=SC1090
  source "$exports_file"
  compose_project="${NOVA_ENV_COMPOSE_PROJECT:-$compose_project}"
fi

if [[ ! -f "$compose_env" ]]; then
  echo "environment is not initialized: $runtime_dir"
  exit 0
fi

docker compose \
  --env-file "$compose_env" \
  -p "$compose_project" \
  -f "$compose_file" \
  ps

echo
echo "Fixed discovery entry:"
echo "  current: $current_link"
echo "  manifest: ${NOVA_ENV_MANIFEST:-$runtime_dir/manifest.json}"
echo "  readme: ${NOVA_ENV_README:-$runtime_dir/README.md}"
echo
echo "Generated environment:"
echo "  env: $exports_file"
echo "  standalone config: ${NOVAROCKS_STANDALONE_CONFIG:-$runtime_dir/standalone-managed-lake.toml}"
echo "  sql-test config: ${NOVAROCKS_SQL_TEST_CONFIG:-$runtime_dir/sql-test.conf}"
echo "  REST catalog SQL: ${NOVAROCKS_ICE_REST_CATALOG_SQL:-$runtime_dir/ice-rest-catalog.sql}"
echo "  REST warehouse: ${NOVAROCKS_ICEBERG_REST_WAREHOUSE:-unknown}"
echo "  Spark defaults: ${NOVAROCKS_SPARK_DEFAULTS:-$runtime_dir/spark-defaults.conf}"
echo "  Spark v3 smoke SQL: ${NOVAROCKS_SPARK_V3_SMOKE_SQL:-$runtime_dir/spark-iceberg-v3-smoke.sql}"
echo "  Spark SQL helper: ${NOVAROCKS_SPARK_SQL:-$SCRIPT_DIR/spark-sql.sh}"
