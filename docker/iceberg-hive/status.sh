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
config_file="${NOVA_ENV_HIVE_CONFIG_FILE:-$SCRIPT_DIR/shared.env}"

if [[ -f "$config_file" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$config_file"
  set +a
fi

shared_docker="${NOVA_ENV_SHARED_DOCKER:-true}"
configured_hive_compose_project="${NOVA_ENV_SHARED_HIVE_COMPOSE_PROJECT:-nr-iceberg-hive}"
if [[ "$shared_docker" == "true" ]]; then
  hive_compose_project="$configured_hive_compose_project"
else
  hive_compose_project="${NOVA_ENV_HIVE_COMPOSE_PROJECT:-nr-iceberg-hive-${env_id}}"
fi

if [[ -f "$exports_file" ]]; then
  # shellcheck disable=SC1090
  source "$exports_file"
  shared_docker="${NOVA_ENV_SHARED_DOCKER:-$shared_docker}"
  if [[ "$shared_docker" == "true" ]]; then
    hive_compose_project="$configured_hive_compose_project"
  else
    hive_compose_project="${NOVA_ENV_HIVE_COMPOSE_PROJECT:-$hive_compose_project}"
  fi
fi

if [[ ! -f "$compose_env" ]]; then
  echo "environment is not initialized: $runtime_dir"
  exit 0
fi

docker compose \
  --env-file "$compose_env" \
  -p "$hive_compose_project" \
  -f "$compose_file" \
  ps

echo
echo "Fixed discovery entry:"
echo "  current: $current_link"
echo "  manifest: ${NOVA_ENV_HIVE_MANIFEST:-$runtime_dir/manifest.json}"
echo "  readme: ${NOVA_ENV_HIVE_README:-$runtime_dir/README.md}"
echo "  shared docker: ${NOVA_ENV_SHARED_DOCKER:-$shared_docker}"
echo "  shared config: ${NOVA_ENV_HIVE_CONFIG_FILE:-$config_file}"
echo
echo "Generated environment:"
echo "  env: $exports_file"
echo "  HMS URI: ${NOVAROCKS_ICEBERG_HMS_URI:-unknown}"
echo "  HMS warehouse: ${NOVAROCKS_ICEBERG_HMS_WAREHOUSE:-unknown}"
echo "  HMS catalog SQL: ${NOVAROCKS_ICE_HMS_CATALOG_SQL:-$runtime_dir/ice-hms-catalog.sql}"
echo "  Spark HMS defaults: ${NOVAROCKS_SPARK_HMS_DEFAULTS:-$runtime_dir/spark-hms-defaults.conf}"
echo "  REST Docker network: ${NOVA_ENV_REST_NETWORK:-unknown}"
