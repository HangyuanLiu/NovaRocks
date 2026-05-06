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
runtime_dir="$SCRIPT_DIR/runtime/$env_id"
compose_file="$SCRIPT_DIR/iceberg-rest-compose.yml"
compose_env="$runtime_dir/compose.env"
exports_file="$runtime_dir/env.sh"
compose_project="nr-${env_id}"

if [[ -f "$exports_file" ]]; then
  # shellcheck disable=SC1090
  source "$exports_file"
  compose_project="${NOVA_ENV_COMPOSE_PROJECT:-$compose_project}"
fi

down_args=()
remove_runtime=false
for arg in "$@"; do
  case "$arg" in
    -v|--volumes)
      down_args+=("--volumes")
      ;;
    --purge)
      down_args+=("--volumes")
      remove_runtime=true
      ;;
    *)
      echo "unknown argument: $arg" >&2
      echo "usage: $0 [--volumes|--purge]" >&2
      exit 2
      ;;
  esac
done

if [[ ! -f "$compose_env" ]]; then
  echo "environment is not initialized: $runtime_dir" >&2
  exit 0
fi

docker compose \
  --env-file "$compose_env" \
  -p "$compose_project" \
  -f "$compose_file" \
  down "${down_args[@]}"

if [[ "$remove_runtime" == true ]]; then
  rm -rf "$runtime_dir"
fi
