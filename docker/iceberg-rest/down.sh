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
config_file="${NOVA_ENV_CONFIG_FILE:-$SCRIPT_DIR/shared.env}"

if [[ -f "$config_file" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$config_file"
  set +a
fi

shared_docker="${NOVA_ENV_SHARED_DOCKER:-true}"
configured_compose_project="${NOVA_ENV_SHARED_COMPOSE_PROJECT:-nr-iceberg-rest}"
if [[ "$shared_docker" == "true" ]]; then
  compose_project="$configured_compose_project"
  stop_docker=false
else
  compose_project="${NOVA_ENV_COMPOSE_PROJECT:-nr-${env_id}}"
  stop_docker=true
fi

if [[ -f "$exports_file" ]]; then
  # shellcheck disable=SC1090
  source "$exports_file"
  shared_docker="${NOVA_ENV_SHARED_DOCKER:-$shared_docker}"
  if [[ "$shared_docker" == "true" ]]; then
    compose_project="$configured_compose_project"
  else
    compose_project="${NOVA_ENV_COMPOSE_PROJECT:-$compose_project}"
  fi
fi

down_args=()
remove_runtime=false
purge_requested=false
for arg in "$@"; do
  case "$arg" in
    --docker)
      stop_docker=true
      ;;
    --runtime-only)
      stop_docker=false
      ;;
    -v|--volumes)
      down_args+=("--volumes")
      stop_docker=true
      ;;
    --purge)
      remove_runtime=true
      purge_requested=true
      ;;
    *)
      echo "unknown argument: $arg" >&2
      echo "usage: $0 [--docker] [--runtime-only] [--volumes|--purge]" >&2
      exit 2
      ;;
  esac
done

if [[ "$purge_requested" == true && "$stop_docker" == true ]]; then
  down_args+=("--volumes")
fi

if [[ "$stop_docker" == true ]]; then
  if [[ ! -f "$compose_env" ]]; then
    echo "environment is not initialized: $runtime_dir" >&2
  else
    docker compose \
      --env-file "$compose_env" \
      -p "$compose_project" \
      -f "$compose_file" \
      down "${down_args[@]}"
  fi
else
  echo "Shared Docker is left running: $compose_project"
fi

if [[ "$remove_runtime" == true ]]; then
  if [[ -L "$current_link" || -e "$current_link" ]]; then
    current_target="$(cd "$current_link" 2>/dev/null && pwd || true)"
    current_ref="$(readlink "$current_link" 2>/dev/null || true)"
    if [[ "$current_target" == "$runtime_dir" || "$current_ref" == "$env_id" ]]; then
      rm -rf "$current_link"
    fi
  fi
  rm -rf "$runtime_dir"
fi
