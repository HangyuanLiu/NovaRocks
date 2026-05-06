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
compose_project="nr-${env_id}"
runtime_dir="$SCRIPT_DIR/runtime/$env_id"
compose_file="$SCRIPT_DIR/iceberg-rest-compose.yml"
compose_env="$runtime_dir/compose.env"
exports_file="$runtime_dir/env.sh"

mkdir -p "$runtime_dir"

port_in_use() {
  local port="$1"
  lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1
}

choose_port() {
  local start="$1"
  local port="$start"
  local limit=$((start + 200))
  while port_in_use "$port"; do
    port=$((port + 1))
    if (( port > limit )); then
      echo "no free port near $start" >&2
      return 1
    fi
  done
  printf '%s\n' "$port"
}

hash4="${hash:0:4}"
offset=$((16#$hash4 % 1000))

if [[ -f "$exports_file" ]]; then
  # shellcheck disable=SC1090
  source "$exports_file"
  minio_port="${NOVA_ENV_MINIO_PORT}"
  minio_console_port="${NOVA_ENV_MINIO_CONSOLE_PORT}"
  rest_port="${NOVA_ENV_REST_PORT}"
  mysql_port="${NOVA_ENV_MYSQL_PORT}"
else
  minio_port="$(choose_port $((19000 + offset)))"
  minio_console_port="$(choose_port $((20000 + offset)))"
  rest_port="$(choose_port $((21000 + offset)))"
  mysql_port="$(choose_port $((23000 + offset)))"
fi

minio_user="${MINIO_ROOT_USER:-admin}"
minio_password="${MINIO_ROOT_PASSWORD:-admin123}"
rest_image="${ICEBERG_REST_IMAGE:-tabulario/iceberg-rest:1.6.0}"
rest_mirror_image="docker.1panel.live/tabulario/iceberg-rest:1.6.0"

if ! docker image inspect "$rest_image" >/dev/null 2>&1; then
  if [[ "$rest_image" == "tabulario/iceberg-rest:1.6.0" ]] \
    && docker image inspect "$rest_mirror_image" >/dev/null 2>&1; then
    docker tag "$rest_mirror_image" "$rest_image"
  else
    cat >&2 <<EOF
Missing Iceberg REST image: $rest_image

Pull it first, for example:
  docker pull docker.1panel.live/tabulario/iceberg-rest:1.6.0
  docker tag docker.1panel.live/tabulario/iceberg-rest:1.6.0 tabulario/iceberg-rest:1.6.0

Or set ICEBERG_REST_IMAGE to an already available image.
EOF
    exit 1
  fi
fi

managed_warehouse="s3://novarocks/$env_id/sql-tests-managed-lake"
iceberg_warehouse="s3://novarocks/$env_id/iceberg-catalog"
rest_warehouse="s3://warehouse/$env_id/rest"
minio_endpoint="http://127.0.0.1:$minio_port"
rest_uri="http://127.0.0.1:$rest_port"

cat > "$compose_env" <<EOF
NOVA_ENV_MINIO_PORT=$minio_port
NOVA_ENV_MINIO_CONSOLE_PORT=$minio_console_port
NOVA_ENV_REST_PORT=$rest_port
NOVA_ENV_REST_WAREHOUSE_URI=$rest_warehouse
MINIO_ROOT_USER=$minio_user
MINIO_ROOT_PASSWORD=$minio_password
ICEBERG_REST_IMAGE=$rest_image
EOF

cat > "$runtime_dir/standalone-managed-lake.toml" <<EOF
[server]
host = "127.0.0.1"

[standalone_server]
mysql_port = $mysql_port
user = "root"
metadata_db_path = "$runtime_dir/standalone-managed-lake.sqlite"
warehouse_uri = "$managed_warehouse"

[standalone_server.object_store]
endpoint = "$minio_endpoint"
access_key_id = "$minio_user"
access_key_secret = "$minio_password"
enable_path_style_access = true
EOF

cat > "$runtime_dir/sql-test.conf" <<EOF
[cluster]
host = 127.0.0.1
port = $mysql_port
user = root
password =

[env]
url = http://127.0.0.1:8030
oss_ak = $minio_user
oss_sk = $minio_password
oss_endpoint = $minio_endpoint
managed_lake_warehouse = $managed_warehouse
iceberg_catalog_type = hadoop
iceberg_catalog_warehouse = $iceberg_warehouse
EOF

cat > "$runtime_dir/ice-rest-catalog.sql" <<EOF
CREATE EXTERNAL CATALOG ice_rest
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "rest",
  "uri" = "$rest_uri",
  "warehouse" = "$rest_warehouse",
  "aws.s3.endpoint" = "$minio_endpoint",
  "aws.s3.access_key" = "$minio_user",
  "aws.s3.secret_key" = "$minio_password",
  "aws.s3.region" = "us-east-1",
  "aws.s3.enable_path_style_access" = "true"
);
EOF

cat > "$exports_file" <<EOF
export NOVAROCKS_WORKSPACE_ROOT="$WORKSPACE_ROOT"
export NOVA_ENV_ID="$env_id"
export NOVA_ENV_COMPOSE_PROJECT="$compose_project"
export NOVA_ENV_RUNTIME_DIR="$runtime_dir"
export NOVA_ENV_MINIO_PORT="$minio_port"
export NOVA_ENV_MINIO_CONSOLE_PORT="$minio_console_port"
export NOVA_ENV_REST_PORT="$rest_port"
export NOVA_ENV_MYSQL_PORT="$mysql_port"
export AWS_S3_ENDPOINT="$minio_endpoint"
export AWS_S3_ACCESS_KEY_ID="$minio_user"
export AWS_S3_SECRET_ACCESS_KEY="$minio_password"
export MINIO_ROOT_USER="$minio_user"
export MINIO_ROOT_PASSWORD="$minio_password"
export CATALOG_WAREHOUSE_URI="$iceberg_warehouse"
export NOVAROCKS_MANAGED_LAKE_WAREHOUSE="$managed_warehouse"
export NOVAROCKS_ICEBERG_REST_URI="$rest_uri"
export NOVAROCKS_STANDALONE_CONFIG="$runtime_dir/standalone-managed-lake.toml"
export NOVAROCKS_SQL_TEST_CONFIG="$runtime_dir/sql-test.conf"
export NOVAROCKS_ICE_REST_CATALOG_SQL="$runtime_dir/ice-rest-catalog.sql"
EOF

docker compose \
  --env-file "$compose_env" \
  -p "$compose_project" \
  -f "$compose_file" \
  up -d --remove-orphans

wait_http() {
  local url="$1"
  local name="$2"
  for _ in $(seq 1 60); do
    if curl -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "$name did not become ready at $url" >&2
  docker compose --env-file "$compose_env" -p "$compose_project" -f "$compose_file" logs --tail=120 >&2
  return 1
}

wait_http "$minio_endpoint/minio/health/live" "MinIO"
wait_http "$rest_uri/v1/config" "Iceberg REST"

cat <<EOF
NovaRocks workspace environment is ready.

Workspace: $WORKSPACE_ROOT
Environment id: $env_id
Runtime dir: $runtime_dir
Compose project: $compose_project

MinIO endpoint: $minio_endpoint
MinIO console: http://127.0.0.1:$minio_console_port
Iceberg REST: $rest_uri

Use:
  source "$exports_file"
  cargo run -- standalone-server --config "\$NOVAROCKS_STANDALONE_CONFIG"
  cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- --config "\$NOVAROCKS_SQL_TEST_CONFIG" --suite iceberg --mode verify
EOF
