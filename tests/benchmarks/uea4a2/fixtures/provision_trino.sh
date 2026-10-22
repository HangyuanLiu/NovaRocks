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

if [[ $# -ne 1 ]]; then
  echo "usage: provision_trino.sh OUTPUT_DIR" >&2
  exit 2
fi

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd "$script_dir/../../../.." && pwd)
output_dir=$1
[[ ! -e "$output_dir" ]] || { echo "output directory already exists" >&2; exit 2; }
command -v docker >/dev/null || { echo "docker is required" >&2; exit 2; }
env_file="${NOVA_ENV_REST_ENV_FILE:-$repo_root/docker/iceberg-rest/runtime/current/env.sh}"
env_file="$(python3 -c 'import pathlib, sys; print(pathlib.Path(sys.argv[1]).resolve(strict=True))' "$env_file")"
source "$env_file"
owns_fixture_run=false
[[ "${NOVA_FIXTURE_RUN_CONTEXT:-null}" == null ]] && owns_fixture_run=true
NOVA_FIXTURE_RUN_CONTEXT="$(python3 "$repo_root/docker/iceberg-rest/fixture_run.py" context --env-file "$env_file")"
export NOVA_FIXTURE_RUN_CONTEXT
NOVA_FIXTURE_RUN_STORAGE="$(python3 "$repo_root/docker/iceberg-rest/fixture_run.py" storage --env-file "$env_file")"
export NOVA_FIXTURE_RUN_STORAGE
trino_owner=(python3 "$repo_root/docker/iceberg-rest/trino_run.py" --env-file "$env_file")

readonly image_digest=sha256:db58cc93e593a2706553745f276bb119c9810e69918be56ecde088ba7ccb0534
readonly image_ref=$image_digest
actual_image=$(docker image inspect "$image_ref" --format '{{.Id}}' 2>/dev/null) || {
  echo "pinned Trino 483 image is absent; provision it explicitly" >&2
  exit 1
}
[[ "$actual_image" == "$image_digest" ]] || { echo "Trino image digest mismatch" >&2; exit 1; }

mkdir -p "$output_dir"
output_dir=$(cd "$output_dir" && pwd)
container="nr-uea4a2-trino-$$"
schema="uea4a2_writer_$$"
schema_location=""
if [[ "$NOVA_FIXTURE_RUN_CONTEXT" != null ]]; then
  schema="$(python3 -c 'import json,sys;print(json.loads(sys.argv[1])["identity"]["run_id"]+"_tr")' "$NOVA_FIXTURE_RUN_CONTEXT")"
fi
table=sample
cleanup() {
  local primary=$? cleanup=0
  trap - EXIT INT TERM
  if [[ -n "${container_id:-}" ]]; then
    if [[ "$NOVA_FIXTURE_RUN_CONTEXT" == null ]]; then
      "${trino_owner[@]}" exec --container "$container_id" -- /usr/bin/trino --server http://localhost:8080 \
        --execute "DROP TABLE IF EXISTS iceberg.$schema.$table" >/dev/null || cleanup=$?
      "${trino_owner[@]}" exec --container "$container_id" -- /usr/bin/trino --server http://localhost:8080 \
        --execute "DROP SCHEMA IF EXISTS iceberg.$schema" >/dev/null || cleanup=$?
    fi
    "${trino_owner[@]}" stop --container "$container_id" || cleanup=$?
    if [[ "$NOVA_FIXTURE_RUN_CONTEXT" != null && "$owns_fixture_run" == true && "$cleanup" == 0 ]]; then
      NOVA_FIXTURE_CLEANUP_MODE=true python3 "$repo_root/docker/iceberg-rest/fixture_run.py" cleanup-run --env-file "$env_file" \
        >"$output_dir/cleanup-report.json" || cleanup=$?
    fi
  fi
  (( primary == 0 )) || exit "$primary"
  exit "$cleanup"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
container_id="$("${trino_owner[@]}" start --image "$image_ref" --name "$container" \
  --config-dir "$repo_root/tests/datasketches-tck/interop/trino/rest-catalog")"
container="$container_id"

healthy=false
for _ in $(seq 1 120); do
  if [[ "$(docker inspect "$container" --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' 2>/dev/null || true)" == healthy ]]; then
    healthy=true
    break
  fi
  sleep 1
done
[[ "$healthy" == true ]] || { docker logs "$container" >&2; exit 1; }
trino() { "${trino_owner[@]}" exec --container "$container" -- /usr/bin/trino --server http://localhost:8080 "$@"; }
version=$(trino --output-format TSV --execute 'SELECT version()')
[[ "$version" == 483 ]] || { echo "unexpected Trino version: $version" >&2; exit 1; }

sql="CREATE TABLE iceberg.$schema.$table WITH (format = 'PARQUET') AS SELECT CAST(id AS BIGINT) AS id, CAST(id % 17 AS INTEGER) AS category, CAST(id * 0.01 AS DECIMAL(12,2)) AS amount, CAST(ARRAY[CAST(id % 5 AS INTEGER), CAST(id % 7 AS INTEGER)] AS ARRAY(INTEGER)) AS nested FROM UNNEST(sequence(0, 4095)) AS t(id)"
location_clause=""
[[ -z "$schema_location" ]] || location_clause=" WITH (location = '$schema_location')"
trino --execute "CREATE SCHEMA iceberg.$schema$location_clause" >/dev/null
trino --execute "$sql" >/dev/null
rows=$(trino --output-format TSV --execute "SELECT count(*) FROM iceberg.$schema.$table")
[[ "$rows" == 4096 ]] || { echo "Trino row oracle failed: $rows" >&2; exit 1; }
object=$(trino --output-format TSV --execute "SELECT file_path FROM iceberg.$schema.\"$table\$files\"")
[[ "$object" == s3://* && "$object" != *$'\n'* ]] || {
  echo "Trino did not publish exactly one S3 Parquet data file" >&2
  exit 1
}
if [[ "$NOVA_FIXTURE_RUN_STORAGE" != null ]]; then
  python3 - "$repo_root/docker/iceberg-rest" "$NOVA_FIXTURE_RUN_STORAGE" "$object" <<'PYSCOPE'
import json,sys
sys.path.insert(0,sys.argv[1])
from scratch import RunStorageScope
if not RunStorageScope.from_dict(json.loads(sys.argv[2])).belongs(sys.argv[3]):
    raise SystemExit("InvalidScratchScope: actual Trino object is outside the original run storage ranges")
PYSCOPE
fi
python3 "$repo_root/docker/iceberg-rest/fixture_run.py" compose --env-file "$env_file" -- \
  --env-file "$NOVA_ENV_COMPOSE_ENV" -p "$NOVA_ENV_COMPOSE_PROJECT" -f "$NOVA_ENV_COMPOSE_FILE" \
  run --rm --no-deps --entrypoint /bin/sh mc -c \
  'mc alias set store http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null && mc cat "$1"' _ \
  "store/${object#s3://}" > "$output_dir/trino-483.parquet"

TRINO_OUTPUT_DIR="$output_dir" TRINO_IMAGE_DIGEST="$image_digest" \
TRINO_OBJECT="$object" TRINO_SQL="$sql" python3 - <<'PY'
import hashlib
import json
import os
from pathlib import Path

import pyarrow.parquet as pq

root = Path(os.environ['TRINO_OUTPUT_DIR'])
file = root / 'trino-483.parquet'
table = pq.read_table(file)
metadata = pq.ParquetFile(file).metadata
assert table.num_rows == 4096
receipt = {
    'schema_version': 1,
    'writer': 'Trino',
    'writer_version': '483',
    'image_digest': os.environ['TRINO_IMAGE_DIGEST'],
    'generator': 'provision_trino.sh',
    'sql': os.environ['TRINO_SQL'],
    'source_object': os.environ['TRINO_OBJECT'],
    'file': file.name,
    'sha256': hashlib.sha256(file.read_bytes()).hexdigest(),
    'file_bytes': file.stat().st_size,
    'rows': table.num_rows,
    'row_groups': metadata.num_row_groups,
    'columns': table.column_names,
    'created_by': metadata.created_by,
    'oracle_sha256': hashlib.sha256(
        json.dumps(table.to_pylist(), sort_keys=True, default=str).encode()
    ).hexdigest(),
}
(root / 'trino-manifest.json').write_text(json.dumps(receipt, indent=2, sort_keys=True) + '\n')
print(f"Trino corpus sha256:{receipt['sha256']} rows:{receipt['rows']}")
PY
