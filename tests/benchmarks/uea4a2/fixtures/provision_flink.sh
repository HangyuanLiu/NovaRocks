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

# Flink 1.20 SQL Client, filesystem and Parquet contracts:
# https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/table/sqlclient/
# https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/connectors/table/filesystem/
# https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/connectors/table/formats/parquet/
# Official Docker Hub image identity:
# https://hub.docker.com/layers/library/flink/1.20.5-scala_2.12-java17/images/sha256-580ce3348fd8d0909fc535c9bf274b03e169ace39c88562ca231a761e33ea301

readonly image_tag=1.20.5-scala_2.12-java17
readonly image_index_digest=sha256:a0fcd5400842d794f9a1b6292a148db1b3ca217bcf68741cc538e8bc9bc0b5ae
readonly image_arm64_digest=sha256:580ce3348fd8d0909fc535c9bf274b03e169ace39c88562ca231a761e33ea301
readonly jar_name=flink-sql-parquet-1.20.5.jar
readonly jar_url=https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-parquet/1.20.5/flink-sql-parquet-1.20.5.jar
readonly jar_sha256=6b98bbb43f6e4a721343621ec29850e0b0f19e55d9434624ffccf4cb1236f9f5
readonly hadoop_jar_name=flink-shaded-hadoop-2-uber-2.8.3-10.0.jar
readonly hadoop_jar_url=https://repo.maven.apache.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar
readonly hadoop_jar_sha256=492b2a559f2a1dad3808b51d9a26a575dbb1202004c9f85f5059c520e0632127

usage() {
  echo 'usage: provision_flink.sh OUTPUT_DIR | provision_flink.sh --verify OUTPUT_DIR' >&2
  exit 2
}

[[ $# -eq 1 || ( $# -eq 2 && $1 == --verify ) ]] || usage
if [[ $1 == --verify ]]; then
  output_dir=$2
  mode=verify
else
  output_dir=$1
  mode=provision
fi

verify_fixture() {
  FLINK_CORPUS_DIR="$output_dir" python3 - <<'PY'
import hashlib
import json
import os
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

root = Path(os.environ['FLINK_CORPUS_DIR'])
receipt = json.loads((root / 'flink-manifest.json').read_text())

def require(condition, message):
    if not condition:
        raise ValueError(message)

require(receipt['schema_version'] == 1 and receipt['writer'] == 'Flink', 'invalid Flink receipt')
require(receipt['writer_version'] == '1.20.5', 'Flink version mismatch')
require(receipt['image_index_digest'] == 'sha256:a0fcd5400842d794f9a1b6292a148db1b3ca217bcf68741cc538e8bc9bc0b5ae', 'Flink image index mismatch')
require(receipt['image_platform_manifest_digest'] == 'sha256:580ce3348fd8d0909fc535c9bf274b03e169ace39c88562ca231a761e33ea301', 'Flink image manifest mismatch')
require(receipt['parquet_bundle']['sha256'] == '6b98bbb43f6e4a721343621ec29850e0b0f19e55d9434624ffccf4cb1236f9f5', 'Flink Parquet bundle mismatch')
require(receipt['hadoop_runtime']['sha256'] == '492b2a559f2a1dad3808b51d9a26a575dbb1202004c9f85f5059c520e0632127', 'Flink Hadoop runtime mismatch')
require(receipt['hadoop_runtime']['url'] == 'https://repo.maven.apache.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar', 'Flink Hadoop runtime source mismatch')
require(receipt['parameters'] == {'runtime_mode': 'batch', 'parallelism': 1, 'filesystem_sink': 'local', 'compression': 'SNAPPY', 'utc_timezone': True}, 'Flink SQL parameters mismatch')

def sha(path):
    digest = hashlib.sha256()
    with path.open('rb') as source:
        for block in iter(lambda: source.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()

require(sha(root / receipt['sql_file']) == receipt['sql_sha256'], 'Flink SQL hash mismatch')
require(sha(root / receipt['parquet_bundle']['file']) == receipt['parquet_bundle']['sha256'], 'Flink Parquet bundle hash mismatch')
require(receipt['hadoop_runtime']['file'] == 'flink-shaded-hadoop-2-uber-2.8.3-10.0.jar', 'Flink Hadoop runtime filename mismatch')
require(sha(root / receipt['hadoop_runtime']['file']) == receipt['hadoop_runtime']['sha256'], 'Flink Hadoop runtime hash mismatch')
require(receipt['files'], 'Flink produced no Parquet files')
tables = []
for item in receipt['files']:
    path = root / item['file']
    require(path.is_file() and sha(path) == item['sha256'], f'Parquet file missing or hash mismatch: {path}')
    metadata = pq.read_metadata(path)
    table = pq.read_table(path)
    require(path.stat().st_size == item['file_bytes'], f'Parquet file size mismatch: {path}')
    require(metadata.created_by == item['created_by'], f'Parquet writer mismatch: {path}')
    require(metadata.num_row_groups == item['row_groups'], f'Parquet row group mismatch: {path}')
    require(table.num_rows == item['rows'], f'Parquet row count mismatch: {path}')
    require(table.column_names == receipt['columns'], f'Parquet schema mismatch: {path}')
    tables.append(table)
table = pa.concat_tables(tables).combine_chunks()
require(table.num_rows == 4096, 'Flink corpus row count mismatch')
rows = sorted(table.to_pylist(), key=lambda row: row['id'])
require([row['id'] for row in rows] == list(range(4096)), 'Flink corpus IDs mismatch')
for row in rows:
    value = row['id']
    require(row['category'] == value % 17, f'Flink category mismatch: {value}')
    require(row['label'] == (None if value % 17 == 0 else f'label-{value % 11}'), f'Flink label mismatch: {value}')
    require(row['amount'] == Decimal(value) / Decimal(100), f'Flink amount mismatch: {value}')
    require(row['event_time'] == datetime(2024, 1, 1), f'Flink timestamp mismatch: {value}')
    require(row['nested'] == [value % 5, value % 7], f'Flink nested value mismatch: {value}')
oracle = hashlib.sha256(json.dumps(rows, sort_keys=True, default=str).encode()).hexdigest()
require(oracle == receipt['oracle_sha256'], 'Flink row oracle mismatch')
print(f"verified Flink corpus rows:{len(rows)} files:{len(tables)} oracle:{oracle}")
PY
}

if [[ $mode == verify ]]; then
  [[ -d $output_dir ]] || { echo 'Flink corpus directory is missing' >&2; exit 1; }
  verify_fixture
  exit 0
fi

[[ ! -e $output_dir ]] || { echo 'output directory already exists' >&2; exit 2; }
command -v docker >/dev/null || { echo 'docker is required' >&2; exit 2; }
command -v curl >/dev/null || { echo 'curl is required' >&2; exit 2; }
command -v python3 >/dev/null || { echo 'python3 with pyarrow is required' >&2; exit 2; }

image_ref=${FLINK_IMAGE:-docker.m.daocloud.io/library/flink:$image_tag}
image_info=$(docker image inspect "$image_ref" --format '{{.Os}}/{{.Architecture}} {{.Id}} {{range .RepoDigests}}{{.}} {{end}}' 2>/dev/null) || {
  echo "Flink image absent: $image_ref; pull the official tag explicitly before provisioning" >&2
  exit 1
}
[[ $image_info == linux/arm64\ * ]] || { echo 'Flink image must be linux/arm64' >&2; exit 1; }
if [[ $image_info != *"@$image_arm64_digest"* && $image_info != *"@$image_index_digest"* && $image_info != *" $image_arm64_digest "* ]]; then
  echo 'Flink image does not expose the official pinned manifest digest' >&2
  exit 1
fi

mkdir -p "$output_dir"
output_dir=$(cd "$output_dir" && pwd)
jar_path="$output_dir/$jar_name"
if [[ -n ${FLINK_PARQUET_JAR:-} ]]; then
  cp "$FLINK_PARQUET_JAR" "$jar_path"
else
  # This explicit provision step may download. --verify above remains offline.
  curl --fail --location --silent --show-error --retry 3 --max-time 180 \
    --output "$jar_path" "$jar_url"
fi
actual_jar_sha=$(shasum -a 256 "$jar_path" | awk '{print $1}')
[[ $actual_jar_sha == "$jar_sha256" ]] || { echo 'Flink Parquet SQL bundle hash mismatch' >&2; exit 1; }
hadoop_jar_path="$output_dir/$hadoop_jar_name"
if [[ -n ${FLINK_HADOOP_JAR:-} ]]; then
  cp "$FLINK_HADOOP_JAR" "$hadoop_jar_path"
else
  curl --fail --location --silent --show-error --retry 3 --max-time 180 \
    --output "$hadoop_jar_path" "$hadoop_jar_url"
fi
[[ $(shasum -a 256 "$hadoop_jar_path" | awk '{print $1}') == "$hadoop_jar_sha256" ]] || {
  echo 'Flink Hadoop runtime SHA-256 mismatch' >&2
  exit 1
}

FLINK_CORPUS_DIR="$output_dir" python3 - <<'PY'
import os
from pathlib import Path

root = Path(os.environ['FLINK_CORPUS_DIR'])
values = ', '.join(f'({index})' for index in range(64))
sql = f"""SET 'execution.runtime-mode' = 'batch';
SET 'parallelism.default' = '1';
SET 'table.dml-sync' = 'true';
SET 'table.local-time-zone' = 'UTC';
CREATE TABLE flink_fixture (
  id BIGINT, category INT, label STRING, amount DECIMAL(12,2),
  event_time TIMESTAMP(3), nested ARRAY<INT>
) WITH (
  'connector' = 'filesystem',
  'path' = 'file:///work/data',
  'format' = 'parquet',
  'parquet.utc-timezone' = 'true',
  'parquet.compression' = 'SNAPPY'
);
INSERT INTO flink_fixture
SELECT CAST(x.id AS BIGINT), CAST(MOD(x.id, 17) AS INT),
  CASE WHEN MOD(x.id, 17) = 0 THEN CAST(NULL AS STRING)
       ELSE CONCAT('label-', CAST(MOD(x.id, 11) AS STRING)) END,
  CAST(x.id * 0.01 AS DECIMAL(12,2)),
  TIMESTAMP '2024-01-01 00:00:00',
  ARRAY[CAST(MOD(x.id, 5) AS INT), CAST(MOD(x.id, 7) AS INT)]
FROM (
  SELECT a.n * 64 + b.n AS id
  FROM (VALUES {values}) AS a(n)
  CROSS JOIN (VALUES {values}) AS b(n)
) AS x;
"""
(root / 'flink.sql').write_text(sql)
PY

container="nr-uea4a2-flink-$$"
cleanup() {
  result=$?
  trap - EXIT INT TERM
  if docker inspect "$container" >/dev/null 2>&1; then
    docker rm -f "$container" >/dev/null 2>&1 || result=1
    if docker inspect "$container" >/dev/null 2>&1; then
      echo "Flink container still exists after cleanup: $container" >&2
      result=1
    fi
  fi
  exit "$result"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

docker run --detach --rm --pull never --user 0:0 \
  --name "$container" --label org.novarocks.test=uea4a2-writer \
  --mount "type=bind,src=$output_dir,dst=/work" \
  --entrypoint /bin/bash "$image_ref" -lc 'sleep infinity' >/dev/null
docker exec "$container" cp "/work/$jar_name" "/opt/flink/lib/$jar_name"
docker exec "$container" cp "/work/$hadoop_jar_name" "/opt/flink/lib/$hadoop_jar_name"
docker exec "$container" /opt/flink/bin/start-cluster.sh >"$output_dir/flink-cluster.log" 2>&1
docker exec "$container" /opt/flink/bin/flink --version >"$output_dir/flink-version.log" 2>&1
grep -Fq 'Version: 1.20.5' "$output_dir/flink-version.log" || {
  echo 'Flink runtime version mismatch' >&2
  exit 1
}
docker exec "$container" /opt/flink/bin/sql-client.sh embedded \
  -j "/work/$jar_name" -f /work/flink.sql >"$output_dir/flink-sql.log" 2>&1 || {
  tail -80 "$output_dir/flink-sql.log" >&2
  exit 1
}
if grep -Fq '[ERROR]' "$output_dir/flink-sql.log"; then
  tail -80 "$output_dir/flink-sql.log" >&2
  exit 1
fi

FLINK_CORPUS_DIR="$output_dir" FLINK_IMAGE_REF="$image_ref" \
FLINK_IMAGE_INFO="$image_info" FLINK_JAR_URL="$jar_url" \
FLINK_IMAGE_INDEX_DIGEST="$image_index_digest" \
FLINK_IMAGE_ARM64_DIGEST="$image_arm64_digest" python3 - <<'PY'
import hashlib
import json
import os
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

root = Path(os.environ['FLINK_CORPUS_DIR'])
files = sorted(path for path in (root / 'data').rglob('part-*') if path.is_file())
if not files:
    raise ValueError('Flink filesystem sink did not publish a Parquet file')

def sha(path):
    digest = hashlib.sha256()
    with path.open('rb') as source:
        for block in iter(lambda: source.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()

receipts = []
tables = []
for file in files:
    metadata = pq.read_metadata(file)
    table = pq.read_table(file)
    receipts.append({
        'file': str(file.relative_to(root)), 'sha256': sha(file),
        'file_bytes': file.stat().st_size, 'rows': table.num_rows,
        'row_groups': metadata.num_row_groups, 'created_by': metadata.created_by,
    })
    tables.append(table)
table = pa.concat_tables(tables).combine_chunks()
rows = sorted(table.to_pylist(), key=lambda row: row['id'])
receipt = {
    'schema_version': 1,
    'writer': 'Flink',
    'writer_version': '1.20.5',
    'generator': 'provision_flink.sh',
    'image_tag': '1.20.5-scala_2.12-java17',
    'image_ref': os.environ['FLINK_IMAGE_REF'],
    'image_index_digest': os.environ['FLINK_IMAGE_INDEX_DIGEST'],
    'image_platform_manifest_digest': os.environ['FLINK_IMAGE_ARM64_DIGEST'],
    'image_inspect': os.environ['FLINK_IMAGE_INFO'],
    'parquet_bundle': {
        'file': 'flink-sql-parquet-1.20.5.jar',
        'url': os.environ['FLINK_JAR_URL'],
        'sha256': sha(root / 'flink-sql-parquet-1.20.5.jar'),
    },
    'hadoop_runtime': {
        'file': 'flink-shaded-hadoop-2-uber-2.8.3-10.0.jar',
        'url': 'https://repo.maven.apache.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar',
        'sha256': sha(root / 'flink-shaded-hadoop-2-uber-2.8.3-10.0.jar'),
    },
    'sql_file': 'flink.sql',
    'sql_sha256': sha(root / 'flink.sql'),
    'parameters': {'runtime_mode': 'batch', 'parallelism': 1, 'filesystem_sink': 'local', 'compression': 'SNAPPY', 'utc_timezone': True},
    'files': receipts,
    'rows': len(rows),
    'columns': table.column_names,
    'oracle_sha256': hashlib.sha256(json.dumps(rows, sort_keys=True, default=str).encode()).hexdigest(),
}
(root / 'flink-manifest.json').write_text(json.dumps(receipt, indent=2, sort_keys=True) + '\n')
PY
verify_fixture
