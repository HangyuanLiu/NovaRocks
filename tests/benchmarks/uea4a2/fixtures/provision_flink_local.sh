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

# This is an explicit alternative to provision_flink.sh when Docker Hub cannot
# supply its pinned Flink image. It runs the genuine Apache Flink 1.20.5 binary
# distribution with Java 17 and the matching Flink Parquet SQL bundle.
# https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/deployment/java_compatibility/
# https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/table/sqlclient/
# https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/connectors/table/formats/parquet/

readonly official_archive_url=https://archive.apache.org/dist/flink/flink-1.20.5/flink-1.20.5-bin-scala_2.12.tgz
readonly archive_sha512=c846ecbcc4a1705724832acdbad27538c31f17aa494e8b69ff5f92d724574be83d1dff0b363aa0570e5a448f529224705d967d0d14df3267143eec5064a52a1f
readonly jar_name=flink-sql-parquet-1.20.5.jar
readonly jar_url=https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-parquet/1.20.5/flink-sql-parquet-1.20.5.jar
readonly jar_sha256=6b98bbb43f6e4a721343621ec29850e0b0f19e55d9434624ffccf4cb1236f9f5
readonly hadoop_jar_name=flink-shaded-hadoop-2-uber-2.8.3-10.0.jar
readonly hadoop_jar_url=https://repo.maven.apache.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar
readonly hadoop_jar_sha256=492b2a559f2a1dad3808b51d9a26a575dbb1202004c9f85f5059c520e0632127

usage() {
  echo 'usage: provision_flink_local.sh OUTPUT_DIR | provision_flink_local.sh --verify OUTPUT_DIR' >&2
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

import pyarrow.parquet as pq

root = Path(os.environ['FLINK_CORPUS_DIR'])
receipt = json.loads((root / 'flink-local-manifest.json').read_text())

def require(condition, message):
    if not condition:
        raise ValueError(message)

def sha(path, algorithm='sha256'):
    digest = hashlib.new(algorithm)
    with path.open('rb') as source:
        for block in iter(lambda: source.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()

require(receipt.get('schema_version') == 1 and receipt.get('writer') == 'Flink', 'invalid Flink receipt')
require(receipt.get('provision_method') == 'official-archive-local', 'Flink provision method mismatch')
require(receipt.get('writer_version') == '1.20.5', 'Flink version mismatch')
archive = receipt['distribution_archive']
require(archive['official_url'] == 'https://archive.apache.org/dist/flink/flink-1.20.5/flink-1.20.5-bin-scala_2.12.tgz', 'Flink archive source mismatch')
require(archive['sha512'] == 'c846ecbcc4a1705724832acdbad27538c31f17aa494e8b69ff5f92d724574be83d1dff0b363aa0570e5a448f529224705d967d0d14df3267143eec5064a52a1f', 'Flink archive hash mismatch')
require(receipt['java']['major_version'] == 17, 'Flink local writer did not use Java 17')
require(receipt['parquet_bundle']['sha256'] == '6b98bbb43f6e4a721343621ec29850e0b0f19e55d9434624ffccf4cb1236f9f5', 'Flink Parquet bundle mismatch')
require(receipt['parquet_bundle']['url'] == 'https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-parquet/1.20.5/flink-sql-parquet-1.20.5.jar', 'Flink Parquet bundle source mismatch')
require(receipt['parameters'] == {'runtime_mode': 'batch', 'parallelism': 1, 'filesystem_sink': 'local', 'compression': 'SNAPPY', 'utc_timezone': True}, 'Flink SQL parameters mismatch')
require(receipt['sql_file'] == 'flink-local.sql', 'Flink SQL filename mismatch')
require(sha(root / receipt['sql_file']) == receipt['sql_sha256'], 'Flink SQL hash mismatch')
require(receipt['parquet_bundle']['file'] == 'flink-sql-parquet-1.20.5.jar', 'Flink bundle filename mismatch')
require(sha(root / receipt['parquet_bundle']['file']) == receipt['parquet_bundle']['sha256'], 'Flink bundle file hash mismatch')
require(receipt['hadoop_runtime']['file'] == 'flink-shaded-hadoop-2-uber-2.8.3-10.0.jar', 'Flink Hadoop runtime filename mismatch')
require(receipt['hadoop_runtime']['sha256'] == '492b2a559f2a1dad3808b51d9a26a575dbb1202004c9f85f5059c520e0632127', 'Flink Hadoop runtime hash mismatch')
require(receipt['hadoop_runtime']['url'] == 'https://repo.maven.apache.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar', 'Flink Hadoop runtime source mismatch')
require(sha(root / receipt['hadoop_runtime']['file']) == receipt['hadoop_runtime']['sha256'], 'Flink Hadoop runtime file hash mismatch')
require(receipt['file'] == 'flink-local-1.20.5.parquet', 'Flink data filename mismatch')
file = root / receipt['file']
require(file.is_file() and sha(file) == receipt['sha256'], 'Flink Parquet file missing or hash mismatch')
metadata = pq.read_metadata(file)
table = pq.read_table(file)
require(file.stat().st_size == receipt['file_bytes'], 'Flink Parquet size mismatch')
require(metadata.created_by == receipt['created_by'], 'Flink Parquet footer writer mismatch')
require(metadata.num_row_groups == receipt['row_groups'], 'Flink Parquet row groups mismatch')
require(table.column_names == receipt['columns'], 'Flink Parquet columns mismatch')
require(table.num_rows == receipt['rows'] == 4096, 'Flink row count mismatch')
rows = sorted(table.to_pylist(), key=lambda row: row['id'])
require([row['id'] for row in rows] == list(range(4096)), 'Flink row IDs mismatch')
for row in rows:
    value = row['id']
    require(row['category'] == value % 17, f'Flink category mismatch: {value}')
    require(row['label'] == (None if value % 17 == 0 else f'label-{value % 11}'), f'Flink label mismatch: {value}')
    require(row['amount'] == Decimal(value) / Decimal(100), f'Flink decimal mismatch: {value}')
    require(row['event_time'] == datetime(2024, 1, 1), f'Flink timestamp mismatch: {value}')
    require(row['nested'] == [value % 5, value % 7], f'Flink array mismatch: {value}')
oracle = hashlib.sha256(json.dumps(rows, sort_keys=True, default=str).encode()).hexdigest()
require(oracle == receipt['oracle_sha256'], 'Flink row oracle mismatch')
print(f"verified local Flink corpus rows:{len(rows)} sha256:{receipt['sha256']}")
PY
}

if [[ $mode == verify ]]; then
  [[ -d $output_dir ]] || { echo 'Flink corpus directory is missing' >&2; exit 1; }
  verify_fixture
  exit 0
fi

[[ ! -e $output_dir ]] || { echo 'output directory already exists' >&2; exit 2; }
command -v curl >/dev/null || { echo 'curl is required' >&2; exit 2; }
command -v python3 >/dev/null || { echo 'python3 with pyarrow is required' >&2; exit 2; }

java_home=${FLINK_JAVA_HOME:-/Users/harbor/project/starrocks/thirdparty/src/jdk-17.0.13+11/Contents/Home}
[[ -x $java_home/bin/java ]] || { echo 'a Java 17 runtime is required' >&2; exit 1; }
java_version=$("$java_home/bin/java" -version 2>&1)
[[ $java_version == *'version "17.'* ]] || { echo 'Flink 1.20 local provision requires Java 17' >&2; exit 1; }
export JAVA_HOME=$java_home
export PATH="$java_home/bin:$PATH"

archive_path=${FLINK_ARCHIVE:-/tmp/novarocks-uea4a2-flink-1.20.5-bin-scala_2.12.tgz}
transport_url=${FLINK_ARCHIVE_URL:-$official_archive_url}
if [[ ! -f $archive_path ]]; then
  echo "explicitly provisioning Flink archive from $transport_url" >&2
  curl --fail --location --silent --show-error --retry 3 --max-time 1800 \
    --output "$archive_path" "$transport_url"
fi
actual_archive_sha=$(shasum -a 512 "$archive_path" | awk '{print $1}')
[[ $actual_archive_sha == "$archive_sha512" ]] || { echo 'Flink official archive SHA-512 mismatch' >&2; exit 1; }

mkdir -p "$output_dir"
output_dir=$(cd "$output_dir" && pwd)
jar_path="$output_dir/$jar_name"
if [[ -n ${FLINK_PARQUET_JAR:-} ]]; then
  cp "$FLINK_PARQUET_JAR" "$jar_path"
else
  curl --fail --location --silent --show-error --retry 3 --max-time 180 \
    --output "$jar_path" "$jar_url"
fi
[[ $(shasum -a 256 "$jar_path" | awk '{print $1}') == "$jar_sha256" ]] || {
  echo 'Flink Parquet SQL bundle SHA-256 mismatch' >&2
  exit 1
}
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

temp_dir=$(mktemp -d '/tmp/uea4a2-flink-local.XXXXXX')
flink_home="$temp_dir/flink-1.20.5"
cluster_started=false
gateway_started=false
cleanup() {
  result=$?
  trap - EXIT INT TERM
  if [[ $gateway_started == true ]]; then
    "$flink_home/bin/sql-gateway.sh" stop >>"$output_dir/flink-stop.log" 2>&1 || result=1
  fi
  if [[ $cluster_started == true ]]; then
    "$flink_home/bin/stop-cluster.sh" >>"$output_dir/flink-stop.log" 2>&1 || result=1
  fi
  if [[ -n ${FLINK_PID_DIR:-} && -d $FLINK_PID_DIR ]]; then
    for pid_file in "$FLINK_PID_DIR"/*.pid; do
      [[ -f $pid_file ]] || continue
      pid=$(cat "$pid_file")
      if kill -0 "$pid" 2>/dev/null; then
        echo "Flink process still running after stop: $pid" >&2
        result=1
      fi
    done
  fi
  python3 - "$temp_dir" <<'PY'
import shutil
import sys
from pathlib import Path

directory = Path(sys.argv[1]).resolve()
if directory.parent not in (Path('/tmp'), Path('/private/tmp')) or not directory.name.startswith('uea4a2-flink-local.'):
    raise ValueError('refusing to clean an unrelated directory')
shutil.rmtree(directory)
PY
  exit "$result"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

tar -xzf "$archive_path" -C "$temp_dir"
[[ -x $flink_home/bin/sql-client.sh ]] || { echo 'Flink archive layout mismatch' >&2; exit 1; }
mkdir -p "$temp_dir/conf" "$temp_dir/pid" "$temp_dir/log"
cp -R "$flink_home/conf/." "$temp_dir/conf/"
export FLINK_CONF_DIR="$temp_dir/conf"
export FLINK_PID_DIR="$temp_dir/pid"
export FLINK_LOG_DIR="$temp_dir/log"
export FLINK_IDENT_STRING="uea4a2-$$"

ports=$(python3 - <<'PY'
import socket
ports = []
for _ in range(3):
    with socket.socket() as server:
        server.bind(('127.0.0.1', 0))
        ports.append(server.getsockname()[1])
print(*ports)
PY
)
read -r rpc_port rest_port gateway_port <<<"$ports"
config_file="$FLINK_CONF_DIR/config.yaml"
[[ -f $config_file ]] || config_file="$FLINK_CONF_DIR/flink-conf.yaml"
[[ -f $config_file ]] || { echo 'Flink configuration file missing' >&2; exit 1; }
cat >>"$config_file" <<EOF
jobmanager.rpc.address: localhost
jobmanager.rpc.port: $rpc_port
rest.address: localhost
rest.port: $rest_port
rest.bind-port: $rest_port
sql-gateway.endpoint.rest.address: localhost
sql-gateway.endpoint.rest.bind-address: 127.0.0.1
sql-gateway.endpoint.rest.port: $gateway_port
sql-gateway.endpoint.rest.bind-port: $gateway_port
taskmanager.numberOfTaskSlots: 2
parallelism.default: 1
EOF

FLINK_CORPUS_DIR="$output_dir" python3 - <<'PY'
import os
from pathlib import Path

root = Path(os.environ['FLINK_CORPUS_DIR'])
values = ', '.join(f'({index})' for index in range(64))
sink_uri = (root / 'data').as_uri().replace("'", "''")
sql = f"""SET 'execution.runtime-mode' = 'batch';
SET 'parallelism.default' = '1';
SET 'table.dml-sync' = 'true';
SET 'table.local-time-zone' = 'UTC';
CREATE TABLE flink_fixture (
  id BIGINT, category INT, label STRING, amount DECIMAL(12,2),
  event_time TIMESTAMP(3), nested ARRAY<INT>
) WITH (
  'connector' = 'filesystem',
  'path' = '{sink_uri}',
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
(root / 'flink-local.sql').write_text(sql)
PY

"$flink_home/bin/flink" --version >"$output_dir/flink-local-version.log" 2>&1
grep -Fq 'Version: 1.20.5' "$output_dir/flink-local-version.log" || {
  echo 'Flink distribution version mismatch' >&2
  exit 1
}
cp "$jar_path" "$flink_home/lib/$jar_name"
cp "$hadoop_jar_path" "$flink_home/lib/$hadoop_jar_name"
cluster_started=true
"$flink_home/bin/start-cluster.sh" >"$output_dir/flink-local-cluster.log" 2>&1
ready=false
for _ in $(seq 1 60); do
  if curl --fail --silent --max-time 1 "http://127.0.0.1:$rest_port/overview" >/dev/null; then
    ready=true
    break
  fi
  sleep 1
done
[[ $ready == true ]] || { echo 'Flink local cluster did not become ready' >&2; exit 1; }
gateway_started=true
"$flink_home/bin/sql-gateway.sh" start >"$output_dir/flink-local-gateway.log" 2>&1
ready=false
for _ in $(seq 1 60); do
  if curl --fail --silent --max-time 1 "http://127.0.0.1:$gateway_port/v1/info" >/dev/null; then
    ready=true
    break
  fi
  sleep 1
done
[[ $ready == true ]] || { echo 'Flink SQL Gateway did not become ready' >&2; exit 1; }
"$flink_home/bin/sql-client.sh" gateway --endpoint "http://127.0.0.1:$gateway_port" -f "$output_dir/flink-local.sql" \
  >"$output_dir/flink-local-sql.log" 2>&1 || {
  tail -80 "$output_dir/flink-local-sql.log" >&2
  exit 1
}
if grep -Fq '[ERROR]' "$output_dir/flink-local-sql.log"; then
  tail -80 "$output_dir/flink-local-sql.log" >&2
  exit 1
fi

FLINK_CORPUS_DIR="$output_dir" FLINK_ARCHIVE_TRANSPORT="$transport_url" \
FLINK_ARCHIVE_SHA="$archive_sha512" FLINK_JAVA_VERSION="$java_version" python3 - <<'PY'
import hashlib
import json
import os
import shutil
from pathlib import Path

import pyarrow.parquet as pq

root = Path(os.environ['FLINK_CORPUS_DIR'])
files = sorted(path for path in (root / 'data').rglob('part-*') if path.is_file())
if len(files) != 1:
    raise ValueError(f'expected exactly one Flink Parquet file, got {len(files)}')
source = files[0]
target = root / 'flink-local-1.20.5.parquet'
shutil.copyfile(source, target)
metadata = pq.read_metadata(target)
table = pq.read_table(target)
rows = sorted(table.to_pylist(), key=lambda row: row['id'])

def sha(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()

receipt = {
    'schema_version': 1,
    'writer': 'Flink',
    'writer_version': '1.20.5',
    'provision_method': 'official-archive-local',
    'generator': 'provision_flink_local.sh',
    'distribution_archive': {
        'official_url': 'https://archive.apache.org/dist/flink/flink-1.20.5/flink-1.20.5-bin-scala_2.12.tgz',
        'transport_url': os.environ['FLINK_ARCHIVE_TRANSPORT'],
        'sha512': os.environ['FLINK_ARCHIVE_SHA'],
    },
    'java': {'major_version': 17, 'runtime_version': os.environ['FLINK_JAVA_VERSION'].splitlines()[0]},
    'parquet_bundle': {
        'file': 'flink-sql-parquet-1.20.5.jar',
        'url': 'https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-parquet/1.20.5/flink-sql-parquet-1.20.5.jar',
        'sha256': sha(root / 'flink-sql-parquet-1.20.5.jar'),
    },
    'hadoop_runtime': {
        'file': 'flink-shaded-hadoop-2-uber-2.8.3-10.0.jar',
        'url': 'https://repo.maven.apache.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar',
        'sha256': sha(root / 'flink-shaded-hadoop-2-uber-2.8.3-10.0.jar'),
    },
    'sql_file': 'flink-local.sql',
    'sql_sha256': sha(root / 'flink-local.sql'),
    'parameters': {'runtime_mode': 'batch', 'parallelism': 1, 'filesystem_sink': 'local', 'compression': 'SNAPPY', 'utc_timezone': True},
    'file': target.name,
    'sha256': sha(target),
    'file_bytes': target.stat().st_size,
    'rows': table.num_rows,
    'row_groups': metadata.num_row_groups,
    'columns': table.column_names,
    'created_by': metadata.created_by,
    'oracle_sha256': hashlib.sha256(json.dumps(rows, sort_keys=True, default=str).encode()).hexdigest(),
}
(root / 'flink-local-manifest.json').write_text(json.dumps(receipt, indent=2, sort_keys=True) + '\n')
PY
verify_fixture
