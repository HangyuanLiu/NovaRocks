-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- @sequential=true
-- @order_sensitive=false
-- @tags=iceberg,recursive_schema,ddl,ctas,required,map_order
-- Compare three complete native six-row bags; Map entry order within each
-- cell remains significant. The source recipe independently fixes each row.
-- SDK/Parquet required and field-ID evidence distinguishes ordinary DDL
-- defaults from CTAS's already-proved source children. No encoding claim.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG recursive_ddl_ctas_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "rest",
  "uri" = "${iceberg_rest_uri}",
  "warehouse" = "${iceberg_rest_warehouse}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "credential.object-store-metadata.consumer-role" = "frontend",
  "credential.object-store-metadata.mode" = "static",
  "credential.object-store-metadata.name" = "${iceberg_object_store_credential_name}",
  "credential.object-store-metadata.generation" = "${iceberg_object_store_credential_generation}",
  "credential.object-store-data.consumer-role" = "backend",
  "credential.object-store-data.mode" = "static",
  "credential.object-store-data.name" = "${iceberg_object_store_credential_name}",
  "credential.object-store-data.generation" = "${iceberg_object_store_credential_generation}",
  "aws.s3.region" = "us-east-1",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE recursive_ddl_ctas_${uuid0}.ns_${uuid0};
SET CATALOG recursive_ddl_ctas_${uuid0};
USE ns_${uuid0};
SET enable_materialized_view_rewrite = false;

-- query 2
-- @skip_result_check=true
-- @result_contains=RECURSIVE_SOURCE_READY
shell: set -eu
uea_workspace="${NOVAROCKS_WORKSPACE_ROOT:-.}"
export NOVA_ENV_REST_ENV_FILE="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "${NOVA_ENV_REST_ENV_FILE:-$uea_workspace/docker/iceberg-rest/runtime/current/env.sh}")"
uea_receipts="$uea_workspace/reports/uea7b3/recursive-ddl-ctas/${uuid0}"
mkdir -p "$uea_receipts"
uea_scala="$uea_receipts/initial-source.scala"
uea_log="$uea_receipts/initial-source.log"
cat "$uea_workspace/tests/sql/fixtures/iceberg-delete-applicability/generate.scala" "$uea_workspace/tests/sql/fixtures/mv-visible-content-encodings/fixture.scala" > "$uea_scala"
cat >> "$uea_scala" <<'SCALA'
try {
  DeleteApplicabilityFixture.initialize(org.apache.spark.sql.SparkSession.active, "ns_${uuid0}", "recursive_ddl_ctas")
  RecursiveTypeFixture.initialize("ns_${uuid0}")
} catch { case failure: Throwable => failure.printStackTrace(); System.exit(1) }
SCALA
if ! "$uea_workspace/docker/iceberg-rest/spark-shell.sh" "$uea_scala" > "$uea_log" 2>&1; then tail -80 "$uea_log" >&2; exit 1; fi
grep -q '^RECURSIVE_SOURCE_READY$' "$uea_log" || { tail -80 "$uea_log" >&2; exit 1; }
python3 - "$uea_log" "$uea_receipts/initial-source.json" <<'PY_RECEIPT'
import json,sys
matches=[]
with open(sys.argv[1],'rb') as log:
    while True:
        line=log.readline(256*1024+64)
        if not line: break
        assert len(line)<256*1024+64, 'Spark receipt/log line budget exceeded'
        if line.startswith(b'UEA4G_RECEIPT '):
            payload=line[len(b'UEA4G_RECEIPT '):].rstrip(b'\r\n')
            assert len(payload)<=256*1024, 'Spark receipt budget exceeded'
            node=json.loads(payload)
            if node.get('record')=='recursive_source_initial':
                matches.append(payload)
                assert len(matches)==1, 'duplicate exact stage receipt'
assert len(matches)==1, 'missing exact stage receipt'
with open(sys.argv[2],'wb') as output: output.write(matches[0]+b'\n')
PY_RECEIPT
printf 'RECURSIVE_SOURCE_READY\n'

-- query 3
SELECT label,payload,ordered FROM recursive_source;

-- query 4
-- @skip_result_check=true
CREATE TABLE recursive_ddl (
  label STRING,
  payload STRUCT<items:ARRAY<BIGINT>,attrs:MAP<STRING,BIGINT>,detail:STRUCT<code:BIGINT,note:STRING>>,
  ordered MAP<STRING,BIGINT>
) TBLPROPERTIES ("format-version"="3","write.row-lineage"="true");

-- query 5
-- @skip_result_check=true
-- @result_contains=RECURSIVE_DDL_CTAS_PREPARED
shell: set -eu
uea_workspace="${NOVAROCKS_WORKSPACE_ROOT:-.}"
export NOVA_ENV_REST_ENV_FILE="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "${NOVA_ENV_REST_ENV_FILE:-$uea_workspace/docker/iceberg-rest/runtime/current/env.sh}")"
uea_receipts="$uea_workspace/reports/uea7b3/recursive-ddl-ctas/${uuid0}"
mkdir -p "$uea_receipts"
uea_scala="$uea_receipts/prepared.scala"
uea_log="$uea_receipts/prepared.log"
cat "$uea_workspace/tests/sql/fixtures/iceberg-delete-applicability/generate.scala" "$uea_workspace/tests/sql/fixtures/mv-visible-content-encodings/fixture.scala" > "$uea_scala"
cat >> "$uea_scala" <<'SCALA'
try {
  DeleteApplicabilityFixture.initialize(org.apache.spark.sql.SparkSession.active, "ns_${uuid0}", "recursive_ddl_ctas")
  RecursiveTypeFixture.prepareDdlCtas("ns_${uuid0}")
} catch { case failure: Throwable => failure.printStackTrace(); System.exit(1) }
SCALA
if ! "$uea_workspace/docker/iceberg-rest/spark-shell.sh" "$uea_scala" > "$uea_log" 2>&1; then tail -80 "$uea_log" >&2; exit 1; fi
grep -q '^RECURSIVE_DDL_CTAS_PREPARED$' "$uea_log" || { tail -80 "$uea_log" >&2; exit 1; }
python3 - "$uea_log" "$uea_receipts/prepared.json" <<'PY_RECEIPT'
import json,sys
matches=[]
with open(sys.argv[1],'rb') as log:
    while True:
        line=log.readline(256*1024+64)
        if not line: break
        assert len(line)<256*1024+64, 'Spark receipt/log line budget exceeded'
        if line.startswith(b'UEA4G_RECEIPT '):
            payload=line[len(b'UEA4G_RECEIPT '):].rstrip(b'\r\n')
            assert len(payload)<=256*1024, 'Spark receipt budget exceeded'
            node=json.loads(payload)
            if node.get('record')=='recursive_ddl_ctas_prepared':
                matches.append(payload)
                assert len(matches)==1, 'duplicate exact stage receipt'
assert len(matches)==1, 'missing exact stage receipt'
with open(sys.argv[2],'wb') as output: output.write(matches[0]+b'\n')
PY_RECEIPT
printf 'RECURSIVE_DDL_CTAS_PREPARED\n'

-- query 6
-- @skip_result_check=true
INSERT INTO recursive_ddl (label,payload,ordered)
SELECT label,payload,ordered FROM recursive_source;

-- query 7
SELECT label,payload,ordered FROM recursive_ddl;

-- query 8
-- @skip_result_check=true
CREATE TABLE recursive_ctas
TBLPROPERTIES ("format-version"="3","write.row-lineage"="true") AS
SELECT label,payload,ordered FROM recursive_source;

-- query 9
SELECT label,payload,ordered FROM recursive_ctas;

-- query 10
-- @skip_result_check=true
-- @result_contains=RECURSIVE_DDL_CTAS_OBSERVED
shell: set -eu
uea_workspace="${NOVAROCKS_WORKSPACE_ROOT:-.}"
export NOVA_ENV_REST_ENV_FILE="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "${NOVA_ENV_REST_ENV_FILE:-$uea_workspace/docker/iceberg-rest/runtime/current/env.sh}")"
uea_receipts="$uea_workspace/reports/uea7b3/recursive-ddl-ctas/${uuid0}"
mkdir -p "$uea_receipts"
uea_scala="$uea_receipts/observed.scala"
uea_log="$uea_receipts/observed.log"
cat "$uea_workspace/tests/sql/fixtures/iceberg-delete-applicability/generate.scala" "$uea_workspace/tests/sql/fixtures/mv-visible-content-encodings/fixture.scala" > "$uea_scala"
uea_frozen="$(python3 - "$uea_receipts/prepared.json" <<'PY_RECEIPT'
import base64,json,sys
with open(sys.argv[1],'rb') as receipt:
    raw=receipt.read(256*1024+1)
assert 0<len(raw)<=256*1024, 'frozen receipt budget exceeded'
node=json.loads(raw)
assert node['record']=='recursive_ddl_ctas_prepared' and node['namespace']=='ns_${uuid0}'
print(base64.b64encode(raw).decode('ascii'))
PY_RECEIPT
)"
cat >> "$uea_scala" <<SCALA
try {
  DeleteApplicabilityFixture.initialize(org.apache.spark.sql.SparkSession.active, "ns_${uuid0}", "recursive_ddl_ctas")
  RecursiveTypeFixture.observeDdlCtas("ns_${uuid0}",new String(java.util.Base64.getDecoder.decode("$uea_frozen"),java.nio.charset.StandardCharsets.UTF_8))
} catch { case failure: Throwable => failure.printStackTrace(); System.exit(1) }
SCALA
if ! "$uea_workspace/docker/iceberg-rest/spark-shell.sh" "$uea_scala" > "$uea_log" 2>&1; then tail -80 "$uea_log" >&2; exit 1; fi
grep -q '^RECURSIVE_DDL_CTAS_OBSERVED$' "$uea_log" || { tail -80 "$uea_log" >&2; exit 1; }
python3 - "$uea_log" "$uea_receipts/observed.json" <<'PY_RECEIPT'
import json,sys
matches=[]
with open(sys.argv[1],'rb') as log:
    while True:
        line=log.readline(256*1024+64)
        if not line: break
        assert len(line)<256*1024+64, 'Spark receipt/log line budget exceeded'
        if line.startswith(b'UEA4G_RECEIPT '):
            payload=line[len(b'UEA4G_RECEIPT '):].rstrip(b'\r\n')
            assert len(payload)<=256*1024, 'Spark receipt budget exceeded'
            node=json.loads(payload)
            if node.get('record')=='recursive_ddl_ctas_observed':
                matches.append(payload)
                assert len(matches)==1, 'duplicate exact stage receipt'
assert len(matches)==1, 'missing exact stage receipt'
with open(sys.argv[2],'wb') as output: output.write(matches[0]+b'\n')
PY_RECEIPT
printf 'RECURSIVE_DDL_CTAS_OBSERVED\n'

-- query 11
-- @cleanup=true
-- @skip_result_check=true
DROP TABLE IF EXISTS recursive_ddl_ctas_${uuid0}.ns_${uuid0}.recursive_ctas FORCE;
DROP TABLE IF EXISTS recursive_ddl_ctas_${uuid0}.ns_${uuid0}.recursive_ddl FORCE;
DROP TABLE IF EXISTS recursive_ddl_ctas_${uuid0}.ns_${uuid0}.recursive_source FORCE;
DROP DATABASE recursive_ddl_ctas_${uuid0}.ns_${uuid0};
DROP CATALOG recursive_ddl_ctas_${uuid0};
