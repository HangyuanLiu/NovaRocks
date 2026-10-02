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
-- @order_sensitive=true
-- @tags=iceberg,field_domain,foreign,corrupt_metadata,physical_int32_overflow
-- All data values originate in the independent official SDK fixture. Invalid
-- declarations are persisted through real SDK property commits. Actual files
-- retain INT32/STRING primitives; overflow input never uses a SQL CAST.
-- Error detail assertions correspond to typed provider CorruptData or
-- ResourceExhausted causes; this runner has no public token for those kinds.
-- Receipts label expected kinds rather than claiming an observed wire code.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG invalid_domains_${uuid0}
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
CREATE DATABASE invalid_domains_${uuid0}.ns_${uuid0};
SET CATALOG invalid_domains_${uuid0};
USE ns_${uuid0};
SET enable_materialized_view_rewrite = false;


-- query 2
-- @skip_result_check=true
-- @result_contains=FIELD_DOMAIN_INVALID_READY
shell: set -eu
uea_workspace="${NOVAROCKS_WORKSPACE_ROOT:-.}"
export NOVA_ENV_REST_ENV_FILE="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "${NOVA_ENV_REST_ENV_FILE:-$uea_workspace/docker/iceberg-rest/runtime/current/env.sh}")"
uea_receipts="$uea_workspace/reports/uea7b3/field-domain-invalid/${uuid0}"
mkdir -p "$uea_receipts"
uea_scala="$uea_receipts/initialize.scala"
uea_log="$uea_receipts/initialize.log"
cat "$uea_workspace/tests/sql/fixtures/iceberg-delete-applicability/generate.scala" "$uea_workspace/tests/sql/fixtures/uea7b3-field-domain-invalid/fixture.scala" > "$uea_scala"
cat >> "$uea_scala" <<'SCALA'
try { FieldDomainInvalidFixture.run {
  DeleteApplicabilityFixture.initialize(org.apache.spark.sql.SparkSession.active, "ns_${uuid0}", "field_domain_invalid")
  FieldDomainInvalidFixture.initialize("ns_${uuid0}")
} } catch { case failure: Throwable => failure.printStackTrace(); System.exit(1) }
SCALA
if ! "$uea_workspace/docker/iceberg-rest/spark-shell.sh" "$uea_scala" > "$uea_log" 2>&1; then tail -80 "$uea_log" >&2; exit 1; fi
grep -q '^FIELD_DOMAIN_INVALID_READY$' "$uea_log" || { tail -80 "$uea_log" >&2; exit 1; }
python3 - "$uea_log" "$uea_receipts/initialize.json" <<'PY_RECEIPT'
import json,sys
matches=[]
with open(sys.argv[1],'rb') as log:
    lines=0
    while True:
        line=log.readline(256*1024+64)
        if not line: break
        lines+=1
        assert lines<=10000, 'Spark receipt/log line count budget exceeded'
        assert len(line)<256*1024+64, 'Spark receipt/log line budget exceeded'
        if line.startswith(b'UEA4G_RECEIPT '):
            payload=line[len(b'UEA4G_RECEIPT '):].rstrip(b'\r\n')
            assert len(payload)<=256*1024, 'Spark receipt budget exceeded'
            node=json.loads(payload)
            if node.get('record')=='field_domain_invalid_initial':
                assert isinstance(node.get('namespace'),str) and node['namespace']=='ns_${uuid0}'
                tables=node.get('tables')
                assert isinstance(tables,list) and len(tables)==20, 'missing exact owned table set'
                names=[t.get('case') for t in tables]
                assert len(set(names))==20, 'duplicate owned table receipt'
                for table in tables:
                    assert isinstance(table.get('table_uuid'),str) and table['table_uuid']
                    assert isinstance(table.get('metadata_sha256'),str) and len(table['metadata_sha256'])==64
                    assert type(table.get('snapshot')) is int and table['snapshot']>0
                    assert isinstance(table.get('schema_json'),str) and table['schema_json']
                    assert isinstance(table.get('retained_schemas'),list) and len(table['retained_schemas'])==1
                    physical=table.get('physical_file')
                    assert isinstance(physical,dict) and isinstance(physical.get('sha256'),str) and len(physical['sha256'])==64
                    assert isinstance(table.get('properties'),list) and isinstance(table.get('sdk_rows'),list)
                    assert isinstance(table.get('files'),list) and len(table['files'])==1
                matches.append(payload)
                assert len(matches)==1, 'duplicate exact stage receipt'
assert len(matches)==1, 'missing exact stage receipt'
with open(sys.argv[2],'wb') as output: output.write(matches[0]+b'\n')
PY_RECEIPT
printf 'FIELD_DOMAIN_INVALID_READY\n'

-- query 3
SELECT typeof(j) AS json_type,typeof(n) AS n_type,typeof(s) AS s_type,
       typeof(xs) AS xs_type,typeof(js) AS js_type FROM fd_foreign LIMIT 1;

-- query 4
SELECT id,j,n,s,xs,js FROM fd_foreign ORDER BY id;

-- query 5
SELECT typeof(j) AS json_type,typeof(n) AS n_type,typeof(s) AS s_type,
       typeof(xs) AS xs_type,typeof(js) AS js_type FROM fd_formal_empty LIMIT 1;

-- query 6
SELECT id,j,n,s,xs,js FROM fd_formal_empty ORDER BY id;

-- query 7
SELECT typeof(j) AS json_type,typeof(n) AS n_type,typeof(s) AS s_type,
       typeof(xs) AS xs_type,typeof(js) AS js_type FROM fd_valid LIMIT 1;

-- query 8
SELECT id,j,n,s,xs,js FROM fd_valid ORDER BY id;

-- query 9
-- @expect_error=unsupported Iceberg field domain namespace
SELECT j,n FROM fd_namespace;

-- query 10
-- @expect_error=unsupported Iceberg field domain version
SELECT j,n FROM fd_version;

-- query 11
-- @expect_error=invalid Iceberg field domains
SELECT j,n FROM fd_domain;

-- query 12
-- @expect_error=duplicate, noncanonical, invalid, or excessive field IDs
SELECT j,n FROM fd_duplicate;

-- query 13
-- @expect_error=Iceberg field domain requires its declared primitive storage
SELECT j,n FROM fd_carrier;

-- query 14
-- @expect_error=Iceberg field domain has no proven storage history
SELECT j,n FROM fd_history;

-- query 15
-- @expect_error=conflicting Iceberg field domain authorities
SELECT j,n FROM fd_dual;

-- query 16
-- @expect_error=invalid Iceberg field domains
SELECT j,n FROM fd_member;

-- query 17
-- @expect_error=Iceberg field domain payload exceeds its byte budget
SELECT j,n FROM fd_budget;

-- query 18
-- @expect_error=Iceberg visible INT exceeds its declared logical domain
SELECT n FROM fd_tiny_root_high;

-- query 19
-- @expect_error=Iceberg visible INT exceeds its declared logical domain
SELECT n FROM fd_tiny_root_low;

-- query 20
-- @expect_error=Iceberg visible INT exceeds its declared logical domain
SELECT xs FROM fd_tiny_list_high;

-- query 21
-- @expect_error=Iceberg visible INT exceeds its declared logical domain
SELECT xs FROM fd_tiny_list_low;

-- query 22
-- @expect_error=Iceberg visible INT exceeds its declared logical domain
SELECT n FROM fd_small_root_high;

-- query 23
-- @expect_error=Iceberg visible INT exceeds its declared logical domain
SELECT n FROM fd_small_root_low;

-- query 24
-- @expect_error=Iceberg visible INT exceeds its declared logical domain
SELECT xs FROM fd_small_list_high;

-- query 25
-- @expect_error=Iceberg visible INT exceeds its declared logical domain
SELECT xs FROM fd_small_list_low;

-- query 26
SELECT id,j,n,s,xs,js FROM fd_valid ORDER BY id;

-- query 27
-- @skip_result_check=true
-- @result_contains=FIELD_DOMAIN_INVALID_UNCHANGED
shell: set -eu
uea_workspace="${NOVAROCKS_WORKSPACE_ROOT:-.}"
export NOVA_ENV_REST_ENV_FILE="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "${NOVA_ENV_REST_ENV_FILE:-$uea_workspace/docker/iceberg-rest/runtime/current/env.sh}")"
uea_receipts="$uea_workspace/reports/uea7b3/field-domain-invalid/${uuid0}"
mkdir -p "$uea_receipts"
uea_scala="$uea_receipts/observe.scala"
uea_log="$uea_receipts/observe.log"
cat "$uea_workspace/tests/sql/fixtures/iceberg-delete-applicability/generate.scala" "$uea_workspace/tests/sql/fixtures/uea7b3-field-domain-invalid/fixture.scala" > "$uea_scala"
python3 - "$uea_receipts/initialize.json" "$uea_scala" <<'PY_FROZEN'
import base64,json,sys
from pathlib import Path
cap=256*1024
path=Path(sys.argv[1])
assert 0<path.stat().st_size<=cap, 'Frozen SDK receipt byte budget exceeded'
with path.open('rb') as source: raw=source.read(cap+1)
assert 0<len(raw)<=cap and raw.count(b'\n')<=1, 'Frozen SDK receipt byte/line budget exceeded'
def unique(pairs):
    node={}
    for key,value in pairs:
        assert key not in node, 'Duplicate frozen receipt key'
        node[key]=value
    return node
node=json.loads(raw,object_pairs_hook=unique)
assert isinstance(node,dict) and node.get('record')=='field_domain_invalid_initial'
assert node.get('namespace')=='ns_${uuid0}' and isinstance(node.get('tables'),list) and len(node['tables'])==20
canonical=json.dumps(node,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode('utf-8')
assert 0<len(canonical)<=cap, 'Canonical frozen receipt exceeds byte budget'
encoded=base64.b64encode(canonical).decode('ascii')
chunks=[encoded[i:i+16384] for i in range(0,len(encoded),16384)]
assert 0<len(chunks)<=22, 'Frozen receipt literal count exceeded'
with Path(sys.argv[2]).open('a') as script:
    script.write('\nval fieldDomainFrozen = Vector('+','.join(json.dumps(chunk) for chunk in chunks)+').mkString\n')
PY_FROZEN
cat >> "$uea_scala" <<'SCALA'
try { FieldDomainInvalidFixture.run {
  DeleteApplicabilityFixture.initialize(org.apache.spark.sql.SparkSession.active, "ns_${uuid0}", "field_domain_invalid")
  FieldDomainInvalidFixture.observe("ns_${uuid0}",fieldDomainFrozen)
} } catch { case failure: Throwable => failure.printStackTrace(); System.exit(1) }
SCALA
if ! "$uea_workspace/docker/iceberg-rest/spark-shell.sh" "$uea_scala" > "$uea_log" 2>&1; then tail -80 "$uea_log" >&2; exit 1; fi
grep -q '^FIELD_DOMAIN_INVALID_UNCHANGED$' "$uea_log" || { tail -80 "$uea_log" >&2; exit 1; }
python3 - "$uea_log" "$uea_receipts/observe.json" <<'PY_RECEIPT'
import json,sys
matches=[]
with open(sys.argv[1],'rb') as log:
    lines=0
    while True:
        line=log.readline(256*1024+64)
        if not line: break
        lines+=1
        assert lines<=10000, 'Spark receipt/log line count budget exceeded'
        assert len(line)<256*1024+64, 'Spark receipt/log line budget exceeded'
        if line.startswith(b'UEA4G_RECEIPT '):
            payload=line[len(b'UEA4G_RECEIPT '):].rstrip(b'\r\n')
            assert len(payload)<=256*1024, 'Spark receipt budget exceeded'
            node=json.loads(payload)
            if node.get('record')=='field_domain_invalid_unchanged':
                assert isinstance(node.get('namespace'),str) and node['namespace']=='ns_${uuid0}'
                tables=node.get('tables')
                assert isinstance(tables,list) and len(tables)==20, 'missing exact owned table set'
                names=[t.get('case') for t in tables]
                assert len(set(names))==20, 'duplicate owned table receipt'
                for table in tables:
                    assert isinstance(table.get('table_uuid'),str) and table['table_uuid']
                    assert isinstance(table.get('metadata_sha256'),str) and len(table['metadata_sha256'])==64
                    assert type(table.get('snapshot')) is int and table['snapshot']>0
                    assert isinstance(table.get('schema_json'),str) and table['schema_json']
                    assert isinstance(table.get('retained_schemas'),list) and len(table['retained_schemas'])==1
                    physical=table.get('physical_file')
                    assert isinstance(physical,dict) and isinstance(physical.get('sha256'),str) and len(physical['sha256'])==64
                    assert isinstance(table.get('properties'),list) and isinstance(table.get('sdk_rows'),list)
                    assert isinstance(table.get('files'),list) and len(table['files'])==1
                matches.append(payload)
                assert len(matches)==1, 'duplicate exact stage receipt'
assert len(matches)==1, 'missing exact stage receipt'
with open(sys.argv[2],'wb') as output: output.write(matches[0]+b'\n')
PY_RECEIPT
printf 'FIELD_DOMAIN_INVALID_UNCHANGED\n'

-- query 28
-- @skip_result_check=true
-- @cleanup=true
-- @result_contains=FIELD_DOMAIN_INVALID_CLEANED
shell: set -eu
uea_workspace="${NOVAROCKS_WORKSPACE_ROOT:-.}"
export NOVA_ENV_REST_ENV_FILE="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "${NOVA_ENV_REST_ENV_FILE:-$uea_workspace/docker/iceberg-rest/runtime/current/env.sh}")"
uea_receipts="$uea_workspace/reports/uea7b3/field-domain-invalid/${uuid0}"
mkdir -p "$uea_receipts"
uea_scala="$uea_receipts/cleanup.scala"
uea_log="$uea_receipts/cleanup.log"
cat "$uea_workspace/tests/sql/fixtures/iceberg-delete-applicability/generate.scala" "$uea_workspace/tests/sql/fixtures/uea7b3-field-domain-invalid/fixture.scala" > "$uea_scala"
python3 - "$uea_receipts/initialize.json" "$uea_scala" <<'PY_FROZEN'
import base64,json,sys
from pathlib import Path
cap=256*1024
path=Path(sys.argv[1])
assert 0<path.stat().st_size<=cap, 'Frozen SDK receipt byte budget exceeded'
with path.open('rb') as source: raw=source.read(cap+1)
assert 0<len(raw)<=cap and raw.count(b'\n')<=1, 'Frozen SDK receipt byte/line budget exceeded'
def unique(pairs):
    node={}
    for key,value in pairs:
        assert key not in node, 'Duplicate frozen receipt key'
        node[key]=value
    return node
node=json.loads(raw,object_pairs_hook=unique)
assert isinstance(node,dict) and node.get('record')=='field_domain_invalid_initial'
assert node.get('namespace')=='ns_${uuid0}' and isinstance(node.get('tables'),list) and len(node['tables'])==20
canonical=json.dumps(node,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode('utf-8')
assert 0<len(canonical)<=cap, 'Canonical frozen receipt exceeds byte budget'
encoded=base64.b64encode(canonical).decode('ascii')
chunks=[encoded[i:i+16384] for i in range(0,len(encoded),16384)]
assert 0<len(chunks)<=22, 'Frozen receipt literal count exceeded'
with Path(sys.argv[2]).open('a') as script:
    script.write('\nval fieldDomainFrozen = Vector('+','.join(json.dumps(chunk) for chunk in chunks)+').mkString\n')
PY_FROZEN
cat >> "$uea_scala" <<'SCALA'
try { FieldDomainInvalidFixture.run {
  DeleteApplicabilityFixture.initialize(org.apache.spark.sql.SparkSession.active, "ns_${uuid0}", "field_domain_invalid")
  FieldDomainInvalidFixture.cleanup("ns_${uuid0}",fieldDomainFrozen)
} } catch { case failure: Throwable => failure.printStackTrace(); System.exit(1) }
SCALA
if ! "$uea_workspace/docker/iceberg-rest/spark-shell.sh" "$uea_scala" > "$uea_log" 2>&1; then tail -80 "$uea_log" >&2; exit 1; fi
grep -q '^FIELD_DOMAIN_INVALID_CLEANED$' "$uea_log" || { tail -80 "$uea_log" >&2; exit 1; }
printf 'FIELD_DOMAIN_INVALID_CLEANED\n'

-- query 29
-- @cleanup=true
-- @skip_result_check=true
DROP DATABASE invalid_domains_${uuid0}.ns_${uuid0};
DROP CATALOG invalid_domains_${uuid0};
