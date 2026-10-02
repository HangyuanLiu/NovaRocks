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
-- @tags=iceberg,field_domain,json,null_map_key,ctas,mv
-- Reject actual visible NULL keys without advancing the lake publication.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG null_key_domains_${uuid0}
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
CREATE DATABASE null_key_domains_${uuid0}.ns_${uuid0};
SET CATALOG null_key_domains_${uuid0};
USE ns_${uuid0};
SET enable_materialized_view_rewrite = false;

-- query 2
-- @skip_result_check=true
CREATE TABLE null_key_guard (id BIGINT, m MAP<INT,JSON>)
TBLPROPERTIES ("format-version"="3","write.row-lineage"="true");
INSERT INTO null_key_guard VALUES
(1,map{1:CAST('{"b":2,"a":1}' AS JSON)}),
(2,CAST(NULL AS MAP<INT,JSON>)),
(3,CAST(map{} AS MAP<INT,JSON>)),
(4,map{2:CAST(NULL AS JSON)});
CREATE MATERIALIZED VIEW null_key_mv
DISTRIBUTED BY HASH(id) BUCKETS 3
REFRESH DEFERRED MANUAL
PROPERTIES ('storage_engine'='iceberg')
AS SELECT id,map{CAST(NULL AS INT):CAST(id AS JSON)} AS m FROM null_key_guard;

-- query 3
SELECT id,m FROM null_key_guard;

-- query 4
-- @skip_result_check=true
-- @result_contains=FIELD_DOMAIN_NULL_KEY_FROZEN
shell: set -eu
uea_workspace="${NOVAROCKS_WORKSPACE_ROOT:-.}"
export NOVA_ENV_REST_ENV_FILE="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "${NOVA_ENV_REST_ENV_FILE:-$uea_workspace/docker/iceberg-rest/runtime/current/env.sh}")"
export UEA_DOMAIN_GUARD_RECEIPTS="$uea_workspace/reports/uea7b3/domain-null-key/${uuid0}"
mkdir -p "$UEA_DOMAIN_GUARD_RECEIPTS"
uea_scala="$UEA_DOMAIN_GUARD_RECEIPTS/before.scala"
uea_log="$UEA_DOMAIN_GUARD_RECEIPTS/before.log"
cat "$uea_workspace/tests/sql/fixtures/iceberg-delete-applicability/generate.scala" "$uea_workspace/tests/sql/fixtures/mv-visible-content-encodings/fixture.scala" > "$uea_scala"
cat >> "$uea_scala" <<'SCALA'
try { FieldDomainNullKeyFixture.run {
  DeleteApplicabilityFixture.initialize(org.apache.spark.sql.SparkSession.active, "ns_${uuid0}", "domain_null_key")
  FieldDomainNullKeyFixture.before("ns_${uuid0}")
}} catch { case failure: Throwable => failure.printStackTrace(); System.exit(1) }
SCALA
if ! "$uea_workspace/docker/iceberg-rest/spark-shell.sh" "$uea_scala" > "$uea_log" 2>&1; then tail -80 "$uea_log" >&2; exit 1; fi
grep -q '^FIELD_DOMAIN_NULL_KEY_FROZEN$' "$uea_log" || { tail -80 "$uea_log" >&2; exit 1; }
grep '^UEA4G_RECEIPT ' "$uea_log" > "$UEA_DOMAIN_GUARD_RECEIPTS/before.jsonl"
printf 'FIELD_DOMAIN_NULL_KEY_FROZEN\n'

-- query 5
-- @expect_error=NULL map key
INSERT INTO null_key_guard VALUES (5,map{CAST(NULL AS INT):CAST('{}' AS JSON)});

-- query 6
-- @expect_error=NULL map key
CREATE TABLE null_key_ctas
TBLPROPERTIES ("format-version"="3","write.row-lineage"="true") AS
SELECT map{CAST(NULL AS INT):CAST(id AS JSON)} AS m FROM null_key_guard;

-- query 7
-- @expect_error=NULL map key
REFRESH MATERIALIZED VIEW null_key_mv WITH SYNC MODE;

-- query 8
-- @skip_result_check=true
-- @result_contains=FIELD_DOMAIN_NULL_KEY_UNCOMMITTED
shell: set -eu
uea_workspace="${NOVAROCKS_WORKSPACE_ROOT:-.}"
export NOVA_ENV_REST_ENV_FILE="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "${NOVA_ENV_REST_ENV_FILE:-$uea_workspace/docker/iceberg-rest/runtime/current/env.sh}")"
export UEA_DOMAIN_GUARD_RECEIPTS="$uea_workspace/reports/uea7b3/domain-null-key/${uuid0}"
mkdir -p "$UEA_DOMAIN_GUARD_RECEIPTS"
uea_scala="$UEA_DOMAIN_GUARD_RECEIPTS/after.scala"
uea_log="$UEA_DOMAIN_GUARD_RECEIPTS/after.log"
cat "$uea_workspace/tests/sql/fixtures/iceberg-delete-applicability/generate.scala" "$uea_workspace/tests/sql/fixtures/mv-visible-content-encodings/fixture.scala" > "$uea_scala"
uea_frozen="$(python3 -c 'import json,sys,base64; from pathlib import Path; p=Path(sys.argv[1]); assert 0<p.stat().st_size<=1048576; rows=[json.loads(line.removeprefix("UEA4G_RECEIPT ")) for line in p.read_text().splitlines() if line.startswith("UEA4G_RECEIPT ")]; rows=[r for r in rows if r.get("record")=="field_domain_null_key_before"]; assert len(rows)==1; raw=json.dumps(rows[0],separators=(",",":"),ensure_ascii=False).encode(); assert 0<len(raw)<=262144; print(base64.b64encode(raw).decode())' "$UEA_DOMAIN_GUARD_RECEIPTS/before.jsonl")"
printf '\nval nullKeyFrozen = DeleteApplicabilityFixture.mapper.readTree(java.util.Base64.getDecoder.decode("%s"))\n' "$uea_frozen" >> "$uea_scala"
cat >> "$uea_scala" <<'SCALA'
try { FieldDomainNullKeyFixture.run {
  DeleteApplicabilityFixture.initialize(org.apache.spark.sql.SparkSession.active, "ns_${uuid0}", "domain_null_key")
  FieldDomainNullKeyFixture.after("ns_${uuid0}",nullKeyFrozen)
}} catch { case failure: Throwable => failure.printStackTrace(); System.exit(1) }
SCALA
if ! "$uea_workspace/docker/iceberg-rest/spark-shell.sh" "$uea_scala" > "$uea_log" 2>&1; then tail -80 "$uea_log" >&2; exit 1; fi
grep -q '^FIELD_DOMAIN_NULL_KEY_UNCOMMITTED$' "$uea_log" || { tail -80 "$uea_log" >&2; exit 1; }
grep '^UEA4G_RECEIPT ' "$uea_log" > "$UEA_DOMAIN_GUARD_RECEIPTS/after.jsonl"
printf 'FIELD_DOMAIN_NULL_KEY_UNCOMMITTED\n'

-- query 9
SELECT id,m FROM null_key_guard;

-- query 10
-- @cleanup=true
-- @skip_result_check=true
DROP MATERIALIZED VIEW IF EXISTS null_key_domains_${uuid0}.ns_${uuid0}.null_key_mv;
DROP TABLE IF EXISTS null_key_domains_${uuid0}.ns_${uuid0}.null_key_ctas FORCE;
DROP TABLE IF EXISTS null_key_domains_${uuid0}.ns_${uuid0}.null_key_guard FORCE;
DROP DATABASE null_key_domains_${uuid0}.ns_${uuid0};
DROP CATALOG null_key_domains_${uuid0};
