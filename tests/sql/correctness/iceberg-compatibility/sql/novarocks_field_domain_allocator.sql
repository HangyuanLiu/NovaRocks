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
-- @tags=iceberg,field_domain,narrow,map_allocator,struct_key,ddl,ctas
-- SDK Map keys are records; no SQL map() constructor support is implied.
-- The returned Java SDK field IDs and raw Parquet fields independently prove
-- key/value IDs were allocated before the key's nested children.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG allocator_domains_${uuid0}
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
CREATE DATABASE allocator_domains_${uuid0}.ns_${uuid0};
SET CATALOG allocator_domains_${uuid0};
USE ns_${uuid0};
SET enable_materialized_view_rewrite = false;

-- query 2
-- @skip_result_check=true
CREATE TABLE domain_allocator (m MAP<STRUCT<k TINYINT>,SMALLINT>)
TBLPROPERTIES ("format-version"="3","write.row-lineage"="true");

-- query 3
-- @skip_result_check=true
-- @result_contains=FIELD_DOMAIN_ALLOCATOR_READY
shell: set -eu
uea_workspace="${NOVAROCKS_WORKSPACE_ROOT:-.}"
export NOVA_ENV_REST_ENV_FILE="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "${NOVA_ENV_REST_ENV_FILE:-$uea_workspace/docker/iceberg-rest/runtime/current/env.sh}")"
uea_receipts="$uea_workspace/reports/uea7b3/domain-allocator/${uuid0}"
mkdir -p "$uea_receipts"
uea_scala="$uea_receipts/initialize.scala"
uea_log="$uea_receipts/initialize.log"
cat "$uea_workspace/tests/sql/fixtures/iceberg-delete-applicability/generate.scala" "$uea_workspace/tests/sql/fixtures/mv-visible-content-encodings/fixture.scala" > "$uea_scala"
cat >> "$uea_scala" <<'SCALA'
try {
  DeleteApplicabilityFixture.initialize(org.apache.spark.sql.SparkSession.active, "ns_${uuid0}", "domain_allocator")
  FieldDomainAllocatorFixture.initialize("ns_${uuid0}")
} catch { case failure: Throwable => failure.printStackTrace(); System.exit(1) }
SCALA
if ! "$uea_workspace/docker/iceberg-rest/spark-shell.sh" "$uea_scala" > "$uea_log" 2>&1; then tail -80 "$uea_log" >&2; exit 1; fi
grep -q '^FIELD_DOMAIN_ALLOCATOR_READY$' "$uea_log" || { tail -80 "$uea_log" >&2; exit 1; }
grep '^UEA4G_RECEIPT ' "$uea_log" > "$uea_receipts/initialize.jsonl"
printf 'FIELD_DOMAIN_ALLOCATOR_READY\n'

-- query 4
SELECT m FROM domain_allocator;

-- query 5
-- @skip_result_check=true
CREATE TABLE domain_allocator_ctas
TBLPROPERTIES ("format-version"="3","write.row-lineage"="true") AS
SELECT m FROM domain_allocator;

-- query 6
SELECT m FROM domain_allocator_ctas;

-- query 7
-- @skip_result_check=true
CREATE TABLE domain_allocator_insert (m MAP<STRUCT<k TINYINT>,SMALLINT>)
TBLPROPERTIES ("format-version"="3","write.row-lineage"="true");

-- query 8
-- @skip_result_check=true
INSERT INTO domain_allocator_insert SELECT m FROM domain_allocator;

-- query 9
SELECT m FROM domain_allocator_insert;

-- query 10
-- @skip_result_check=true
-- @result_contains=FIELD_DOMAIN_ALLOCATOR_OBSERVED
shell: set -eu
uea_workspace="${NOVAROCKS_WORKSPACE_ROOT:-.}"
export NOVA_ENV_REST_ENV_FILE="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "${NOVA_ENV_REST_ENV_FILE:-$uea_workspace/docker/iceberg-rest/runtime/current/env.sh}")"
uea_receipts="$uea_workspace/reports/uea7b3/domain-allocator/${uuid0}"
mkdir -p "$uea_receipts"
uea_scala="$uea_receipts/observe.scala"
uea_log="$uea_receipts/observe.log"
cat "$uea_workspace/tests/sql/fixtures/iceberg-delete-applicability/generate.scala" "$uea_workspace/tests/sql/fixtures/mv-visible-content-encodings/fixture.scala" > "$uea_scala"
cat >> "$uea_scala" <<'SCALA'
try {
  DeleteApplicabilityFixture.initialize(org.apache.spark.sql.SparkSession.active, "ns_${uuid0}", "domain_allocator")
  FieldDomainAllocatorFixture.observe("ns_${uuid0}")
} catch { case failure: Throwable => failure.printStackTrace(); System.exit(1) }
SCALA
if ! "$uea_workspace/docker/iceberg-rest/spark-shell.sh" "$uea_scala" > "$uea_log" 2>&1; then tail -80 "$uea_log" >&2; exit 1; fi
grep -q '^FIELD_DOMAIN_ALLOCATOR_OBSERVED$' "$uea_log" || { tail -80 "$uea_log" >&2; exit 1; }
grep '^UEA4G_RECEIPT ' "$uea_log" > "$uea_receipts/observe.jsonl"
printf 'FIELD_DOMAIN_ALLOCATOR_OBSERVED\n'

-- query 11
-- @cleanup=true
-- @skip_result_check=true
DROP TABLE IF EXISTS allocator_domains_${uuid0}.ns_${uuid0}.domain_allocator_insert FORCE;
DROP TABLE IF EXISTS allocator_domains_${uuid0}.ns_${uuid0}.domain_allocator_ctas FORCE;
DROP TABLE IF EXISTS allocator_domains_${uuid0}.ns_${uuid0}.domain_allocator FORCE;
DROP DATABASE allocator_domains_${uuid0}.ns_${uuid0};
DROP CATALOG allocator_domains_${uuid0};
