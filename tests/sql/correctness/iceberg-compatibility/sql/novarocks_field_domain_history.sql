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
-- @tags=iceberg,field_domain,schema_history,metadata_depth
-- Official SDK updates preserve exact field IDs and frozen declarations through
-- rename/reorder. Drop/re-add receives a new unmarked INTEGER ID. INT to LONG
-- promotion retains history, while current reads must no longer narrow LONG.
-- Each tagged snapshot is created after a real append under its actual schema.
-- SDK complete bags and raw Parquet facts independently check current/history;
-- native results freeze exact domains and rows, including duplicates and NULLs.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG domainhistory_${uuid0}
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
CREATE DATABASE domainhistory_${uuid0}.ns_${uuid0};
SET CATALOG domainhistory_${uuid0};
USE ns_${uuid0};
SET enable_materialized_view_rewrite = false;

-- query 2
-- @skip_result_check=true
-- @result_contains=FIELD_DOMAIN_HISTORY_READY
shell: set -eu
uea_workspace="${NOVAROCKS_WORKSPACE_ROOT:-.}"
export NOVA_ENV_REST_ENV_FILE="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "${NOVA_ENV_REST_ENV_FILE:-$uea_workspace/docker/iceberg-rest/runtime/current/env.sh}")"
uea_receipts="$uea_workspace/reports/uea7b3/field-domain-history/${uuid0}"
mkdir -p "$uea_receipts"
uea_scala="$uea_receipts/initial.scala"
uea_log="$uea_receipts/initial.log"
cat "$uea_workspace/tests/sql/fixtures/iceberg-delete-applicability/generate.scala" "$uea_workspace/tests/sql/fixtures/mv-visible-content-encodings/fixture.scala" "$uea_workspace/tests/sql/fixtures/uea7b3-metadata-history/fixture.scala" > "$uea_scala"
cat >> "$uea_scala" <<'SCALA'
try {
  DeleteApplicabilityFixture.initialize(org.apache.spark.sql.SparkSession.active, "ns_${uuid0}", "metadata_history")
  MetadataHistoryFixture.historyInitialize("ns_${uuid0}")
} catch { case failure: Throwable => failure.printStackTrace(); System.exit(1) }
SCALA
if ! "$uea_workspace/docker/iceberg-rest/spark-shell.sh" "$uea_scala" > "$uea_log" 2>&1; then tail -80 "$uea_log" >&2; exit 1; fi
grep -q '^FIELD_DOMAIN_HISTORY_READY$' "$uea_log" || { tail -80 "$uea_log" >&2; exit 1; }
grep '^UEA4G_RECEIPT ' "$uea_log" > "$uea_receipts/initial.jsonl"
printf 'FIELD_DOMAIN_HISTORY_READY\n'

-- query 3
SELECT id,payload.tiny AS tiny,payload.note AS note,payload.reused AS reused,plain,small AS small FROM domain_history ORDER BY id;

-- query 4
SELECT typeof(payload) AS payload_type,typeof(payload.tiny) AS tiny_type,typeof(payload.note) AS note_type,typeof(payload.reused) AS reused_type,typeof(plain) AS plain_type,typeof(small) AS small_type FROM domain_history WHERE id=2;

-- query 5
-- @skip_result_check=true
-- @result_contains=FIELD_DOMAIN_RENAME_READY
shell: set -eu
uea_workspace="${NOVAROCKS_WORKSPACE_ROOT:-.}"
export NOVA_ENV_REST_ENV_FILE="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "${NOVA_ENV_REST_ENV_FILE:-$uea_workspace/docker/iceberg-rest/runtime/current/env.sh}")"
uea_receipts="$uea_workspace/reports/uea7b3/field-domain-history/${uuid0}"
mkdir -p "$uea_receipts"
uea_scala="$uea_receipts/renamed.scala"
uea_log="$uea_receipts/renamed.log"
cat "$uea_workspace/tests/sql/fixtures/iceberg-delete-applicability/generate.scala" "$uea_workspace/tests/sql/fixtures/mv-visible-content-encodings/fixture.scala" "$uea_workspace/tests/sql/fixtures/uea7b3-metadata-history/fixture.scala" > "$uea_scala"
cat >> "$uea_scala" <<'SCALA'
try {
  DeleteApplicabilityFixture.initialize(org.apache.spark.sql.SparkSession.active, "ns_${uuid0}", "metadata_history")
  MetadataHistoryFixture.historyRenameReorder("ns_${uuid0}")
} catch { case failure: Throwable => failure.printStackTrace(); System.exit(1) }
SCALA
if ! "$uea_workspace/docker/iceberg-rest/spark-shell.sh" "$uea_scala" > "$uea_log" 2>&1; then tail -80 "$uea_log" >&2; exit 1; fi
grep -q '^FIELD_DOMAIN_RENAME_READY$' "$uea_log" || { tail -80 "$uea_log" >&2; exit 1; }
grep '^UEA4G_RECEIPT ' "$uea_log" > "$uea_receipts/renamed.jsonl"
printf 'FIELD_DOMAIN_RENAME_READY\n'

-- query 6
SELECT id,payload.tiny_renamed AS tiny,payload.note AS note,payload.reused AS reused,plain,small_renamed AS small FROM domain_history ORDER BY id;

-- query 7
SELECT typeof(payload) AS payload_type,typeof(payload.tiny_renamed) AS tiny_type,typeof(payload.note) AS note_type,typeof(payload.reused) AS reused_type,typeof(plain) AS plain_type,typeof(small_renamed) AS small_type FROM domain_history WHERE id=2;

-- query 8
-- @skip_result_check=true
-- @result_contains=FIELD_DOMAIN_READD_READY
shell: set -eu
uea_workspace="${NOVAROCKS_WORKSPACE_ROOT:-.}"
export NOVA_ENV_REST_ENV_FILE="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "${NOVA_ENV_REST_ENV_FILE:-$uea_workspace/docker/iceberg-rest/runtime/current/env.sh}")"
uea_receipts="$uea_workspace/reports/uea7b3/field-domain-history/${uuid0}"
mkdir -p "$uea_receipts"
uea_scala="$uea_receipts/readded.scala"
uea_log="$uea_receipts/readded.log"
cat "$uea_workspace/tests/sql/fixtures/iceberg-delete-applicability/generate.scala" "$uea_workspace/tests/sql/fixtures/mv-visible-content-encodings/fixture.scala" "$uea_workspace/tests/sql/fixtures/uea7b3-metadata-history/fixture.scala" > "$uea_scala"
cat >> "$uea_scala" <<'SCALA'
try {
  DeleteApplicabilityFixture.initialize(org.apache.spark.sql.SparkSession.active, "ns_${uuid0}", "metadata_history")
  MetadataHistoryFixture.historyDropReadd("ns_${uuid0}")
} catch { case failure: Throwable => failure.printStackTrace(); System.exit(1) }
SCALA
if ! "$uea_workspace/docker/iceberg-rest/spark-shell.sh" "$uea_scala" > "$uea_log" 2>&1; then tail -80 "$uea_log" >&2; exit 1; fi
grep -q '^FIELD_DOMAIN_READD_READY$' "$uea_log" || { tail -80 "$uea_log" >&2; exit 1; }
grep '^UEA4G_RECEIPT ' "$uea_log" > "$uea_receipts/readded.jsonl"
printf 'FIELD_DOMAIN_READD_READY\n'

-- query 9
SELECT id,payload.tiny_renamed AS tiny,payload.note AS note,payload.reused AS reused,plain,small_renamed AS small FROM domain_history ORDER BY id;

-- query 10
SELECT typeof(payload) AS payload_type,typeof(payload.tiny_renamed) AS tiny_type,typeof(payload.note) AS note_type,typeof(payload.reused) AS reused_type,typeof(plain) AS plain_type,typeof(small_renamed) AS small_type FROM domain_history WHERE id=2;

-- query 11
SELECT id,payload.tiny_renamed AS tiny,payload.note AS note,payload.reused AS reused,plain,small_renamed AS small FROM domain_history FOR VERSION AS OF 'domain_renamed' ORDER BY id;

-- query 12
SELECT typeof(payload) AS payload_type,typeof(payload.tiny_renamed) AS tiny_type,typeof(payload.note) AS note_type,typeof(payload.reused) AS reused_type,typeof(plain) AS plain_type,typeof(small_renamed) AS small_type FROM domain_history FOR VERSION AS OF 'domain_renamed' WHERE id=2;

-- query 13
-- @skip_result_check=true
-- @result_contains=FIELD_DOMAIN_PROMOTION_READY
shell: set -eu
uea_workspace="${NOVAROCKS_WORKSPACE_ROOT:-.}"
export NOVA_ENV_REST_ENV_FILE="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "${NOVA_ENV_REST_ENV_FILE:-$uea_workspace/docker/iceberg-rest/runtime/current/env.sh}")"
uea_receipts="$uea_workspace/reports/uea7b3/field-domain-history/${uuid0}"
mkdir -p "$uea_receipts"
uea_scala="$uea_receipts/promoted.scala"
uea_log="$uea_receipts/promoted.log"
cat "$uea_workspace/tests/sql/fixtures/iceberg-delete-applicability/generate.scala" "$uea_workspace/tests/sql/fixtures/mv-visible-content-encodings/fixture.scala" "$uea_workspace/tests/sql/fixtures/uea7b3-metadata-history/fixture.scala" > "$uea_scala"
cat >> "$uea_scala" <<'SCALA'
try {
  DeleteApplicabilityFixture.initialize(org.apache.spark.sql.SparkSession.active, "ns_${uuid0}", "metadata_history")
  MetadataHistoryFixture.historyPromote("ns_${uuid0}")
} catch { case failure: Throwable => failure.printStackTrace(); System.exit(1) }
SCALA
if ! "$uea_workspace/docker/iceberg-rest/spark-shell.sh" "$uea_scala" > "$uea_log" 2>&1; then tail -80 "$uea_log" >&2; exit 1; fi
grep -q '^FIELD_DOMAIN_PROMOTION_READY$' "$uea_log" || { tail -80 "$uea_log" >&2; exit 1; }
grep '^UEA4G_RECEIPT ' "$uea_log" > "$uea_receipts/promoted.jsonl"
printf 'FIELD_DOMAIN_PROMOTION_READY\n'

-- query 14
SELECT id,payload.tiny_renamed AS tiny,payload.note AS note,payload.reused AS reused,plain,small_renamed AS small FROM domain_history ORDER BY id;

-- query 15
SELECT typeof(payload) AS payload_type,typeof(payload.tiny_renamed) AS tiny_type,typeof(payload.note) AS note_type,typeof(payload.reused) AS reused_type,typeof(plain) AS plain_type,typeof(small_renamed) AS small_type FROM domain_history WHERE id=2;

-- query 16
SELECT id,payload.tiny_renamed AS tiny,payload.note AS note,payload.reused AS reused,plain,small_renamed AS small FROM domain_history FOR VERSION AS OF 'domain_before_promotion' ORDER BY id;

-- query 17
SELECT typeof(payload) AS payload_type,typeof(payload.tiny_renamed) AS tiny_type,typeof(payload.note) AS note_type,typeof(payload.reused) AS reused_type,typeof(plain) AS plain_type,typeof(small_renamed) AS small_type FROM domain_history FOR VERSION AS OF 'domain_before_promotion' WHERE id=2;

-- query 18
-- @cleanup=true
-- @skip_result_check=true
DROP TABLE IF EXISTS domainhistory_${uuid0}.ns_${uuid0}.domain_history FORCE;
DROP DATABASE domainhistory_${uuid0}.ns_${uuid0};
DROP CATALOG domainhistory_${uuid0};
