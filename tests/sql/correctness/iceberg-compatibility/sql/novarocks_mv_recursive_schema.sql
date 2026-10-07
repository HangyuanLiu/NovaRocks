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
-- @tags=mv,iceberg,visible_bag,recursive_schema,required,map_order
-- This case proves recursive schema bindings and ordered content bags.
-- Dictionary/plain page evidence belongs to the separate original encoding case.
-- Source and target content bags are independently checked in Spark, including
-- nullable/empty recursive values and array order; native refresh owns apply.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG recursive_encodings_${uuid0}
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
CREATE DATABASE recursive_encodings_${uuid0}.ns_${uuid0};
SET CATALOG recursive_encodings_${uuid0};
USE ns_${uuid0};
SET enable_materialized_view_rewrite = false;

-- query 2
-- @skip_result_check=true
-- @result_contains=RECURSIVE_SOURCE_READY
shell: set -eu
uea_workspace="${NOVAROCKS_WORKSPACE_ROOT:-.}"
export NOVA_ENV_REST_ENV_FILE="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "${NOVA_ENV_REST_ENV_FILE:-$uea_workspace/docker/iceberg-rest/runtime/current/env.sh}")"
uea_receipts="$uea_workspace/reports/uea7b3/recursive-schema/${uuid0}"
mkdir -p "$uea_receipts"
uea_scala="$uea_receipts/initial-source.scala"
uea_log="$uea_receipts/initial-source.log"
cat "$uea_workspace/tests/sql/fixtures/iceberg-delete-applicability/generate.scala" "$uea_workspace/tests/sql/fixtures/mv-visible-content-encodings/fixture.scala" > "$uea_scala"
cat >> "$uea_scala" <<'SCALA'
try {
  DeleteApplicabilityFixture.initialize(org.apache.spark.sql.SparkSession.active, "ns_${uuid0}", "recursive_types")
  RecursiveTypeFixture.initialize("ns_${uuid0}")
} catch { case failure: Throwable => failure.printStackTrace(); System.exit(1) }
SCALA
if ! "$uea_workspace/docker/iceberg-rest/spark-shell.sh" "$uea_scala" > "$uea_log" 2>&1; then tail -80 "$uea_log" >&2; exit 1; fi
grep -q '^RECURSIVE_SOURCE_READY$' "$uea_log" || { tail -80 "$uea_log" >&2; exit 1; }
grep '^UEA4G_RECEIPT ' "$uea_log" > "$uea_receipts/initial-source.jsonl"
printf 'RECURSIVE_SOURCE_READY\n'

-- query 3
-- @order_sensitive=false
-- @imv_equivalence_check=recursive_mv
CREATE MATERIALIZED VIEW recursive_mv DISTRIBUTED BY HASH(label) BUCKETS 3 REFRESH DEFERRED MANUAL PROPERTIES ('storage_engine'='iceberg') AS SELECT label,payload,ordered,js,smalls FROM recursive_source;
REFRESH MATERIALIZED VIEW recursive_mv WITH SYNC MODE;
SELECT label,payload,ordered,js,smalls FROM recursive_mv;

-- query 4
-- @skip_result_check=true
-- @result_contains=RECURSIVE_MV_OBSERVED
shell: set -eu
uea_workspace="${NOVAROCKS_WORKSPACE_ROOT:-.}"
export NOVA_ENV_REST_ENV_FILE="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "${NOVA_ENV_REST_ENV_FILE:-$uea_workspace/docker/iceberg-rest/runtime/current/env.sh}")"
uea_receipts="$uea_workspace/reports/uea7b3/recursive-schema/${uuid0}"
mkdir -p "$uea_receipts"
uea_scala="$uea_receipts/initial-target.scala"
uea_log="$uea_receipts/initial-target.log"
cat "$uea_workspace/tests/sql/fixtures/iceberg-delete-applicability/generate.scala" "$uea_workspace/tests/sql/fixtures/mv-visible-content-encodings/fixture.scala" > "$uea_scala"
cat >> "$uea_scala" <<'SCALA'
try {
  DeleteApplicabilityFixture.initialize(org.apache.spark.sql.SparkSession.active, "ns_${uuid0}", "recursive_types")
  RecursiveTypeFixture.observe("ns_${uuid0}","initial")
} catch { case failure: Throwable => failure.printStackTrace(); System.exit(1) }
SCALA
if ! "$uea_workspace/docker/iceberg-rest/spark-shell.sh" "$uea_scala" > "$uea_log" 2>&1; then tail -80 "$uea_log" >&2; exit 1; fi
grep -q '^RECURSIVE_MV_OBSERVED$' "$uea_log" || { tail -80 "$uea_log" >&2; exit 1; }
grep '^UEA4G_RECEIPT ' "$uea_log" > "$uea_receipts/initial-target.jsonl"
printf 'RECURSIVE_MV_OBSERVED\n'

-- query 5
-- @skip_result_check=true
-- @result_contains=RECURSIVE_SOURCE_CHANGED
shell: set -eu
uea_workspace="${NOVAROCKS_WORKSPACE_ROOT:-.}"
export NOVA_ENV_REST_ENV_FILE="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "${NOVA_ENV_REST_ENV_FILE:-$uea_workspace/docker/iceberg-rest/runtime/current/env.sh}")"
uea_receipts="$uea_workspace/reports/uea7b3/recursive-schema/${uuid0}"
mkdir -p "$uea_receipts"
uea_scala="$uea_receipts/delta-source.scala"
uea_log="$uea_receipts/delta-source.log"
cat "$uea_workspace/tests/sql/fixtures/iceberg-delete-applicability/generate.scala" "$uea_workspace/tests/sql/fixtures/mv-visible-content-encodings/fixture.scala" > "$uea_scala"
cat >> "$uea_scala" <<'SCALA'
try {
  DeleteApplicabilityFixture.initialize(org.apache.spark.sql.SparkSession.active, "ns_${uuid0}", "recursive_types")
  RecursiveTypeFixture.mutate("ns_${uuid0}")
} catch { case failure: Throwable => failure.printStackTrace(); System.exit(1) }
SCALA
if ! "$uea_workspace/docker/iceberg-rest/spark-shell.sh" "$uea_scala" > "$uea_log" 2>&1; then tail -80 "$uea_log" >&2; exit 1; fi
grep -q '^RECURSIVE_SOURCE_CHANGED$' "$uea_log" || { tail -80 "$uea_log" >&2; exit 1; }
grep '^UEA4G_RECEIPT ' "$uea_log" > "$uea_receipts/delta-source.jsonl"
printf 'RECURSIVE_SOURCE_CHANGED\n'

-- query 6
-- @order_sensitive=false
-- @imv_equivalence_check=recursive_mv
REFRESH MATERIALIZED VIEW recursive_mv WITH SYNC MODE;
SELECT label,payload,ordered,js,smalls FROM recursive_mv;

-- query 7
-- @skip_result_check=true
-- @result_contains=RECURSIVE_MV_OBSERVED
shell: set -eu
uea_workspace="${NOVAROCKS_WORKSPACE_ROOT:-.}"
export NOVA_ENV_REST_ENV_FILE="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "${NOVA_ENV_REST_ENV_FILE:-$uea_workspace/docker/iceberg-rest/runtime/current/env.sh}")"
uea_receipts="$uea_workspace/reports/uea7b3/recursive-schema/${uuid0}"
mkdir -p "$uea_receipts"
uea_scala="$uea_receipts/incremental-target.scala"
uea_log="$uea_receipts/incremental-target.log"
cat "$uea_workspace/tests/sql/fixtures/iceberg-delete-applicability/generate.scala" "$uea_workspace/tests/sql/fixtures/mv-visible-content-encodings/fixture.scala" > "$uea_scala"
cat >> "$uea_scala" <<'SCALA'
try {
  DeleteApplicabilityFixture.initialize(org.apache.spark.sql.SparkSession.active, "ns_${uuid0}", "recursive_types")
  RecursiveTypeFixture.observe("ns_${uuid0}","incremental")
} catch { case failure: Throwable => failure.printStackTrace(); System.exit(1) }
SCALA
if ! "$uea_workspace/docker/iceberg-rest/spark-shell.sh" "$uea_scala" > "$uea_log" 2>&1; then tail -80 "$uea_log" >&2; exit 1; fi
grep -q '^RECURSIVE_MV_OBSERVED$' "$uea_log" || { tail -80 "$uea_log" >&2; exit 1; }
grep '^UEA4G_RECEIPT ' "$uea_log" > "$uea_receipts/incremental-target.jsonl"
printf 'RECURSIVE_MV_OBSERVED\n'

-- query 8
-- @order_sensitive=false
-- @imv_equivalence_check=recursive_mv
REFRESH MATERIALIZED VIEW recursive_mv FULL WITH SYNC MODE;
SELECT label,payload,ordered,js,smalls FROM recursive_mv;

-- query 9
-- @skip_result_check=true
-- @result_contains=RECURSIVE_MV_OBSERVED
shell: set -eu
uea_workspace="${NOVAROCKS_WORKSPACE_ROOT:-.}"
export NOVA_ENV_REST_ENV_FILE="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "${NOVA_ENV_REST_ENV_FILE:-$uea_workspace/docker/iceberg-rest/runtime/current/env.sh}")"
uea_receipts="$uea_workspace/reports/uea7b3/recursive-schema/${uuid0}"
mkdir -p "$uea_receipts"
uea_scala="$uea_receipts/full-target.scala"
uea_log="$uea_receipts/full-target.log"
cat "$uea_workspace/tests/sql/fixtures/iceberg-delete-applicability/generate.scala" "$uea_workspace/tests/sql/fixtures/mv-visible-content-encodings/fixture.scala" > "$uea_scala"
cat >> "$uea_scala" <<'SCALA'
try {
  DeleteApplicabilityFixture.initialize(org.apache.spark.sql.SparkSession.active, "ns_${uuid0}", "recursive_types")
  RecursiveTypeFixture.observe("ns_${uuid0}","full")
} catch { case failure: Throwable => failure.printStackTrace(); System.exit(1) }
SCALA
if ! "$uea_workspace/docker/iceberg-rest/spark-shell.sh" "$uea_scala" > "$uea_log" 2>&1; then tail -80 "$uea_log" >&2; exit 1; fi
grep -q '^RECURSIVE_MV_OBSERVED$' "$uea_log" || { tail -80 "$uea_log" >&2; exit 1; }
grep '^UEA4G_RECEIPT ' "$uea_log" > "$uea_receipts/full-target.jsonl"
printf 'RECURSIVE_MV_OBSERVED\n'

-- query 10
-- @cleanup=true
-- @skip_result_check=true
DROP MATERIALIZED VIEW IF EXISTS ns_${uuid0}.recursive_mv;
DROP TABLE IF EXISTS recursive_encodings_${uuid0}.ns_${uuid0}.recursive_source FORCE;
DROP DATABASE recursive_encodings_${uuid0}.ns_${uuid0};
DROP CATALOG recursive_encodings_${uuid0};
