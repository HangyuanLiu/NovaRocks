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
uea_publication="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "${NOVA_ENV_REST_ENV_FILE:-$uea_workspace/docker/iceberg-rest/runtime/current/env.sh}")"
uea_receipts="$uea_workspace/reports/uea7b3/field-domain-invalid/${uuid0}"
python3 "$uea_workspace/tests/sql/fixtures/uea7b3-field-domain-invalid/lifecycle.py" initialize \
  --workspace "$uea_workspace" --publication "$uea_publication" \
  --directory "$uea_receipts" --namespace "ns_${uuid0}"

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
uea_publication="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "${NOVA_ENV_REST_ENV_FILE:-$uea_workspace/docker/iceberg-rest/runtime/current/env.sh}")"
uea_receipts="$uea_workspace/reports/uea7b3/field-domain-invalid/${uuid0}"
python3 "$uea_workspace/tests/sql/fixtures/uea7b3-field-domain-invalid/lifecycle.py" observe \
  --workspace "$uea_workspace" --publication "$uea_publication" \
  --directory "$uea_receipts" --namespace "ns_${uuid0}"

-- query 28
-- @skip_result_check=true
-- @cleanup=true
-- @result_contains=FIELD_DOMAIN_INVALID_CLEANED
shell: set -eu
uea_workspace="${NOVAROCKS_WORKSPACE_ROOT:-.}"
uea_publication="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "${NOVA_ENV_REST_ENV_FILE:-$uea_workspace/docker/iceberg-rest/runtime/current/env.sh}")"
uea_receipts="$uea_workspace/reports/uea7b3/field-domain-invalid/${uuid0}"
python3 "$uea_workspace/tests/sql/fixtures/uea7b3-field-domain-invalid/lifecycle.py" cleanup \
  --workspace "$uea_workspace" --publication "$uea_publication" \
  --directory "$uea_receipts" --namespace "ns_${uuid0}"

-- query 29
-- @cleanup=true
-- @skip_result_check=true
DROP DATABASE invalid_domains_${uuid0}.ns_${uuid0};
DROP CATALOG invalid_domains_${uuid0};
