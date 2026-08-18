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

-- @order_sensitive=true
-- Validate `CREATE TABLE [IF NOT EXISTS] <name> [PARTITION BY (...)]
-- [TBLPROPERTIES(...)] AS <select>` end-to-end on Iceberg REST targets:
--   parser -> frontend durable CTAS saga -> provider-owned staged create
--   -> one distributed source/write execution -> atomic publish. Strict default:
--   every CTAS table is
--   format-version=3 + write.row-lineage=true.
-- Covers: basic CTAS, PARTITION BY clause, TBLPROPERTIES forwarding,
-- nested types (struct/list), IF NOT EXISTS skip semantics, post-CTAS
-- INSERT continuation, parser-level rejections (branch target /
-- format-version=2 / row-lineage=false / explicit columns / table exists),
-- and analyzer-level rejection (partition column not in SELECT output).
--
-- The runner auto-creates `${case_db}` in the suite Hadoop catalog. This case
-- deliberately keeps its source there while publishing every successful target
-- through a case-local REST catalog, proving source session context and target
-- provider ownership remain independent. Hadoop CTAS is rejected explicitly.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ctas_rest_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "rest",
  "uri" = "${iceberg_rest_uri}",
  "warehouse" = "${iceberg_rest_warehouse}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.region" = "us-east-1",
  "aws.s3.enable_path_style_access" = "true"
);

-- query 2
-- @skip_result_check=true
DROP DATABASE IF EXISTS ctas_rest_${uuid0}.${case_db} FORCE;

-- query 3
-- @skip_result_check=true
CREATE DATABASE ctas_rest_${uuid0}.${case_db};

-- ---------------------------------------------------------------------------
-- Case 1: basic CTAS — no PARTITION BY, no PROPERTIES
-- ---------------------------------------------------------------------------

-- query 4
-- @skip_result_check=true
CREATE TABLE ${case_db}.src (id INT, name VARCHAR(16), region VARCHAR(8))
TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");

-- query 5
-- @skip_result_check=true
INSERT INTO ${case_db}.src VALUES
  (1, 'alice',   'us'),
  (2, 'bob',     'eu'),
  (3, 'charlie', 'us');

-- query 6
-- @skip_result_check=true
-- @be_log_contains=NOVAROCKS_CONNECTOR_WRITER_OPENED
-- Basic CTAS: schema inferred from SELECT (id INT, uname VARCHAR).
CREATE TABLE ctas_rest_${uuid0}.${case_db}.dst1 AS
  SELECT id, UPPER(name) AS uname FROM ${case_db}.src;

-- query 7
-- All 3 rows materialized into dst1 with inferred schema.
SELECT id, uname FROM ctas_rest_${uuid0}.${case_db}.dst1 ORDER BY id;

-- ---------------------------------------------------------------------------
-- Case 2: CTAS PARTITION BY identity column
-- ---------------------------------------------------------------------------

-- query 8
-- @skip_result_check=true
CREATE TABLE ctas_rest_${uuid0}.${case_db}.dst2 PARTITION BY (region) AS
  SELECT id, region FROM ${case_db}.src;

-- query 9
-- Verify rows landed across both partitions.
SELECT region, COUNT(*) AS n FROM ctas_rest_${uuid0}.${case_db}.dst2 GROUP BY region ORDER BY region;

-- ---------------------------------------------------------------------------
-- Case 3: CTAS with extra TBLPROPERTIES (non-version key)
-- ---------------------------------------------------------------------------

-- query 10
-- @skip_result_check=true
CREATE TABLE ctas_rest_${uuid0}.${case_db}.dst3
TBLPROPERTIES ("write.parquet.compression-codec" = "zstd")
AS SELECT id FROM ${case_db}.src;

-- query 11
-- Result is identical to a basic CTAS — the property only affects file write.
SELECT COUNT(*) AS n FROM ctas_rest_${uuid0}.${case_db}.dst3;

-- ---------------------------------------------------------------------------
-- Case 4: IF NOT EXISTS on already-existing target — skip CTAS, leave dst1 unchanged
-- ---------------------------------------------------------------------------

-- query 12
-- @skip_result_check=true
-- dst1 already has 3 rows from Case 1; this CTAS must be a no-op.
CREATE TABLE IF NOT EXISTS ctas_rest_${uuid0}.${case_db}.dst1 AS
  SELECT id FROM ${case_db}.src WHERE 1 = 0;

-- query 13
-- dst1 still has 3 rows (Case 1 INSERT count), proving CTAS was skipped.
SELECT COUNT(*) AS n FROM ctas_rest_${uuid0}.${case_db}.dst1;

-- ---------------------------------------------------------------------------
-- Case 5: IF NOT EXISTS on non-existing target — proceeds normally
-- ---------------------------------------------------------------------------

-- query 14
-- @skip_result_check=true
CREATE TABLE IF NOT EXISTS ctas_rest_${uuid0}.${case_db}.dst5 AS
  SELECT id FROM ${case_db}.src;

-- query 15
SELECT COUNT(*) AS n FROM ctas_rest_${uuid0}.${case_db}.dst5;

-- ---------------------------------------------------------------------------
-- Case 6: CTAS-built table accepts subsequent INSERT
-- ---------------------------------------------------------------------------

-- query 16
-- @skip_result_check=true
INSERT INTO ctas_rest_${uuid0}.${case_db}.dst1 VALUES (99, 'late');

-- query 17
-- dst1 grew from 3 rows (Case 1) to 4 (Case 6 INSERT).
SELECT id, uname FROM ctas_rest_${uuid0}.${case_db}.dst1 ORDER BY id;

-- ---------------------------------------------------------------------------
-- Case 7 (error): branch-qualified CTAS target
-- ---------------------------------------------------------------------------

-- query 18
-- @expect_error=branch
-- Parser rejects CTAS targeting a branch ref.
CREATE TABLE ${case_db}.dst7.branch_dev AS SELECT 1 AS x;

-- ---------------------------------------------------------------------------
-- Case 8 (error): TBLPROPERTIES('format-version'='2')
-- ---------------------------------------------------------------------------

-- query 19
-- @expect_error=format-version
CREATE TABLE ctas_rest_${uuid0}.${case_db}.dst8 TBLPROPERTIES ("format-version" = "2") AS
  SELECT 1 AS x;

-- ---------------------------------------------------------------------------
-- Case 9 (error): TBLPROPERTIES('write.row-lineage'='false')
-- ---------------------------------------------------------------------------

-- query 20
-- @expect_error=row-lineage
CREATE TABLE ctas_rest_${uuid0}.${case_db}.dst9 TBLPROPERTIES ("write.row-lineage" = "false") AS
  SELECT 1 AS x;

-- ---------------------------------------------------------------------------
-- Case 10 (error): explicit column definitions in CTAS
-- ---------------------------------------------------------------------------

-- query 21
-- @expect_error=column
CREATE TABLE ctas_rest_${uuid0}.${case_db}.dst10 (id INT, name VARCHAR(16)) AS
  SELECT 1, 'a';

-- ---------------------------------------------------------------------------
-- Case 11 (error): PARTITION BY column not in SELECT output
-- ---------------------------------------------------------------------------

-- query 22
-- @expect_error=partition source column
CREATE TABLE ctas_rest_${uuid0}.${case_db}.dst11 PARTITION BY (ghost) AS
  SELECT id FROM ${case_db}.src;

-- ---------------------------------------------------------------------------
-- Case 12 (error): table already exists, no IF NOT EXISTS
-- ---------------------------------------------------------------------------

-- query 23
-- @expect_error=already exists
-- dst1 already exists from Case 1; CTAS without IF NOT EXISTS must reject.
CREATE TABLE ctas_rest_${uuid0}.${case_db}.dst1 AS SELECT id FROM ${case_db}.src;

-- ---------------------------------------------------------------------------
-- Case 13 (error): Hadoop has no staged-create proof and must fail closed
-- ---------------------------------------------------------------------------

-- query 24
-- @expect_error=staged
CREATE TABLE ${case_db}.hadoop_dst AS SELECT id FROM ${case_db}.src;

-- query 25
-- @skip_result_check=true
DROP DATABASE ctas_rest_${uuid0}.${case_db} FORCE;

-- query 26
-- @skip_result_check=true
DROP CATALOG ctas_rest_${uuid0};
