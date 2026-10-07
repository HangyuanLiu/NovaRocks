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
-- @tags=mv,iceberg,visible_bag,duplicates,multiplicity,partitions
-- UEA-7B3: native 1FE+3BE acceptance input; no golden is recorded before verification.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG bag_${uuid0}
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
CREATE DATABASE bag_${uuid0}.ns_${uuid0};
CREATE TABLE bag_${uuid0}.ns_${uuid0}.fact (id BIGINT NOT NULL, part INT, label STRING, amount BIGINT)
PARTITION BY (part)
TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");
SET CATALOG bag_${uuid0};
USE ns_${uuid0};
SET enable_materialized_view_rewrite = false;

-- query 2
-- Separate commits and source partitions carry indistinguishable visible outputs. Native receipts must verify Task placement.
-- @skip_result_check=true
INSERT INTO fact VALUES (1,1,'same',7),(2,2,'same',7);
INSERT INTO fact VALUES (3,3,'same',7),(4,1,'same',7);
INSERT INTO fact VALUES (5,2,'same',7),(6,3,'same',7);

-- query 3
-- @skip_result_check=true
CREATE MATERIALIZED VIEW duplicates_mv
DISTRIBUTED BY HASH(label) BUCKETS 3
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT label, amount FROM fact;
REFRESH MATERIALIZED VIEW duplicates_mv;

-- query 4
-- @skip_result_check=true
-- @imv_equivalence_check=duplicates_mv
SELECT * FROM duplicates_mv;

-- query 5
-- @skip_result_check=true
-- @result_contains=BAG_INITIAL_SIX_OK
-- @result_not_contains=BAG_INITIAL_SIX_FAIL
SELECT IF((SELECT COUNT(*) FROM duplicates_mv) = 6, 'BAG_INITIAL_SIX_OK', 'BAG_INITIAL_SIX_FAIL') AS status;

-- query 6
-- Pure append must not read the target bag.
-- @skip_result_check=true
INSERT INTO fact VALUES (7,1,'same',7);

-- query 7
-- @skip_result_check=true
-- @result_not_contains=QUOTA PRECLAIM
-- @result_not_contains=QUOTA TRIM
-- @result_not_contains=IcebergMvTargetBag
EXPLAIN VERBOSE REFRESH MATERIALIZED VIEW duplicates_mv;

-- query 8
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW duplicates_mv;
DELETE FROM fact WHERE id = 1;
REFRESH MATERIALIZED VIEW duplicates_mv;

-- query 9
-- @skip_result_check=true
-- @imv_equivalence_check=duplicates_mv
SELECT * FROM duplicates_mv;

-- query 10
-- @skip_result_check=true
-- @result_contains=BAG_RETRACT_ONE_OK
-- @result_not_contains=BAG_RETRACT_ONE_FAIL
SELECT IF((SELECT COUNT(*) FROM duplicates_mv) = 6, 'BAG_RETRACT_ONE_OK', 'BAG_RETRACT_ONE_FAIL') AS status;

-- query 11
-- @skip_result_check=true
DELETE FROM fact WHERE id IN (2,3,4);
REFRESH MATERIALIZED VIEW duplicates_mv;

-- query 12
-- @skip_result_check=true
-- @imv_equivalence_check=duplicates_mv
SELECT * FROM duplicates_mv;

-- query 13
-- @skip_result_check=true
-- @result_contains=BAG_RETRACT_N_OK
-- @result_not_contains=BAG_RETRACT_N_FAIL
SELECT IF((SELECT COUNT(*) FROM duplicates_mv) = 3, 'BAG_RETRACT_N_OK', 'BAG_RETRACT_N_FAIL') AS status;

-- query 14
-- @skip_result_check=true
DELETE FROM fact WHERE id > 0;
REFRESH MATERIALIZED VIEW duplicates_mv;

-- query 15
-- @skip_result_check=true
-- @imv_equivalence_check=duplicates_mv
SELECT * FROM duplicates_mv;

-- query 16
-- @skip_result_check=true
-- @result_contains=BAG_RETRACT_ALL_OK
-- @result_not_contains=BAG_RETRACT_ALL_FAIL
SELECT IF((SELECT COUNT(*) FROM duplicates_mv) = 0, 'BAG_RETRACT_ALL_OK', 'BAG_RETRACT_ALL_FAIL') AS status;

-- query 17
-- @cleanup=true
-- @skip_result_check=true
DROP MATERIALIZED VIEW IF EXISTS duplicates_mv;
DROP TABLE IF EXISTS bag_${uuid0}.ns_${uuid0}.fact FORCE;
DROP DATABASE bag_${uuid0}.ns_${uuid0};
DROP CATALOG bag_${uuid0};
