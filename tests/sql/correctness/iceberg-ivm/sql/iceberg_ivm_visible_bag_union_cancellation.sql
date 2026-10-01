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
-- @tags=mv,iceberg,visible_bag,union,cancellation,visible_alias
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
CREATE TABLE bag_${uuid0}.ns_${uuid0}.left_t (id BIGINT NOT NULL, label STRING, amount BIGINT)
TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");
CREATE TABLE bag_${uuid0}.ns_${uuid0}.right_t (id BIGINT NOT NULL, label STRING, amount BIGINT)
TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");
SET CATALOG bag_${uuid0};
USE ns_${uuid0};
SET enable_materialized_view_rewrite = false;

-- query 2
-- @skip_result_check=true
INSERT INTO left_t VALUES (1,'shared',9),(2,'shared',9),(3,'left',4);
INSERT INTO right_t VALUES (10,'shared',9),(11,'right',5);

-- query 3
-- @skip_result_check=true
CREATE MATERIALIZED VIEW union_mv
DISTRIBUTED BY HASH(__branch_id__) BUCKETS 3
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT label AS __branch_id__, amount FROM left_t UNION ALL SELECT label AS __branch_id__, amount FROM right_t;
REFRESH MATERIALIZED VIEW union_mv;

-- query 4
-- @skip_result_check=true
-- @imv_equivalence_check=union_mv
SELECT * FROM union_mv;

-- query 5
-- Two withdrawals and two insertions cancel across branches without a persisted branch identity.
-- @skip_result_check=true
DELETE FROM left_t WHERE id IN (1,2);
INSERT INTO right_t VALUES (12,'shared',9),(13,'shared',9);
REFRESH MATERIALIZED VIEW union_mv;

-- query 6
-- @skip_result_check=true
-- @imv_equivalence_check=union_mv
SELECT * FROM union_mv;

-- query 7
-- @skip_result_check=true
-- @result_contains=UNION_CANCELLATION_OK
-- @result_not_contains=UNION_CANCELLATION_FAIL
SELECT IF((SELECT COUNT(*) FROM union_mv WHERE __branch_id__ = 'shared' AND amount = 9) = 3 AND (SELECT COUNT(*) FROM union_mv) = 5, 'UNION_CANCELLATION_OK', 'UNION_CANCELLATION_FAIL') AS status;

-- query 8
-- @skip_result_check=true
UPDATE left_t SET label = 'moved' WHERE id = 3;
DELETE FROM right_t WHERE id = 10;
REFRESH MATERIALIZED VIEW union_mv;

-- query 9
-- @skip_result_check=true
-- @imv_equivalence_check=union_mv
SELECT * FROM union_mv;

-- query 10
-- @skip_result_check=true
-- @result_contains=UNION_AFTER_RETRACTION_OK
-- @result_not_contains=UNION_AFTER_RETRACTION_FAIL
SELECT IF((SELECT COUNT(*) FROM union_mv WHERE __branch_id__ = 'shared') = 2 AND (SELECT COUNT(*) FROM union_mv WHERE __branch_id__ = 'moved') = 1, 'UNION_AFTER_RETRACTION_OK', 'UNION_AFTER_RETRACTION_FAIL') AS status;

-- query 11
-- @cleanup=true
-- @skip_result_check=true
DROP MATERIALIZED VIEW IF EXISTS union_mv;
DROP TABLE IF EXISTS bag_${uuid0}.ns_${uuid0}.left_t FORCE;
DROP TABLE IF EXISTS bag_${uuid0}.ns_${uuid0}.right_t FORCE;
DROP DATABASE bag_${uuid0}.ns_${uuid0};
DROP CATALOG bag_${uuid0};
