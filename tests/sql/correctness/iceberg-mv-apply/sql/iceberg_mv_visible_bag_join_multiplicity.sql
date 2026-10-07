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
-- @tags=mv,iceberg,visible_bag,join,self_join,hidden_join_key
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
CREATE TABLE bag_${uuid0}.ns_${uuid0}.left_t (id BIGINT NOT NULL, join_key INT, label STRING)
TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");
CREATE TABLE bag_${uuid0}.ns_${uuid0}.right_t (id BIGINT NOT NULL, join_key INT, label STRING)
TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");
CREATE TABLE bag_${uuid0}.ns_${uuid0}.self_t (id BIGINT NOT NULL, join_key INT, label STRING)
TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");
SET CATALOG bag_${uuid0};
USE ns_${uuid0};
SET enable_materialized_view_rewrite = false;

-- query 2
-- @skip_result_check=true
INSERT INTO left_t VALUES (1,7,'same'),(2,7,'same');
INSERT INTO right_t VALUES (10,7,'same'),(11,7,'same'),(12,7,'same');
INSERT INTO self_t VALUES (1,7,'same'),(2,7,'same');

-- query 3
-- @skip_result_check=true
CREATE MATERIALIZED VIEW join_mv
DISTRIBUTED BY HASH(left_label) BUCKETS 3
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT l.label AS left_label, r.label AS right_label FROM left_t l JOIN right_t r ON l.join_key = r.join_key;
REFRESH MATERIALIZED VIEW join_mv;

-- query 4
-- @skip_result_check=true
CREATE MATERIALIZED VIEW self_mv
DISTRIBUTED BY HASH(left_label) BUCKETS 3
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT l.label AS left_label, r.label AS right_label FROM self_t l JOIN self_t r ON l.join_key = r.join_key;
REFRESH MATERIALIZED VIEW self_mv;

-- query 5
-- @skip_result_check=true
-- @imv_equivalence_check=join_mv
SELECT * FROM join_mv;

-- query 6
-- @skip_result_check=true
-- @imv_equivalence_check=self_mv
SELECT * FROM self_mv;

-- query 7
-- @skip_result_check=true
-- @result_contains=JOIN_INITIAL_MULTIPLICITY_OK
-- @result_not_contains=JOIN_INITIAL_MULTIPLICITY_FAIL
SELECT IF((SELECT COUNT(*) FROM join_mv) = 6 AND (SELECT COUNT(*) FROM self_mv) = 4, 'JOIN_INITIAL_MULTIPLICITY_OK', 'JOIN_INITIAL_MULTIPLICITY_FAIL') AS status;

-- query 8
-- Both join sides change. A self-join reads two occurrences of one source with exact telescoping windows.
-- @skip_result_check=true
DELETE FROM left_t WHERE id = 1;
INSERT INTO right_t VALUES (13,7,'same');
DELETE FROM self_t WHERE id = 1;
INSERT INTO self_t VALUES (3,7,'same'),(4,7,'same');
REFRESH MATERIALIZED VIEW join_mv;
REFRESH MATERIALIZED VIEW self_mv;

-- query 9
-- @skip_result_check=true
-- @imv_equivalence_check=join_mv
SELECT * FROM join_mv;

-- query 10
-- @skip_result_check=true
-- @imv_equivalence_check=self_mv
SELECT * FROM self_mv;

-- query 11
-- @skip_result_check=true
-- @result_contains=JOIN_BOTH_SIDE_MULTIPLICITY_OK
-- @result_not_contains=JOIN_BOTH_SIDE_MULTIPLICITY_FAIL
SELECT IF((SELECT COUNT(*) FROM join_mv) = 4 AND (SELECT COUNT(*) FROM self_mv) = 9, 'JOIN_BOTH_SIDE_MULTIPLICITY_OK', 'JOIN_BOTH_SIDE_MULTIPLICITY_FAIL') AS status;

-- query 12
-- @skip_result_check=true
UPDATE right_t SET label = 'changed' WHERE id = 10;
DELETE FROM self_t WHERE id = 2;
REFRESH MATERIALIZED VIEW join_mv;
REFRESH MATERIALIZED VIEW self_mv;

-- query 13
-- @skip_result_check=true
-- @imv_equivalence_check=join_mv
SELECT * FROM join_mv;

-- query 14
-- @skip_result_check=true
-- @imv_equivalence_check=self_mv
SELECT * FROM self_mv;

-- query 15
-- @skip_result_check=true
-- @result_contains=JOIN_CONTENT_REPLACEMENT_OK
-- @result_not_contains=JOIN_CONTENT_REPLACEMENT_FAIL
SELECT IF((SELECT COUNT(*) FROM join_mv WHERE right_label = 'changed') = 1 AND (SELECT COUNT(*) FROM self_mv) = 4, 'JOIN_CONTENT_REPLACEMENT_OK', 'JOIN_CONTENT_REPLACEMENT_FAIL') AS status;

-- query 16
-- @cleanup=true
-- @skip_result_check=true
DROP MATERIALIZED VIEW IF EXISTS join_mv;
DROP MATERIALIZED VIEW IF EXISTS self_mv;
DROP TABLE IF EXISTS bag_${uuid0}.ns_${uuid0}.left_t FORCE;
DROP TABLE IF EXISTS bag_${uuid0}.ns_${uuid0}.right_t FORCE;
DROP TABLE IF EXISTS bag_${uuid0}.ns_${uuid0}.self_t FORCE;
DROP DATABASE bag_${uuid0}.ns_${uuid0};
DROP CATALOG bag_${uuid0};
