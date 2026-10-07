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
-- @tags=mv,iceberg,visible_bag,float,signed_zero,union
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
CREATE TABLE bag_${uuid0}.ns_${uuid0}.positive_t (id BIGINT NOT NULL, x DOUBLE)
TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");
CREATE TABLE bag_${uuid0}.ns_${uuid0}.negative_t (id BIGINT NOT NULL, x DOUBLE)
TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");
SET CATALOG bag_${uuid0};
USE ns_${uuid0};
SET enable_materialized_view_rewrite = false;

-- query 2
-- @skip_result_check=true
INSERT INTO positive_t VALUES (1,CAST('0.0' AS DOUBLE));
INSERT INTO negative_t VALUES (0,CAST('0.0' AS DOUBLE));

-- query 3
-- @skip_result_check=true
CREATE MATERIALIZED VIEW zero_mv
DISTRIBUTED BY HASH(x) BUCKETS 3
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT x FROM positive_t WHERE id > 0 UNION ALL SELECT x FROM negative_t WHERE id > 0;
REFRESH MATERIALIZED VIEW zero_mv;

-- query 4
-- @skip_result_check=true
-- @result_contains=POSITIVE_ZERO_INITIAL_OK
-- @result_not_contains=POSITIVE_ZERO_INITIAL_FAIL
SELECT IF((SELECT COUNT(*) FROM zero_mv WHERE atan2(x, CAST(-1.0 AS DOUBLE)) > 0) = 1 AND (SELECT COUNT(*) FROM zero_mv WHERE atan2(x, CAST(-1.0 AS DOUBLE)) < 0) = 0, 'POSITIVE_ZERO_INITIAL_OK', 'POSITIVE_ZERO_INITIAL_FAIL') AS status;

-- query 5
-- Numeric equality and GROUP BY are insufficient: atan2 observes the stored sign after write/read.
-- @skip_result_check=true
DELETE FROM positive_t WHERE id = 1;
INSERT INTO negative_t VALUES (2,CAST('-0.0' AS DOUBLE)),(3,CAST('-0.0' AS DOUBLE));
REFRESH MATERIALIZED VIEW zero_mv;

-- query 6
-- @skip_result_check=true
-- @result_contains=SIGNED_ZERO_REPLACEMENT_OK
-- @result_not_contains=SIGNED_ZERO_REPLACEMENT_FAIL
SELECT IF((SELECT COUNT(*) FROM zero_mv WHERE atan2(x, CAST(-1.0 AS DOUBLE)) > 0) = 0 AND (SELECT COUNT(*) FROM zero_mv WHERE atan2(x, CAST(-1.0 AS DOUBLE)) < 0) = 2, 'SIGNED_ZERO_REPLACEMENT_OK', 'SIGNED_ZERO_REPLACEMENT_FAIL') AS status;

-- query 7
-- @skip_result_check=true
-- @result_contains=negative-zero
-- @result_not_contains=positive-zero
SELECT IF(atan2(x, CAST(-1.0 AS DOUBLE)) < 0, 'negative-zero', 'positive-zero') AS content_sign FROM zero_mv ORDER BY content_sign;

-- query 8
-- @skip_result_check=true
DELETE FROM negative_t WHERE id = 2;
INSERT INTO positive_t VALUES (4,CAST('0.0' AS DOUBLE));
REFRESH MATERIALIZED VIEW zero_mv;

-- query 9
-- @skip_result_check=true
-- @result_contains=SIGNED_ZERO_MIXED_OK
-- @result_not_contains=SIGNED_ZERO_MIXED_FAIL
SELECT IF((SELECT COUNT(*) FROM zero_mv WHERE atan2(x, CAST(-1.0 AS DOUBLE)) > 0) = 1 AND (SELECT COUNT(*) FROM zero_mv WHERE atan2(x, CAST(-1.0 AS DOUBLE)) < 0) = 1, 'SIGNED_ZERO_MIXED_OK', 'SIGNED_ZERO_MIXED_FAIL') AS status;

-- query 10
-- @skip_result_check=true
-- @result_contains=SIGNED_ZERO_SOURCE_ORACLE_OK
-- @result_not_contains=SIGNED_ZERO_SOURCE_ORACLE_FAIL
SELECT IF((SELECT COUNT(*) FROM positive_t WHERE atan2(x, CAST(-1.0 AS DOUBLE)) > 0) = 1 AND (SELECT COUNT(*) FROM negative_t WHERE atan2(x, CAST(-1.0 AS DOUBLE)) < 0) = 1, 'SIGNED_ZERO_SOURCE_ORACLE_OK', 'SIGNED_ZERO_SOURCE_ORACLE_FAIL') AS status;

-- query 11
-- @cleanup=true
-- @skip_result_check=true
DROP MATERIALIZED VIEW IF EXISTS zero_mv;
DROP TABLE IF EXISTS bag_${uuid0}.ns_${uuid0}.positive_t FORCE;
DROP TABLE IF EXISTS bag_${uuid0}.ns_${uuid0}.negative_t FORCE;
DROP DATABASE bag_${uuid0}.ns_${uuid0};
DROP CATALOG bag_${uuid0};
