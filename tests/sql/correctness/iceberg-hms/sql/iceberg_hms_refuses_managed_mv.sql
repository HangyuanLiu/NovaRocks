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
-- @tags=mv,iceberg,hive,fail_closed
-- Test Point: native Hive Metastore admits ordinary tables but refuses a
--   document-managed MV before creating its target.
-- Method: create a Hive catalog and an ordinary base table, then attempt a
--   managed MV in the same namespace. Verify that the target is absent and
--   that the base remains writable and readable.
-- Scope: Hive metadata-location checking does not provide the atomic
--   conditional commit required for managed MV documents and publications.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_hms_mv_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hive",
  "iceberg.catalog.hive.metastore.uris" = "${iceberg_hms_uris}",
  "iceberg.catalog.warehouse" = "${iceberg_hms_warehouse}/managed_mv_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "credential.object-store-metadata.consumer-role" = "frontend",
  "credential.object-store-metadata.mode" = "static",
  "credential.object-store-metadata.name" = "${iceberg_object_store_credential_name}",
  "credential.object-store-metadata.generation" = "${iceberg_object_store_credential_generation}",
  "credential.object-store-data.consumer-role" = "backend",
  "credential.object-store-data.mode" = "static",
  "credential.object-store-data.name" = "${iceberg_object_store_credential_name}",
  "credential.object-store-data.generation" = "${iceberg_object_store_credential_generation}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE ice_hms_mv_${uuid0}.ns_${uuid0};
CREATE TABLE ice_hms_mv_${uuid0}.ns_${uuid0}.orders (
  region STRING,
  amount BIGINT
)
TBLPROPERTIES ("format-version" = "3",
  "write.row-lineage" = "true");

-- query 2
-- @expect_error=application-document management requires an Iceberg REST catalog
SET CATALOG ice_hms_mv_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW agg_mv_${uuid0}
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT region, SUM(amount) AS s
FROM ice_hms_mv_${uuid0}.ns_${uuid0}.orders
GROUP BY region;

-- query 3
-- @skip_result_check=true
INSERT INTO ice_hms_mv_${uuid0}.ns_${uuid0}.orders VALUES ('east', 10);

-- query 4
SELECT region, amount FROM ice_hms_mv_${uuid0}.ns_${uuid0}.orders ORDER BY region;

-- query 5
SELECT table_name
FROM information_schema.materialized_views
WHERE table_schema = 'ns_${uuid0}' AND table_name = 'agg_mv_${uuid0}';

-- query 6
-- @cleanup=true
-- @skip_result_check=true
DROP TABLE ice_hms_mv_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE ice_hms_mv_${uuid0}.ns_${uuid0};
DROP CATALOG ice_hms_mv_${uuid0};
