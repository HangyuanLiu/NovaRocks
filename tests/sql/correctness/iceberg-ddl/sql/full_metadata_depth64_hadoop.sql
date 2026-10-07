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
-- @tags=iceberg,metadata_depth,schema_history
-- This is an actual plain-table catalog/file-decoder ingress test, not a
-- managed-MV or independent same-table Spark/SDK oracle. The 63 Struct containers
-- plus INT leaf have semantic depth64; after DROP, the tagged data snapshot
-- still requires that exact retained schema in full TableMetadata. NULL parent
-- rows preserve ordinary writes without manufacturing hidden-child evidence.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG hadoopmetadata64_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_test_warehouse}/metadata64_hadoop_${uuid0}",
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
CREATE DATABASE hadoopmetadata64_${uuid0}.ns_${uuid0};
SET CATALOG hadoopmetadata64_${uuid0};
USE ns_${uuid0};

-- query 2
-- @skip_result_check=true
CREATE TABLE metadata_depth_native (id BIGINT,deep STRUCT<n1 STRUCT<n2 STRUCT<n3 STRUCT<n4 STRUCT<n5 STRUCT<n6 STRUCT<n7 STRUCT<n8 STRUCT<n9 STRUCT<n10 STRUCT<n11 STRUCT<n12 STRUCT<n13 STRUCT<n14 STRUCT<n15 STRUCT<n16 STRUCT<n17 STRUCT<n18 STRUCT<n19 STRUCT<n20 STRUCT<n21 STRUCT<n22 STRUCT<n23 STRUCT<n24 STRUCT<n25 STRUCT<n26 STRUCT<n27 STRUCT<n28 STRUCT<n29 STRUCT<n30 STRUCT<n31 STRUCT<n32 STRUCT<n33 STRUCT<n34 STRUCT<n35 STRUCT<n36 STRUCT<n37 STRUCT<n38 STRUCT<n39 STRUCT<n40 STRUCT<n41 STRUCT<n42 STRUCT<n43 STRUCT<n44 STRUCT<n45 STRUCT<n46 STRUCT<n47 STRUCT<n48 STRUCT<n49 STRUCT<n50 STRUCT<n51 STRUCT<n52 STRUCT<n53 STRUCT<n54 STRUCT<n55 STRUCT<n56 STRUCT<n57 STRUCT<n58 STRUCT<n59 STRUCT<n60 STRUCT<n61 STRUCT<n62 STRUCT<n63 INT>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>)
TBLPROPERTIES ("format-version"="3","write.row-lineage"="true");

-- query 3
-- @skip_result_check=true
INSERT INTO metadata_depth_native(id) VALUES (1),(1),(2);

-- query 4
SELECT id,deep FROM metadata_depth_native ORDER BY id;

-- query 5
SELECT typeof(deep.n1.n2.n3.n4.n5.n6.n7.n8.n9.n10.n11.n12.n13.n14.n15.n16.n17.n18.n19.n20.n21.n22.n23.n24.n25.n26.n27.n28.n29.n30.n31.n32.n33.n34.n35.n36.n37.n38.n39.n40.n41.n42.n43.n44.n45.n46.n47.n48.n49.n50.n51.n52.n53.n54.n55.n56.n57.n58.n59.n60.n61.n62.n63) AS leaf_type,COUNT(*) AS row_count FROM metadata_depth_native GROUP BY 1;

-- query 6
-- @skip_result_check=true
ALTER TABLE metadata_depth_native CREATE TAG depth_before_drop;
ALTER TABLE metadata_depth_native DROP COLUMN deep;
INSERT INTO metadata_depth_native(id) VALUES (3);

-- query 7
SELECT id FROM metadata_depth_native ORDER BY id;

-- query 8
SELECT id FROM metadata_depth_native FOR VERSION AS OF 'depth_before_drop' ORDER BY id;

-- query 9
-- @cleanup=true
-- @skip_result_check=true
DROP TABLE IF EXISTS hadoopmetadata64_${uuid0}.ns_${uuid0}.metadata_depth_native FORCE;
DROP DATABASE hadoopmetadata64_${uuid0}.ns_${uuid0};
DROP CATALOG hadoopmetadata64_${uuid0};
