-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0.
--
-- @sequential=true
-- @tags=mv,iceberg,rest,minio,storage-contract,document-retention
-- Two real refreshes share D/L while each output owns an exact P. The runner
-- reads the isolated REST table metadata before cleanup, not a mocked graph.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG mvgraph_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "rest",
  "uri" = "${iceberg_rest_uri}",
  "warehouse" = "${iceberg_rest_warehouse}",
  "credential.object-store-metadata.consumer-role" = "frontend",
  "credential.object-store-metadata.mode" = "static",
  "credential.object-store-metadata.name" = "${iceberg_object_store_credential_name}",
  "credential.object-store-metadata.generation" = "${iceberg_object_store_credential_generation}",
  "credential.object-store-data.consumer-role" = "backend",
  "credential.object-store-data.mode" = "static",
  "credential.object-store-data.name" = "${iceberg_object_store_credential_name}",
  "credential.object-store-data.generation" = "${iceberg_object_store_credential_generation}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.region" = "us-east-1",
  "aws.s3.enable_path_style_access" = "true"
);

-- query 2
-- @skip_result_check=true
CREATE DATABASE mvgraph_${uuid0}.ns_${uuid0};

-- query 3
-- @skip_result_check=true
CREATE TABLE mvgraph_${uuid0}.ns_${uuid0}.fact (k STRING, v BIGINT)
TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");

-- query 4
-- @skip_result_check=true
INSERT INTO mvgraph_${uuid0}.ns_${uuid0}.fact VALUES ('east', 10);

-- query 5
-- @skip_result_check=true
-- @mv_rest_document_graph=ns_${uuid0}.mv_graph,publications=0
-- @result_contains=package
-- @result_contains=lake-documents
SET CATALOG mvgraph_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW mv_graph
DISTRIBUTED BY HASH(k) BUCKETS 1
REFRESH DEFERRED MANUAL
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT k, v FROM fact;
CALL mvgraph_${uuid0}.system.novarocks_imv_stateless_rebuild(
  table => 'ns_${uuid0}.mv_graph', level => 'package');

-- query 6
-- @skip_result_check=true
-- @mv_rest_document_graph=ns_${uuid0}.mv_graph,publications=1
-- @result_contains=provenance
-- @result_contains=lake-documents
REFRESH MATERIALIZED VIEW mv_graph WITH SYNC MODE;
CALL mvgraph_${uuid0}.system.novarocks_imv_stateless_rebuild(
  table => 'ns_${uuid0}.mv_graph', level => 'provenance');

-- query 7
-- @skip_result_check=true
INSERT INTO mvgraph_${uuid0}.ns_${uuid0}.fact VALUES ('east', 20);

-- query 8
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW mv_graph WITH SYNC MODE;

-- query 9
-- @mv_rest_document_graph=ns_${uuid0}.mv_graph,publications=2
-- @skip_result_check=true
-- @result_contains=east
-- @result_contains=10
-- @result_contains=20
SELECT k, v FROM mv_graph ORDER BY k, v;

-- query 10
-- @cleanup=true
-- @skip_result_check=true
SET CATALOG mvgraph_${uuid0};
USE ns_${uuid0};
DROP MATERIALIZED VIEW mv_graph;
DROP TABLE mvgraph_${uuid0}.ns_${uuid0}.fact FORCE;
DROP DATABASE mvgraph_${uuid0}.ns_${uuid0};
DROP CATALOG mvgraph_${uuid0};
