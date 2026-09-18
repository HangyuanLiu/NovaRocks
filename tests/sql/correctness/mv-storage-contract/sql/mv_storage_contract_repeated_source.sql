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
-- @tags=mv,iceberg,rest,minio,storage-contract
-- Test Objective:
-- One table, referenced twice, is two sources -- and this locks the one place
-- that still cannot say so.
--
-- The canonical definition records two relation occurrences of the same
-- object, and every source fact belongs to its own occurrence rather than to
-- the table name they share. The refresh contract now agrees: base relations,
-- snapshot pins, predecessor facts and the join's own two sides are all named
-- by occurrence, and the join resolves its sides by the qualifier each was
-- bound under rather than by the table they share.
--
-- The provenance a publication records does not: it names each base by table,
-- so two mentions of one table would arrive there as two entries nothing could
-- tell apart. The refusal below is that gap, stated at CREATE rather than at
-- the refresh, because a view that can be created and never refreshed is worse
-- than one refused while the operator is still writing it.
--
-- When the publication provenance names occurrences, this case turns into the
-- positive one it is written as underneath: the CREATE succeeds, the refresh
-- publishes, and each mention keeps its own pin.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG mvsc_${uuid0}
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
CREATE DATABASE mvsc_${uuid0}.ns_${uuid0};

-- query 3
-- @skip_result_check=true
CREATE TABLE mvsc_${uuid0}.ns_${uuid0}.moves (
  account STRING,
  peer STRING,
  amount BIGINT
) TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");

-- query 4
-- @skip_result_check=true
INSERT INTO mvsc_${uuid0}.ns_${uuid0}.moves VALUES
  ('a', 'b', 10), ('b', 'a', 4), ('a', 'c', 6), ('c', 'a', 1);

-- query 5
-- @skip_result_check=true
SET CATALOG mvsc_${uuid0};

-- query 6
-- @skip_result_check=true
USE ns_${uuid0};

-- query 7
-- Two occurrences of `moves`: outgoing and the incoming it is matched against.
-- @skip_result_check=true
-- @expect_error_tier=drift
-- @expect_error=cannot yet publish a view that reads
CREATE MATERIALIZED VIEW move_balance
DISTRIBUTED BY HASH(account) BUCKETS 1
REFRESH DEFERRED MANUAL
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT out.account AS account, SUM(out.amount) AS sent, SUM(inb.amount) AS received
FROM moves out JOIN moves inb ON out.account = inb.peer
GROUP BY out.account;

-- query 8
-- Nothing was created, so nothing is listed. The refusal is complete: it does
-- not leave a half-made view behind for the next statement to trip over.
-- @skip_result_check=true
-- @result_not_contains=move_balance
SET CATALOG mvsc_${uuid0};
USE ns_${uuid0};
SHOW MATERIALIZED VIEWS FROM ns_${uuid0};

-- query 9
-- @skip_result_check=true
SET CATALOG mvsc_${uuid0};
USE ns_${uuid0};
DROP TABLE mvsc_${uuid0}.ns_${uuid0}.moves FORCE;
DROP DATABASE mvsc_${uuid0}.ns_${uuid0};
-- Dropping the attachment matters: two attachments onto one warehouse make the
-- same physical MV discoverable under two catalog names, and its management
-- binds to whichever attachment discovered it first -- so the next case's
-- status query, which names its own catalog, would find nothing.
DROP CATALOG mvsc_${uuid0};
