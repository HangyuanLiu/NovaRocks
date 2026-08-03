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
-- A cross-process FE durability smoke: cleanup uses the FE StateStore, has no
-- BE fragment path, and remains terminal across an FE restart.  Candidate and
-- fault-matrix coverage lives in the provider and runner harness tests.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG cleanup_ice_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_test_warehouse}/cleanup_ice_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE cleanup_ice_${uuid0}.ns_${uuid0};
CREATE TABLE cleanup_ice_${uuid0}.ns_${uuid0}.orders (id INT)
TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");
INSERT INTO cleanup_ice_${uuid0}.ns_${uuid0}.orders VALUES (1), (2);

-- query 2
-- @db=cleanup_ice_${uuid0}.ns_${uuid0}
-- @skip_result_check=true
-- @restart_fe_after_step=true
-- @be_log_not_contains=NOVAROCKS_QUERY_INIT_APPLIED
-- @be_log_not_contains=NOVAROCKS_QUERY_FRAGMENT_ACCEPTED
-- @be_log_not_contains=NOVAROCKS_EXCHANGE_INGRESS
-- @be_log_not_contains=NOVAROCKS_EXCHANGE_EGRESS
-- @be_log_not_contains=NOVAROCKS_CONNECTOR_WRITER_OPENED
-- @be_log_not_contains=NOVAROCKS_CLEANUP_RPC
-- @be_log_not_contains=NOVAROCKS_CLEANUP_WORKER
CALL cleanup_ice_${uuid0}.system.remove_orphan_files(
  table => 'ns_${uuid0}.orders',
  older_than => TIMESTAMP '2099-01-01 00:00:00'
);

-- query 3
-- @db=cleanup_ice_${uuid0}.ns_${uuid0}
-- @be_log_not_contains=NOVAROCKS_QUERY_INIT_APPLIED
-- @be_log_not_contains=NOVAROCKS_QUERY_FRAGMENT_ACCEPTED
-- @be_log_not_contains=NOVAROCKS_EXCHANGE_INGRESS
-- @be_log_not_contains=NOVAROCKS_EXCHANGE_EGRESS
-- @be_log_not_contains=NOVAROCKS_CONNECTOR_WRITER_OPENED
-- @be_log_not_contains=NOVAROCKS_CLEANUP_RPC
-- @be_log_not_contains=NOVAROCKS_CLEANUP_WORKER
SELECT COUNT(*) AS n FROM orders;

-- query 4
-- @skip_result_check=true
DROP TABLE cleanup_ice_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE cleanup_ice_${uuid0}.ns_${uuid0};
DROP CATALOG cleanup_ice_${uuid0};
