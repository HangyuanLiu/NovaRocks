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
-- Test Point: Iceberg v3 copy-on-write UPDATE preserves _row_id
-- Method: insert two rows into a v3 row-lineage table, UPDATE one row's
-- non-partition column, and verify the updated value is visible exactly
-- once and the row's `_row_id` survives.
-- Scope: standalone Iceberg table DDL, INSERT INTO, UPDATE, SELECT, v3
--        row-lineage copy-on-write update sidecar.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_v3_update_cow FORCE;
CREATE TABLE ${case_db}.t_v3_update_cow (
  id BIGINT,
  v STRING
)
TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
INSERT INTO ${case_db}.t_v3_update_cow VALUES
  (1, 'a'),
  (2, 'b');

-- query 2
SELECT id, v
FROM ${case_db}.t_v3_update_cow
ORDER BY id;

-- query 3
-- @skip_result_check=true
UPDATE ${case_db}.t_v3_update_cow AS t SET v = 'bb' WHERE t.id = 2;

-- query 4
SELECT id, v
FROM ${case_db}.t_v3_update_cow
ORDER BY id;

-- query 5
SELECT COUNT(DISTINCT _row_id) AS distinct_row_ids, COUNT(*) AS total_rows
FROM ${case_db}.t_v3_update_cow;

-- query 6
-- @skip_result_check=true
DROP TABLE ${case_db}.t_v3_update_cow FORCE;
