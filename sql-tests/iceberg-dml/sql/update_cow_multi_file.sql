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
-- Test Point: one v3 COW UPDATE activates one sealed rewrite cohort per old
--             data file while committing the aggregate as one snapshot.
-- Method: separate INSERT statements deliberately produce two admitted old
--         files. One UPDATE matches a row in each file. A correct COW plan
--         therefore has two rewrite cohorts but only one terminal commit.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_v3_update_cow_multi_file FORCE;
CREATE TABLE ${case_db}.t_v3_update_cow_multi_file (
  id BIGINT,
  v STRING
)
TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
INSERT INTO ${case_db}.t_v3_update_cow_multi_file VALUES (1, 'a');
INSERT INTO ${case_db}.t_v3_update_cow_multi_file VALUES (2, 'b');

-- query 2
SELECT count(*) AS snaps_before
FROM ${case_db}.t_v3_update_cow_multi_file$snapshots;

-- query 3
-- @skip_result_check=true
UPDATE ${case_db}.t_v3_update_cow_multi_file
SET v = CONCAT(v, '_updated')
WHERE id IN (1, 2);

-- query 4
SELECT count(*) AS snaps_after
FROM ${case_db}.t_v3_update_cow_multi_file$snapshots;

-- query 5
SELECT id, v
FROM ${case_db}.t_v3_update_cow_multi_file
ORDER BY id;

-- query 6
SELECT COUNT(DISTINCT _row_id) AS distinct_row_ids, COUNT(*) AS total_rows
FROM ${case_db}.t_v3_update_cow_multi_file;

-- query 7
-- @skip_result_check=true
DROP TABLE ${case_db}.t_v3_update_cow_multi_file FORCE;
