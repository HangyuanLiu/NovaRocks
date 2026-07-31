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

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.terminal_conflict (
  id BIGINT,
  payload BIGINT
)
TBLPROPERTIES ("format-version" = "3");

-- query 2
-- @skip_result_check=true
INSERT INTO ${case_db}.terminal_conflict VALUES (1, 10);
INSERT INTO ${case_db}.terminal_conflict VALUES (2, 20);
INSERT INTO ${case_db}.terminal_conflict VALUES (3, 30);

-- query 3
-- Inject a second valid terminal payload with the same execution and backend
-- identity but a different digest before FE ACK. The query must fail closed,
-- never publish a successful terminal set, and clean up normally.
-- @terminal_snapshot_conflict_be_index=0
-- @query_control_fragment_backend_limit=2
-- @expect_error=query terminal snapshot conflicts with an already stored participant snapshot
SELECT SUM(left_side.payload) AS total
FROM ${case_db}.terminal_conflict left_side
JOIN ${case_db}.terminal_conflict right_side
  ON left_side.id = right_side.id;

-- query 4
-- Health query after the rejected terminal identity conflict.
-- @result_contains=3
SELECT COUNT(*) FROM ${case_db}.terminal_conflict;
