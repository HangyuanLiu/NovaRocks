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
CREATE TABLE ${case_db}.stage_start (
  id BIGINT,
  payload BIGINT
)
TBLPROPERTIES ("format-version" = "3");

-- query 2
-- @skip_result_check=true
INSERT INTO ${case_db}.stage_start VALUES (1, 10), (2, 20), (3, 30);

-- query 3
-- A normal 1FE+3BE query: every participant reaches ControlReady, prepares
-- its Stage batch, then releases only through StartPreparedQuery.
-- @result_contains=60
-- @be_log_be_count_at_least=NOVAROCKS_QUERY_CONTROL_READY,3
-- @be_log_be_count_at_least=NOVAROCKS_QUERY_FRAGMENT_ACCEPTED,3
SELECT SUM(left_side.payload) AS total
FROM ${case_db}.stage_start left_side
JOIN ${case_db}.stage_start right_side
  ON left_side.id = right_side.id;

-- query 4
-- The first Stage response is lost after the backend committed its bundle;
-- FE retries the same digest and the query must still complete once.
-- @drop_next_stage_ack_be_index=1
-- @result_contains=60
-- @be_log_contains=NOVAROCKS_STAGE_ACK_DROPPED
SELECT SUM(left_side.payload) AS total
FROM ${case_db}.stage_start left_side
JOIN ${case_db}.stage_start right_side
  ON left_side.id = right_side.id;

-- query 5
-- Start response loss is also retry-safe: the retry observes the same gate
-- rather than starting a second worker bundle.
-- @drop_next_start_ack_be_index=1
-- @result_contains=60
-- @be_log_contains=NOVAROCKS_START_ACK_DROPPED
SELECT SUM(left_side.payload) AS total
FROM ${case_db}.stage_start left_side
JOIN ${case_db}.stage_start right_side
  ON left_side.id = right_side.id;

-- query 6
-- Start ACK suppression exercises the same unknown-outcome retry boundary as
-- a dropped response. This is intentionally a success case while the current
-- handler consumes one runner-owned suppression token.
-- @suppress_start_ack_be_index=2
-- @result_contains=60
-- @be_log_contains=NOVAROCKS_START_ACK_SUPPRESSED
SELECT SUM(left_side.payload) AS total
FROM ${case_db}.stage_start left_side
JOIN ${case_db}.stage_start right_side
  ON left_side.id = right_side.id;

-- query 7
-- Fail the first local Stage prepare. The Stage barrier must reject before
-- any participant can pass StartPreparedQuery.
-- @fail_stage_prepare_ordinal=1
-- @expect_error=StageFragments rejected
-- @be_log_contains=NOVAROCKS_STAGE_PREPARE_FAILED
SELECT SUM(left_side.payload) AS total
FROM ${case_db}.stage_start left_side
JOIN ${case_db}.stage_start right_side
  ON left_side.id = right_side.id;

-- query 8
-- Health query after the failed attempt.
-- @result_contains=3
SELECT COUNT(*) FROM ${case_db}.stage_start;
