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
CREATE TABLE ${case_db}.stage_phase_kill (
  id BIGINT,
  delay_s BIGINT
)
TBLPROPERTIES ("format-version" = "3");
INSERT INTO ${case_db}.stage_phase_kill VALUES (1, 10), (2, 10), (3, 10);

-- query 2
-- @kill_query_at_lifecycle_phase=staging
-- @expect_error=Query execution was interrupted
-- @be_log_be_count_at_least=NOVAROCKS_QUERY_LIFECYCLE_TERMINATED,3
SELECT COUNT(*) FROM ${case_db}.stage_phase_kill WHERE sleep(delay_s);

-- query 3
-- @result_contains=3
SELECT COUNT(*) FROM ${case_db}.stage_phase_kill;

-- query 4
-- @kill_query_at_lifecycle_phase=staged
-- @expect_error=Query execution was interrupted
-- @be_log_be_count_at_least=NOVAROCKS_QUERY_LIFECYCLE_TERMINATED,3
SELECT COUNT(*) FROM ${case_db}.stage_phase_kill WHERE sleep(delay_s);

-- query 5
-- @result_contains=3
SELECT COUNT(*) FROM ${case_db}.stage_phase_kill;

-- query 6
-- @kill_query_at_lifecycle_phase=starting
-- @expect_error=Query execution was interrupted
-- @be_log_be_count_at_least=NOVAROCKS_QUERY_LIFECYCLE_TERMINATED,3
SELECT COUNT(*) FROM ${case_db}.stage_phase_kill WHERE sleep(delay_s);

-- query 7
-- @result_contains=3
SELECT COUNT(*) FROM ${case_db}.stage_phase_kill;

-- query 8
-- @kill_query_at_lifecycle_phase=running
-- @expect_error=Query execution was interrupted
-- @be_log_be_count_at_least=NOVAROCKS_QUERY_LIFECYCLE_TERMINATED,3
SELECT COUNT(*) FROM ${case_db}.stage_phase_kill WHERE sleep(delay_s);

-- query 9
-- @result_contains=3
SELECT COUNT(*) FROM ${case_db}.stage_phase_kill;
