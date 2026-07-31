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
CREATE TABLE ${case_db}.fragment_execution_failure (
  id BIGINT,
  delay_s BIGINT
)
TBLPROPERTIES ("format-version" = "3");

-- query 2
-- @skip_result_check=true
INSERT INTO ${case_db}.fragment_execution_failure VALUES (1, 5);

-- query 3
-- @skip_result_check=true
INSERT INTO ${case_db}.fragment_execution_failure VALUES (2, 5);

-- query 4
-- @skip_result_check=true
INSERT INTO ${case_db}.fragment_execution_failure VALUES (3, 5);

-- query 5
-- @fail_fragment_after_start_be_index=1
-- @expect_error=fragment executor failure injected after start
-- @be_log_exact_fragment_cancellation=3
SELECT COUNT(*)
FROM ${case_db}.fragment_execution_failure
WHERE sleep(delay_s);

-- query 6
-- A destructive local executor failure must leave the following query healthy.
-- @result_contains=3
SELECT COUNT(*) FROM ${case_db}.fragment_execution_failure;
