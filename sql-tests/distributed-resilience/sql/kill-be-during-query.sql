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
CREATE TABLE ${case_db}.resilience_series (
  id BIGINT
)
TBLPROPERTIES ("format-version" = "3");
INSERT INTO ${case_db}.resilience_series
SELECT generate_series FROM TABLE(generate_series(1, 333333));
INSERT INTO ${case_db}.resilience_series
SELECT generate_series FROM TABLE(generate_series(333334, 666666));
INSERT INTO ${case_db}.resilience_series
SELECT generate_series FROM TABLE(generate_series(666667, 1000000));

-- query 2
-- @query_control_fragment_backend_limit=2
-- @kill_be_after_fragment_start=1
-- @expect_error=backend 1 lost after heartbeat timeout
SELECT COUNT(*) FROM ${case_db}.resilience_series;

-- query 3
-- @heartbeat_delay_ms=3000
-- @result_contains=1000000
SELECT COUNT(*) FROM TABLE(generate_series(1, 1000000));

-- query 4
-- @kill_be_index=1
-- @restart_be_delay_ms=0
-- @heartbeat_delay_ms=3000
-- @skip_result_check=true
SELECT 1;
