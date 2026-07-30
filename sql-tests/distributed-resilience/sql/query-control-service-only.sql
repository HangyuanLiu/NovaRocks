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
CREATE TABLE ${case_db}.service_only (
  id BIGINT,
  payload BIGINT
)
TBLPROPERTIES ("format-version" = "3");

-- query 2
-- @skip_result_check=true
INSERT INTO ${case_db}.service_only VALUES (1, 10);

-- query 3
-- @skip_result_check=true
INSERT INTO ${case_db}.service_only VALUES (2, 20);

-- query 4
-- @skip_result_check=true
INSERT INTO ${case_db}.service_only VALUES (3, 30);

-- query 5
-- @query_control_fragment_backend_limit=2
-- @result_contains=60
-- @be_log_be_count_at_least=NOVAROCKS_QUERY_CONTROL_READY,3
-- @be_log_contains=expected_fragments=0
-- @be_log_be_count_at_least=NOVAROCKS_QUERY_FRAGMENT_ACCEPTED,2
SELECT SUM(left_side.payload) AS total
FROM ${case_db}.service_only left_side
JOIN ${case_db}.service_only right_side
  ON left_side.id = right_side.id;
