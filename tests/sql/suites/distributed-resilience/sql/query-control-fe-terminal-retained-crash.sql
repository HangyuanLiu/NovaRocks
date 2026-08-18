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
CREATE TABLE ${case_db}.fe_terminal_retained (
  id BIGINT,
  payload BIGINT
)
TBLPROPERTIES ("format-version" = "3");

-- query 2
-- @skip_result_check=true
INSERT INTO ${case_db}.fe_terminal_retained VALUES (1, 10);
INSERT INTO ${case_db}.fe_terminal_retained VALUES (2, 20);
INSERT INTO ${case_db}.fe_terminal_retained VALUES (3, 30);

-- query 3
-- The FE is killed only after a participant has frozen a terminal record and
-- released execution resources, but before it can ACK the immutable record.
-- The fault runner restarts FE, then requires every BE to reclaim the short
-- test-retention record before this expected disconnect completes.
-- @kill_fe_at_lifecycle_phase=terminal-retained
-- @query_control_fragment_backend_limit=2
-- @expect_error=server disconnected
-- @be_log_contains=NOVAROCKS_QUERY_TERMINAL_RETAINED
SELECT SUM(left_side.payload) AS total
FROM ${case_db}.fe_terminal_retained left_side
JOIN ${case_db}.fe_terminal_retained right_side
  ON left_side.id = right_side.id;

-- query 4
-- Health query after FE restart and BE retained-record reclamation.
-- @result_contains=3
SELECT COUNT(*) FROM ${case_db}.fe_terminal_retained;
