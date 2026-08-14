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
CREATE TABLE ${case_db}.terminal_p0_admission (id BIGINT)
TBLPROPERTIES ("format-version" = "3");

-- query 2
-- @skip_result_check=true
INSERT INTO ${case_db}.terminal_p0_admission VALUES (1), (2), (3);

-- query 3
-- A retained-record slot must be reserved before ControlReady; failure is
-- therefore pre-admission and cannot create a partial participant set.
-- @query_lifecycle_fault=terminal-p0-retained-slot-exhausted,0
-- @expect_error=before ControlReady
SELECT COUNT(*) FROM ${case_db}.terminal_p0_admission;

-- query 4
-- The P0 byte bound is independently acquired before ControlReady.
-- @query_lifecycle_fault=terminal-p0-bytes-exhausted,1
-- @expect_error=before ControlReady
SELECT COUNT(*) FROM ${case_db}.terminal_p0_admission;

-- query 5
-- The terminal delivery permit is the third independently required P0 item.
-- @query_lifecycle_fault=terminal-p0-delivery-permit-exhausted,2
-- @expect_error=before ControlReady
SELECT COUNT(*) FROM ${case_db}.terminal_p0_admission;

-- query 6
-- A failed pre-ready attach does not poison the following query.
-- @result_contains=3
SELECT COUNT(*) FROM ${case_db}.terminal_p0_admission;
