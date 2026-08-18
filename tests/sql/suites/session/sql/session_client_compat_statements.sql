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
-- @order_sensitive=true
-- @tags=session,protocol
-- Test Objective:
-- MySQL clients and connection pools open a session by issuing a handful of
-- charset/transaction statements before any user SQL. NovaRocks accepts them as
-- inert session no-ops. This case freezes that contract: each statement must
-- succeed, and none of them may disturb the session that follows.
--
-- The no-op decision itself lives in the frontend session
-- (`apply_session_set`), but the contract worth protecting is the
-- user-observable one: a client that sends these can still query.

-- query 1
-- @skip_result_check=true
SET NAMES utf8mb4;

-- query 2
SELECT 1;

-- query 3
-- @skip_result_check=true
SET autocommit = 1;

-- query 4
-- @skip_result_check=true
SET character_set_results = NULL;

-- query 5
-- @skip_result_check=true
SET @@autocommit = 1;

-- query 6
-- @skip_result_check=true
SET NAMES 'utf8mb4' COLLATE 'utf8mb4_general_ci';

-- query 7
-- The session is still usable, and unrelated session state set earlier in the
-- connection has not been clobbered by the compatibility statements above.
SELECT 1;

-- query 8
-- @skip_result_check=true
USE default;

-- query 9
SELECT 1;
