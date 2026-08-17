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
-- @tags=project,date,from_days
-- Test Objective:
-- 1. Validate FROM_DAYS boundary semantics against StarRocks behavior.
-- 2. Prevent regressions on zero-date sentinel and BIGINT overflow handling.
-- Test Flow:
-- 1. Evaluate FROM_DAYS on valid/in-range/out-of-range integer inputs.
-- 2. Evaluate TO_DAYS(FROM_DAYS(...)) for zero-date sentinel roundtrip.
-- 3. Assert deterministic scalar output.
SELECT
  FROM_DAYS(-1) AS fd_neg1,
  FROM_DAYS(0) AS fd_0,
  FROM_DAYS(3652424) AS fd_max,
  FROM_DAYS(3652425) AS fd_over,
  FROM_DAYS(CAST(2147483647 AS BIGINT)) AS fd_i32_max,
  FROM_DAYS(CAST(2147483648 AS BIGINT)) AS fd_i32_over,
  FROM_DAYS(CAST(-2147483648 AS BIGINT)) AS fd_i32_min,
  FROM_DAYS(CAST(-2147483649 AS BIGINT)) AS fd_i32_under,
  TO_DAYS(FROM_DAYS(-1)) AS td_fd_neg1,
  TO_DAYS(FROM_DAYS(3652425)) AS td_fd_over;
