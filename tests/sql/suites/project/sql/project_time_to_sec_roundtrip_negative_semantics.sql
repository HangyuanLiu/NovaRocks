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
-- @tags=project,date,time,roundtrip
-- Test Objective:
-- 1. Validate TIME_TO_SEC(SEC_TO_TIME(x)) semantics for negative values.
-- 2. Ensure sec_to_time saturation is preserved by time_to_sec roundtrip.
-- 3. Keep negative time literals strict (still NULL for direct string literal).
-- Test Flow:
-- 1. Evaluate sec_to_time on negative and overflow-negative inputs.
-- 2. Apply time_to_sec on sec_to_time outputs and compare with expected seconds.
-- 3. Validate direct negative literal remains NULL.
SELECT
  SEC_TO_TIME(-1) AS s_neg1,
  TIME_TO_SEC(SEC_TO_TIME(-1)) AS rt_neg1,
  SEC_TO_TIME(-2147483648) AS s_floor_cap,
  TIME_TO_SEC(SEC_TO_TIME(-2147483648)) AS rt_floor_cap,
  TIME_TO_SEC('-00:00:01') AS literal_neg;
