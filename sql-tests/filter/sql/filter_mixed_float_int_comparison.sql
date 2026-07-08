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
-- @tags=filter,comparison,numeric
-- Test Objective:
-- 1. Validate mixed float/int comparison for scalar predicates.
-- 2. Prevent regressions where numeric comparison rejects Float64 vs Int32.
-- Test Flow:
-- 1. Build scalar expressions with ABS over integer arithmetic.
-- 2. Compare the result with integer literals.
-- 3. Assert deterministic boolean outputs.
SELECT
  abs(1 - 2) = 0 AS eq_zero,
  abs(1 - 2) = 1 AS eq_one;
