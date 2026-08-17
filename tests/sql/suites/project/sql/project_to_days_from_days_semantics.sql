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
-- @tags=project,date
-- Test Objective:
-- 1. Validate TO_DAYS/FROM_DAYS round-trip semantics.
-- 2. Validate FROM_DAYS accepts integer day values from BIGINT expressions.
-- Test Flow:
-- 1. Compute TO_DAYS for a fixed date.
-- 2. Apply FROM_DAYS to the computed result and BIGINT input.
-- 3. Assert deterministic scalar outputs.
SELECT
  TO_DAYS(DATE '2024-01-10') AS days_v,
  FROM_DAYS(TO_DAYS(DATE '2024-01-10')) AS roundtrip_v,
  FROM_DAYS(CAST(739260 AS BIGINT)) AS bigint_input_v;
