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
-- @tags=filter,comparison,child_type
-- Test Objective:
-- 1. Validate BINARY_PRED child type coercion from FE plan metadata.
-- 2. Prevent regressions where mixed numeric/bool operands fail without explicit casts.
-- Test Flow:
-- 1. Build scalar predicates with mixed operand types.
-- 2. Rely on FE-inferred comparison child type for coercion.
-- 3. Assert deterministic boolean outputs.
SELECT
  abs(1 - 2) = 0 AS abs_eq_zero,
  true = 1 AS bool_eq_one,
  false < 1 AS bool_lt_one,
  cast(1.25 as double) > 1 AS dbl_gt_int,
  cast(1 as decimal(10,2)) = 1 AS dec_eq_int;
