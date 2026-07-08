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
-- @tags=filter,comparison,decimal,child_type
-- Test Objective:
-- 1. Validate decimal BINARY_PRED with mixed precision/scale operands.
-- 2. Prevent regressions where FE child_type=DECIMAL64 rejects compatible decimal children.
-- Test Flow:
-- 1. Build two decimal rows with scale 2.
-- 2. Compare against decimal literals with different precision/scale using >=, <= and BETWEEN.
-- 3. Assert deterministic boolean outputs in ascending decimal order.
WITH t(v) AS (
  SELECT CAST(120.00 AS DECIMAL(7,2))
  UNION ALL
  SELECT CAST(90.00 AS DECIMAL(7,2))
)
SELECT
  v,
  v >= CAST(100 AS DECIMAL(5,0)) AS ge_lower,
  v <= CAST(150.000 AS DECIMAL(9,3)) AS le_upper,
  v BETWEEN CAST(100.0 AS DECIMAL(6,1)) AND CAST(150 AS DECIMAL(4,0)) AS between_flag
FROM t
ORDER BY v;
