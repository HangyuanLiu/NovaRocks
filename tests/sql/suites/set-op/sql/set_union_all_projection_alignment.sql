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
-- @tags=set_op,union_all,projection
-- Test Objective:
-- 1. Validate UNION ALL output remains stable when child branches require projection alignment.
-- 2. Prevent regressions in child projection mapping for UNION_NODE lowering.
-- Test Flow:
-- 1. Build deterministic scalar branches with casted expressions.
-- 2. Apply UNION ALL across branches.
-- 3. Assert ordered final output.
SELECT k, v
FROM (
    SELECT CAST(1 AS BIGINT) AS k, CAST('10' AS INT) AS v
    UNION ALL
    SELECT CAST(2 AS BIGINT) AS k, CAST('20' AS INT) AS v
    UNION ALL
    SELECT CAST(1 AS BIGINT) AS k, CAST('30' AS INT) AS v
) t
ORDER BY k, v;
