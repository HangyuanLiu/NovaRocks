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
-- @tags=set_op,intersect,multi_column,null
-- Test Objective:
-- 1. Validate INTERSECT semantics on multi-column rows containing NULLs.
-- 2. Prevent regressions where multi-column NULL row equality is handled inconsistently.
-- Test Flow:
-- 1. Build two multi-column row sets with overlaps and NULL-containing rows.
-- 2. Apply INTERSECT.
-- 3. Assert deterministic ordered output.
SELECT k, v
FROM (
    (
        SELECT CAST(1 AS BIGINT) AS k, CAST('x' AS VARCHAR) AS v
        UNION ALL
        SELECT CAST(1 AS BIGINT), CAST(NULL AS VARCHAR)
        UNION ALL
        SELECT CAST(NULL AS BIGINT), CAST('n' AS VARCHAR)
        UNION ALL
        SELECT CAST(2 AS BIGINT), CAST('y' AS VARCHAR)
    )
    INTERSECT
    (
        SELECT CAST(1 AS BIGINT) AS k, CAST('x' AS VARCHAR) AS v
        UNION ALL
        SELECT CAST(1 AS BIGINT), CAST(NULL AS VARCHAR)
        UNION ALL
        SELECT CAST(NULL AS BIGINT), CAST('n' AS VARCHAR)
        UNION ALL
        SELECT CAST(3 AS BIGINT), CAST('z' AS VARCHAR)
    )
) t
ORDER BY k IS NULL, k, v IS NULL, v;
