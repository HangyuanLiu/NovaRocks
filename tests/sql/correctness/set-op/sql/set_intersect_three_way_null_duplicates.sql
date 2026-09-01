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
-- @tags=set_op,intersect,null,duplicates
-- Test Objective:
-- 1. Validate INTERSECT semantics across three inputs with duplicated rows and NULL values.
-- 2. Prevent regressions in multi-stage INTERSECT marker progression after set-op refactor.
-- Test Flow:
-- 1. Build three deterministic BIGINT branches containing duplicates and NULL.
-- 2. Apply INTERSECT across three branches.
-- 3. Assert ordered final output.
SELECT x
FROM (
    (
        SELECT CAST(1 AS BIGINT) AS x
        UNION ALL
        SELECT CAST(1 AS BIGINT)
        UNION ALL
        SELECT CAST(2 AS BIGINT)
        UNION ALL
        SELECT CAST(NULL AS BIGINT)
        UNION ALL
        SELECT CAST(3 AS BIGINT)
    )
    INTERSECT
    (
        SELECT CAST(1 AS BIGINT) AS x
        UNION ALL
        SELECT CAST(NULL AS BIGINT)
        UNION ALL
        SELECT CAST(2 AS BIGINT)
        UNION ALL
        SELECT CAST(2 AS BIGINT)
    )
    INTERSECT
    (
        SELECT CAST(1 AS BIGINT) AS x
        UNION ALL
        SELECT CAST(NULL AS BIGINT)
        UNION ALL
        SELECT CAST(4 AS BIGINT)
    )
) t
ORDER BY x IS NULL, x;
