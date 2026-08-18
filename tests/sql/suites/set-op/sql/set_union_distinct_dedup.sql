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
-- @tags=set_op,union
-- Test Objective:
-- 1. Validate UNION distinct dedup semantics.
-- 2. Prevent regressions where duplicate rows survive UNION.
-- Test Flow:
-- 1. Build deterministic scalar branches with duplicates.
-- 2. Apply UNION (distinct).
-- 3. Assert ordered deduplicated output.
SELECT x
FROM (
    SELECT CAST(1 AS BIGINT) AS x
    UNION
    SELECT CAST(2 AS BIGINT)
    UNION
    SELECT CAST(2 AS BIGINT)
    UNION
    SELECT CAST(3 AS BIGINT)
) t
ORDER BY x;
