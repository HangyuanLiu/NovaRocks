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
-- @tags=set_op,union,null
-- Test Objective:
-- 1. Validate UNION distinct NULL dedup semantics.
-- 2. Prevent regressions in NULL equality behavior in set operations.
-- Test Flow:
-- 1. Build branches with NULL and non-NULL values.
-- 2. Apply UNION.
-- 3. Assert deterministic ordering with one NULL row.
SELECT x
FROM (
    SELECT CAST(NULL AS BIGINT) AS x
    UNION
    SELECT CAST(NULL AS BIGINT)
    UNION
    SELECT CAST(1 AS BIGINT)
) t
ORDER BY x IS NULL, x;
