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
-- @tags=set_op,except,varchar
-- Test Objective:
-- 1. Validate set operations over explicit VARCHAR casts.
-- 2. Prevent regressions in string-typed set comparisons.
-- Test Flow:
-- 1. Build VARCHAR scalar sets.
-- 2. Apply EXCEPT.
-- 3. Assert ordered string output.
SELECT s
FROM (
    (
        SELECT CAST('a' AS VARCHAR) AS s
        UNION ALL
        SELECT CAST('b' AS VARCHAR)
        UNION ALL
        SELECT CAST('c' AS VARCHAR)
    )
    EXCEPT
    (
        SELECT CAST('b' AS VARCHAR) AS s
    )
) t
ORDER BY s;
