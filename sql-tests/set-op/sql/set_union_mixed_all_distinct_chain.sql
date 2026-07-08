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
-- @tags=set_op,union,union_all,null,chain
-- Test Objective:
-- 1. Validate mixed UNION DISTINCT / UNION ALL chaining after optimizer normalization.
-- 2. Preserve SQL left-associative semantics: the final UNION ALL keeps duplicates
--    introduced after the distinct stage.
SELECT x
FROM (
    (
        SELECT CAST(1 AS BIGINT) AS x
        UNION ALL
        SELECT CAST(1 AS BIGINT)
        UNION ALL
        SELECT CAST(2 AS BIGINT)
    )
    UNION
    (
        SELECT CAST(2 AS BIGINT) AS x
        UNION ALL
        SELECT CAST(3 AS BIGINT)
        UNION ALL
        SELECT CAST(NULL AS BIGINT)
    )
    UNION ALL
    (
        SELECT CAST(NULL AS BIGINT) AS x
        UNION ALL
        SELECT CAST(4 AS BIGINT)
    )
) t
ORDER BY x IS NULL, x;
