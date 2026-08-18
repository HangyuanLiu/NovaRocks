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
-- @tags=aggregate,count_if,null,composite
-- Test Objective:
-- 1. Validate count_if over nullable composite boolean expressions.
-- 2. Prevent regressions where NULL predicate values are counted as TRUE.
-- Test Flow:
-- 1. Create/reset source table with nullable inputs.
-- 2. Insert rows that yield TRUE/FALSE/NULL predicate outcomes.
-- 3. Aggregate by group and assert deterministic count_if outputs.
CREATE TABLE ${case_db}.t_agg_count_if_nullable_composite (
    g INT,
    x INT,
    y INT
)
TBLPROPERTIES ("format-version" = "3");

INSERT INTO ${case_db}.t_agg_count_if_nullable_composite VALUES
    (1, 3, 1),
    (1, 3, 0),
    (1, NULL, 2),
    (1, 5, NULL),
    (2, 2, 1),
    (2, 1, 5),
    (2, NULL, NULL);

SELECT
    g,
    count_if(x > y AND y > 0) AS cnt_gt,
    count_if(x IS NULL OR y IS NULL) AS cnt_has_null
FROM ${case_db}.t_agg_count_if_nullable_composite
GROUP BY g
ORDER BY g;
