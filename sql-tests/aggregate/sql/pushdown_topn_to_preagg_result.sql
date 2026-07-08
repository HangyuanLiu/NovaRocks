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
-- @tags=aggregate,topn,optimizer,session_rule_disable
-- Test Objective:
-- 1. Validate PushDownTopNToPreAgg preserves grouped TopN results.
-- 2. Compare the same deterministic GROUP BY city ORDER BY city LIMIT query
--    with the rule enabled and disabled.

CREATE TABLE ${case_db}.t_pushdown_topn_preagg_result (
    city VARCHAR(16),
    sales INT
)
TBLPROPERTIES ("format-version" = "3");

INSERT INTO ${case_db}.t_pushdown_topn_preagg_result VALUES
    ('a', 1),
    ('a', 2),
    ('a', 10),
    ('b', 5),
    ('b', 1),
    ('c', 3),
    ('c', 4),
    ('d', 9),
    ('d', 2),
    ('e', 7),
    ('e', 1),
    ('f', 8);

ANALYZE TABLE ${case_db}.t_pushdown_topn_preagg_result;

SET disable_optimizer_rules = '';

SELECT city, SUM(sales) AS total_sales
FROM (
    SELECT city, sales FROM ${case_db}.t_pushdown_topn_preagg_result
    UNION ALL
    SELECT city, sales FROM ${case_db}.t_pushdown_topn_preagg_result
) o
GROUP BY city
ORDER BY city
LIMIT 3;

SET disable_optimizer_rules = 'PushDownTopNToPreAgg';

SELECT city, SUM(sales) AS total_sales
FROM (
    SELECT city, sales FROM ${case_db}.t_pushdown_topn_preagg_result
    UNION ALL
    SELECT city, sales FROM ${case_db}.t_pushdown_topn_preagg_result
) o
GROUP BY city
ORDER BY city
LIMIT 3;

SET disable_optimizer_rules = '';
