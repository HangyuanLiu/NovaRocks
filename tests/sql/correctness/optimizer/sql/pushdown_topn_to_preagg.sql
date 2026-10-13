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

-- @tags=optimizer,topn,aggregate,session_rule_disable
-- Test Objective:
-- Rows TopN budgets consume complete Global groups. The native physical
-- contract checks their exact sequence and input; these EXPLAIN assertions
-- cover the planner's ordered-bound publication without unstable node IDs.

DROP TABLE IF EXISTS ${case_db}.t_pushdown_topn_preagg;
CREATE TABLE ${case_db}.t_pushdown_topn_preagg (city INT, sales INT);
INSERT INTO ${case_db}.t_pushdown_topn_preagg
    SELECT generate_series % 1000, generate_series
    FROM TABLE(generate_series(1, 100000));
ANALYZE TABLE ${case_db}.t_pushdown_topn_preagg;

SET disable_optimizer_rules = '';

-- Normal SplitTopN ranks complete Global groups and publishes a live bound.
-- @skip_result_check=true
-- @result_contains=LOCAL TOP-N
-- @result_contains=HASH AGGREGATE (GLOBAL, group by: [city])
-- @result_contains=domain = OrderedBound(key=Int32 ASC NULLS FIRST, inclusive=true)
-- @result_contains=activation = NonBlockingLive(Batch)
EXPLAIN VERBOSE
SELECT city, SUM(sales)
FROM (
    SELECT city, sales FROM ${case_db}.t_pushdown_topn_preagg
    UNION ALL
    SELECT city, sales FROM ${case_db}.t_pushdown_topn_preagg
) o
GROUP BY city
ORDER BY city
LIMIT 10;

SET disable_optimizer_rules = 'SplitTopN';

-- A single Rows budget still completes the answer, without a split producer.
-- @skip_result_check=true
-- @result_contains=TOP-N
-- @result_contains=HASH AGGREGATE (
-- @result_not_contains=LOCAL TOP-N
-- @result_not_contains=domain = OrderedBound
EXPLAIN VERBOSE
SELECT city, SUM(sales)
FROM (
    SELECT city, sales FROM ${case_db}.t_pushdown_topn_preagg
    UNION ALL
    SELECT city, sales FROM ${case_db}.t_pushdown_topn_preagg
) o
GROUP BY city
ORDER BY city
LIMIT 10;

SET disable_optimizer_rules = '';

-- Ranking an aggregate result cannot publish a bound on its input group key.
-- @skip_result_check=true
-- @result_contains=sum(sales) DESC NULLS LAST
-- @result_not_contains=domain = OrderedBound
EXPLAIN VERBOSE
SELECT city, SUM(sales) AS s
FROM (
    SELECT city, sales FROM ${case_db}.t_pushdown_topn_preagg
    UNION ALL
    SELECT city, sales FROM ${case_db}.t_pushdown_topn_preagg
) o
GROUP BY city
ORDER BY s DESC
LIMIT 10;

-- A proper subset of grouping keys cannot prove a distinct-key bound.
-- @skip_result_check=true
-- @result_contains=group by: [city, sales]
-- @result_not_contains=domain = OrderedBound
EXPLAIN VERBOSE
SELECT city, sales, SUM(sales) AS s
FROM (
    SELECT city, sales FROM ${case_db}.t_pushdown_topn_preagg
    UNION ALL
    SELECT city, sales FROM ${case_db}.t_pushdown_topn_preagg
) o
GROUP BY city, sales
ORDER BY city
LIMIT 10;

SET disable_optimizer_rules = '';
