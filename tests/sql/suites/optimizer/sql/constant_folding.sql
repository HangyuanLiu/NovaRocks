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

-- @tags=optimizer,constant_folding
-- Test Objective:
-- 1. The FoldConstant rewrite rule evaluates constant scalar sub-trees during
--    LogicalNormalize, before predicate pushdown, so the plan carries literals
--    instead of arithmetic / cast / function nodes.
-- 2. A string literal compared against a DATE column folds to the same plan a
--    typed DATE literal produces, which is what makes the predicate eligible
--    for connector static-predicate pushdown (partition / row-group pruning).
-- 3. Volatile functions are never folded.
-- 4. SET disable_optimizer_rules = 'FoldConstant' restores the unfolded plan
--    without changing results.
-- 5. Expressions whose evaluation fails stay in the plan and keep their
--    runtime behaviour (fail-open).

DROP TABLE IF EXISTS ${case_db}.t_fold_dates;
CREATE TABLE ${case_db}.t_fold_dates (id INT, dt DATE);
INSERT INTO ${case_db}.t_fold_dates VALUES
    (1, DATE '2019-12-31'),
    (2, DATE '2020-01-01'),
    (3, DATE '2020-01-02');

-- ---------------------------------------------------------------------------
-- 1. Projection: constant arithmetic collapses to one literal.
--    The output column label still renders the original expression text
--    (labels come from the parsed statement, not from the folded plan).
-- ---------------------------------------------------------------------------
-- @explain_contains=42096
-- @explain_not_contains=40000 + 2000
SELECT 40000 + 2000 + 96;

-- ---------------------------------------------------------------------------
-- 2. Projection: a constant function call collapses to its result literal.
-- ---------------------------------------------------------------------------
-- @explain_contains='2020-01'
-- @explain_not_contains=date_format(
SELECT date_format(DATE '2020-01-01', '%Y-%m');

-- ---------------------------------------------------------------------------
-- 3. Predicate: a string literal against a DATE column folds to the same
--    Date32 literal the typed DATE form produces. Both plans must show the
--    folded epoch-day literal and neither may keep a CAST node, which is the
--    precondition for static-predicate lowering.
-- ---------------------------------------------------------------------------
-- @explain_contains=18262
-- @explain_not_contains=CAST
SELECT id FROM ${case_db}.t_fold_dates WHERE dt = '2020-01-01';

-- @explain_contains=18262
-- @explain_not_contains=CAST
SELECT id FROM ${case_db}.t_fold_dates WHERE dt = DATE '2020-01-01';

-- ---------------------------------------------------------------------------
-- 4. Volatile builtins are never folded: the call must survive in the plan.
-- ---------------------------------------------------------------------------
-- @skip_result_check=true
-- @result_contains=rand
EXPLAIN VERBOSE SELECT rand();

-- @skip_result_check=true
-- @result_contains=now
EXPLAIN VERBOSE SELECT now();

-- ---------------------------------------------------------------------------
-- 5. Disabling the rule restores the unfolded plan and keeps the same result.
-- ---------------------------------------------------------------------------
SET disable_optimizer_rules = 'FoldConstant';

-- @explain_contains=40000 + 2000
SELECT 40000 + 2000 + 96;

-- @explain_contains=CAST
SELECT id FROM ${case_db}.t_fold_dates WHERE dt = '2020-01-01';

SET disable_optimizer_rules = '';

-- ---------------------------------------------------------------------------
-- 6. Fail-open: an expression the evaluator cannot evaluate keeps its runtime
--    behaviour instead of becoming a planning error.
-- ---------------------------------------------------------------------------
SELECT 1 / 0;
