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
-- 1. FoldConstant evaluates constant scalar sub-trees during LogicalNormalize,
--    so the plan carries literals instead of arithmetic / cast / function nodes.
-- 2. A string literal compared against a DATE column folds to the same scan
--    predicate the typed DATE literal produces. That equality is the point of
--    the rule: static-predicate lowering only accepts a bare literal, so the
--    unfolded CAST shape never reached partition / row-group / page pruning.
-- 3. Volatile builtins and environment-sensitive builtins are never folded.
-- 4. SET disable_optimizer_rules = 'FoldConstant' restores the unfolded plan
--    while the results stay identical.
--
-- Note on labels: the output column label is rendered from the parsed
-- statement, so it still shows the original expression text after folding.
-- The assertions below therefore target the projection *expression* position
-- (`[<expr> AS <label>]`), not the label.
DROP TABLE IF EXISTS ${case_db}.t_fold_dates;
CREATE TABLE ${case_db}.t_fold_dates (id INT, dt DATE);
INSERT INTO ${case_db}.t_fold_dates VALUES
    (1, '2019-12-31'),
    (2, '2020-01-01'),
    (3, '2020-01-02');

-- ---------------------------------------------------------------------------
-- 1. Constant arithmetic collapses to one literal.
-- ---------------------------------------------------------------------------
-- @explain_contains=[42096 AS
-- @explain_not_contains=[40000 + 2000 + 96 AS
SELECT 40000 + 2000 + 96;

-- ---------------------------------------------------------------------------
-- 2. A constant function call collapses to its result literal. The string
--    argument binds to the DATE overload, so the intermediate cast folds to a
--    Date32 literal first and the whole call folds afterwards.
-- ---------------------------------------------------------------------------
-- @explain_contains=['2020-01' AS
-- @explain_not_contains=[date_format(
SELECT date_format('2020-01-01', '%Y-%m');

-- ---------------------------------------------------------------------------
-- 3. A string literal against a DATE column folds to the epoch-day literal,
--    with no CAST left in the scan predicate.
-- ---------------------------------------------------------------------------
-- @explain_contains=predicates: dt = 18262
-- @explain_not_contains=CAST(
SELECT id FROM ${case_db}.t_fold_dates WHERE dt = '2020-01-01';

-- ---------------------------------------------------------------------------
-- 4. The typed DATE literal must produce exactly the same scan predicate.
-- ---------------------------------------------------------------------------
-- @explain_contains=predicates: dt = 18262
-- @explain_not_contains=CAST(
SELECT id FROM ${case_db}.t_fold_dates WHERE dt = DATE '2020-01-01';

-- ---------------------------------------------------------------------------
-- 5. Volatile builtins survive in the plan.
-- ---------------------------------------------------------------------------
-- @skip_result_check=true
-- @result_contains=[rand()]
EXPLAIN VERBOSE SELECT rand();

-- @skip_result_check=true
-- @result_contains=[now()]
EXPLAIN VERBOSE SELECT now();

-- ---------------------------------------------------------------------------
-- 6. Environment-sensitive builtins are immutable but read the process
--    timezone, so folding them on the frontend would move that read off the
--    backend. They must survive in the plan too.
-- ---------------------------------------------------------------------------
-- @skip_result_check=true
-- @result_contains=[from_unixtime(1577836800)]
EXPLAIN VERBOSE SELECT from_unixtime(1577836800);

-- ---------------------------------------------------------------------------
-- 7. Division by zero is a value, not an error, in this engine: folding
--    reproduces the runtime NULL instead of failing planning.
-- ---------------------------------------------------------------------------
-- @explain_contains=[NULL AS 1 / 0]
SELECT 1 / 0;

-- ---------------------------------------------------------------------------
-- 8. Disabling the rule restores the unfolded plan; results are unchanged.
-- ---------------------------------------------------------------------------
SET disable_optimizer_rules = 'FoldConstant';

-- @explain_contains=[40000 + 2000 + 96 AS
SELECT 40000 + 2000 + 96;

-- @explain_contains=predicates: dt = CAST('2020-01-01' AS Date32)
SELECT id FROM ${case_db}.t_fold_dates WHERE dt = '2020-01-01';
