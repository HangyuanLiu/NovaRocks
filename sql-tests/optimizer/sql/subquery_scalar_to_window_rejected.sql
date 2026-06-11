-- @tags=optimizer,oq13,subquery_to_window_rejected
-- Test Objective: ApplyToWindow is NOT applied to shapes that violate its
-- preconditions. Each sub-case asserts @explain_not_contains=WINDOW [ and
-- verifies the result rows are correct (ScalarApplyToJoin fallback is sound).
--
-- Three rejection guards:
--   1. Self-join: outer has two instances of the subquery's table.
--   2. Predicate mismatch: subquery has an extra residual filter absent from outer.
--   3. Distinct aggregate: avg(DISTINCT ...) is not window-eligible.
DROP TABLE IF EXISTS ${case_db}.wm_line;
DROP TABLE IF EXISTS ${case_db}.wm_part;
CREATE TABLE ${case_db}.wm_line (l_partkey INT, l_quantity INT, l_ext INT);
CREATE TABLE ${case_db}.wm_part (p_partkey INT, p_brand VARCHAR(16));
INSERT INTO ${case_db}.wm_line VALUES (1,5,100),(1,50,200),(2,7,300),(2,8,150),(3,9,90);
INSERT INTO ${case_db}.wm_part VALUES (1,'B1'),(2,'B1'),(3,'B2');
ANALYZE TABLE ${case_db}.wm_line;
ANALYZE TABLE ${case_db}.wm_part;

SET subquery_unnest_mode='apply';

-- Guard 1: self-join — outer has two instances of wm_line (table-set check fails).
-- @explain_not_contains=WINDOW [
-- @explain_contains=OUTER
SELECT sum(a.l_ext)
FROM ${case_db}.wm_line a, ${case_db}.wm_line b
WHERE a.l_partkey = b.l_partkey
  AND a.l_quantity < (SELECT avg(l_quantity) FROM ${case_db}.wm_line WHERE l_partkey = a.l_partkey);

-- Correctness for self-join rejection.
SELECT sum(a.l_ext)
FROM ${case_db}.wm_line a, ${case_db}.wm_line b
WHERE a.l_partkey = b.l_partkey
  AND a.l_quantity < (SELECT avg(l_quantity) FROM ${case_db}.wm_line WHERE l_partkey = a.l_partkey);

-- Guard 2: predicate mismatch — subquery has extra residual filter (l_quantity > 0)
-- with no matching twin in the outer WHERE block.
-- @explain_not_contains=WINDOW [
-- @explain_contains=OUTER
SELECT sum(l_ext)
FROM ${case_db}.wm_line, ${case_db}.wm_part
WHERE p_partkey = l_partkey
  AND l_quantity < (SELECT avg(l_quantity) FROM ${case_db}.wm_line WHERE l_partkey = p_partkey AND l_quantity > 0);

-- Correctness for predicate mismatch rejection.
SELECT sum(l_ext)
FROM ${case_db}.wm_line, ${case_db}.wm_part
WHERE p_partkey = l_partkey
  AND l_quantity < (SELECT avg(l_quantity) FROM ${case_db}.wm_line WHERE l_partkey = p_partkey AND l_quantity > 0);

-- Guard 3: distinct aggregate — avg(DISTINCT ...) has no window analogue.
-- @explain_not_contains=WINDOW [
-- @explain_contains=OUTER
SELECT sum(l_ext)
FROM ${case_db}.wm_line, ${case_db}.wm_part
WHERE p_partkey = l_partkey
  AND l_quantity < (SELECT avg(DISTINCT l_quantity) FROM ${case_db}.wm_line WHERE l_partkey = p_partkey);

-- Correctness for distinct agg rejection.
SELECT sum(l_ext)
FROM ${case_db}.wm_line, ${case_db}.wm_part
WHERE p_partkey = l_partkey
  AND l_quantity < (SELECT avg(DISTINCT l_quantity) FROM ${case_db}.wm_line WHERE l_partkey = p_partkey);

SET subquery_unnest_mode='legacy';
