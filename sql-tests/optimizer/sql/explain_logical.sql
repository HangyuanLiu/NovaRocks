-- @tags=optimizer,explain,logical
-- Test Objective:
-- 1. Preserve the StarRocks-style EXPLAIN LOGICAL interface.
-- 2. EXPLAIN LOGICAL renders the non-distributed logical plan, not the
--    DistributedPlan fragment form used by ordinary EXPLAIN.
DROP TABLE IF EXISTS ${case_db}.explain_logical_l;
DROP TABLE IF EXISTS ${case_db}.explain_logical_r;
CREATE TABLE ${case_db}.explain_logical_l (k INT, v INT);
CREATE TABLE ${case_db}.explain_logical_r (k INT, v INT);
INSERT INTO ${case_db}.explain_logical_l VALUES (1, 10), (2, 20);
INSERT INTO ${case_db}.explain_logical_r VALUES (1, 100), (3, 300);

-- @skip_result_check=true
-- @result_contains=PROJECT [
-- @result_contains=INNER JOIN
-- @result_contains=on: l.k = r.k
-- @result_contains=0:SCAN
-- @result_not_contains=PLAN FRAGMENT
-- @result_not_contains=HASH JOIN
EXPLAIN LOGICAL
SELECT l.k, r.v
FROM ${case_db}.explain_logical_l l
INNER JOIN ${case_db}.explain_logical_r r ON l.k = r.k;
