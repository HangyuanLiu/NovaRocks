-- @tags=optimizer,g4,hash_source
-- Test Objective:
-- 1. A window partition above the left join must still introduce its own
--    HASH EXCHANGE for the partition key.
-- 2. ANALYZE TABLE supplies the scan row counts used by the optimizer; table
--    names must not carry any row-count heuristic.
DROP TABLE IF EXISTS ${case_db}.lineitem;
DROP TABLE IF EXISTS ${case_db}.orders;
CREATE TABLE ${case_db}.lineitem (k INT, v INT);
CREATE TABLE ${case_db}.orders (k INT, w INT);
INSERT INTO ${case_db}.lineitem VALUES (1, 10), (2, 20);
INSERT INTO ${case_db}.orders VALUES (1, 100), (2, 200);
ANALYZE TABLE ${case_db}.lineitem;
ANALYZE TABLE ${case_db}.orders;

SET disable_optimizer_rules = 'JoinCommutativity';

EXPLAIN VERBOSE
SELECT l.k, l.v, r.w,
       ROW_NUMBER() OVER (PARTITION BY l.k ORDER BY l.v) AS rn
FROM ${case_db}.lineitem l
LEFT JOIN ${case_db}.orders r ON l.k = r.k;

SET disable_optimizer_rules = '';
