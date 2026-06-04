-- @tags=optimizer,g4,hash_source
-- Test Objective:
-- 1. A PARTITIONED join output keyed as ShuffleJoin([l.k, r.k]) must not satisfy
--    a narrower ShuffleAgg([l.k]) requirement from the analytic partition.
-- 2. The window/sort above the join must show its own HASH EXCHANGE
--    with source ShuffleAgg, while join-side exchanges show source ShuffleJoin.
-- 3. lineitem/orders names intentionally use large default row-count estimates
--    so Broadcast is pruned by the broadcast row-count limit.
DROP TABLE IF EXISTS ${case_db}.lineitem;
DROP TABLE IF EXISTS ${case_db}.orders;
CREATE TABLE ${case_db}.lineitem (k INT, v INT);
CREATE TABLE ${case_db}.orders (k INT, w INT);
INSERT INTO ${case_db}.lineitem VALUES (1, 10), (2, 20);
INSERT INTO ${case_db}.orders VALUES (1, 100), (2, 200);

SET disable_optimizer_rules = 'JoinCommutativity';

EXPLAIN VERBOSE
SELECT l.k, l.v, r.w,
       ROW_NUMBER() OVER (PARTITION BY l.k ORDER BY l.v) AS rn
FROM ${case_db}.lineitem l
LEFT JOIN ${case_db}.orders r ON l.k = r.k;

SET disable_optimizer_rules = '';
