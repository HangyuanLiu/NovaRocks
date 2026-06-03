-- @tags=optimizer,stats,oq12
-- Test Objective:
-- Capture outer, semi, and anti join cardinality without one-row collapse.
DROP TABLE IF EXISTS ${case_db}.oq12_stats_join_l;
DROP TABLE IF EXISTS ${case_db}.oq12_stats_join_r;
CREATE TABLE ${case_db}.oq12_stats_join_l (k INT, payload INT);
CREATE TABLE ${case_db}.oq12_stats_join_r (k INT, flag INT);
INSERT INTO ${case_db}.oq12_stats_join_l
    SELECT generate_series % 100, generate_series
    FROM TABLE(generate_series(1, 1000));
INSERT INTO ${case_db}.oq12_stats_join_r
    SELECT generate_series % 80, generate_series % 2
    FROM TABLE(generate_series(1, 800));
ANALYZE TABLE ${case_db}.oq12_stats_join_l;
ANALYZE TABLE ${case_db}.oq12_stats_join_r;

-- @explain_contains=HASH JOIN
-- @explain_contains=LEFT OUTER
-- @explain_contains=oq12_stats_join_l
-- @explain_contains=oq12_stats_join_r
-- @explain_not_contains=stats={rows=1}
EXPLAIN VERBOSE SELECT l.k, r.flag
FROM ${case_db}.oq12_stats_join_l l
LEFT OUTER JOIN ${case_db}.oq12_stats_join_r r ON l.k = r.k;

-- @explain_contains=HASH JOIN
-- @explain_contains=LEFT SEMI
-- @explain_contains=oq12_stats_join_l
-- @explain_contains=oq12_stats_join_r
-- @explain_not_contains=stats={rows=1}
EXPLAIN VERBOSE SELECT l.k
FROM ${case_db}.oq12_stats_join_l l
LEFT SEMI JOIN ${case_db}.oq12_stats_join_r r ON l.k = r.k;

-- @explain_contains=HASH JOIN
-- @explain_contains=LEFT ANTI
-- @explain_contains=oq12_stats_join_l
-- @explain_contains=oq12_stats_join_r
-- @explain_not_contains=stats={rows=1}
EXPLAIN VERBOSE SELECT l.k
FROM ${case_db}.oq12_stats_join_l l
LEFT ANTI JOIN ${case_db}.oq12_stats_join_r r ON l.k = r.k;
