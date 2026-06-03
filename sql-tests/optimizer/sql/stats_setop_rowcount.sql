-- @tags=optimizer,stats,oq12
-- Test Objective:
-- Capture set-operation row-count estimates for all/distinct/intersect/except.
DROP TABLE IF EXISTS ${case_db}.oq12_stats_set_l;
DROP TABLE IF EXISTS ${case_db}.oq12_stats_set_r;
CREATE TABLE ${case_db}.oq12_stats_set_l (k INT);
CREATE TABLE ${case_db}.oq12_stats_set_r (k INT);
INSERT INTO ${case_db}.oq12_stats_set_l
    SELECT generate_series % 100
    FROM TABLE(generate_series(1, 1000));
INSERT INTO ${case_db}.oq12_stats_set_r
    SELECT generate_series % 60
    FROM TABLE(generate_series(1, 600));
ANALYZE TABLE ${case_db}.oq12_stats_set_l;
ANALYZE TABLE ${case_db}.oq12_stats_set_r;

-- @explain_contains=UNION ALL
-- @explain_contains=oq12_stats_set_l
-- @explain_contains=oq12_stats_set_r
EXPLAIN VERBOSE SELECT k FROM ${case_db}.oq12_stats_set_l
UNION ALL
SELECT k FROM ${case_db}.oq12_stats_set_r;

-- @explain_contains=UNION
-- @explain_contains=oq12_stats_set_l
-- @explain_contains=oq12_stats_set_r
EXPLAIN VERBOSE SELECT k FROM ${case_db}.oq12_stats_set_l
UNION
SELECT k FROM ${case_db}.oq12_stats_set_r;

-- @explain_contains=INTERSECT
-- @explain_contains=oq12_stats_set_l
-- @explain_contains=oq12_stats_set_r
EXPLAIN VERBOSE SELECT k FROM ${case_db}.oq12_stats_set_l
INTERSECT
SELECT k FROM ${case_db}.oq12_stats_set_r;

-- @explain_contains=EXCEPT
-- @explain_contains=oq12_stats_set_l
-- @explain_contains=oq12_stats_set_r
EXPLAIN VERBOSE SELECT k FROM ${case_db}.oq12_stats_set_l
EXCEPT
SELECT k FROM ${case_db}.oq12_stats_set_r;
