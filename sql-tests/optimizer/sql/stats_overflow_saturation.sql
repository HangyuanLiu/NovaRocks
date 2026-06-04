-- @tags=optimizer,stats,oq12
-- Test Objective:
-- Q72 proxy: cross join estimates should stay finite and readable.
DROP TABLE IF EXISTS ${case_db}.oq12_stats_big_a;
DROP TABLE IF EXISTS ${case_db}.oq12_stats_big_b;
CREATE TABLE ${case_db}.oq12_stats_big_a (k INT);
CREATE TABLE ${case_db}.oq12_stats_big_b (k INT);
INSERT INTO ${case_db}.oq12_stats_big_a
    SELECT generate_series FROM TABLE(generate_series(1, 100));
INSERT INTO ${case_db}.oq12_stats_big_b
    SELECT generate_series FROM TABLE(generate_series(1, 100));
ANALYZE TABLE ${case_db}.oq12_stats_big_a;
ANALYZE TABLE ${case_db}.oq12_stats_big_b;

-- @explain_contains=CROSS
-- @explain_contains=oq12_stats_big_a
-- @explain_contains=oq12_stats_big_b
-- @explain_not_contains=rows=>=
-- @explain_not_contains=9223372036854775807
EXPLAIN VERBOSE SELECT COUNT(*)
FROM ${case_db}.oq12_stats_big_a a
CROSS JOIN ${case_db}.oq12_stats_big_b b;
