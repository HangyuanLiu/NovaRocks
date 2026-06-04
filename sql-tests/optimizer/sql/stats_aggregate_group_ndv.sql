-- @tags=optimizer,stats,oq12
-- Test Objective:
-- Capture GROUP BY cardinality from two grouping keys.
DROP TABLE IF EXISTS ${case_db}.oq12_stats_group;
CREATE TABLE ${case_db}.oq12_stats_group (a INT, b INT, v INT);
INSERT INTO ${case_db}.oq12_stats_group
    SELECT generate_series % 10, generate_series % 20, generate_series
    FROM TABLE(generate_series(1, 1000));
ANALYZE TABLE ${case_db}.oq12_stats_group;

-- @explain_contains=HASH AGGREGATE
-- @explain_contains=group by: [a, b]
-- @explain_contains=oq12_stats_group
-- @explain_not_contains=stats={rows=1}
EXPLAIN VERBOSE SELECT a, b, SUM(v) AS total_v
FROM ${case_db}.oq12_stats_group
GROUP BY a, b;
