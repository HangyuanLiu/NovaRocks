-- @tags=optimizer,stats,oq12
-- Test Objective:
-- Capture OR selectivity so inclusion-exclusion does not collapse to one row.
DROP TABLE IF EXISTS ${case_db}.oq12_stats_or;
CREATE TABLE ${case_db}.oq12_stats_or (a INT, b INT);
INSERT INTO ${case_db}.oq12_stats_or
    SELECT generate_series % 100, generate_series
    FROM TABLE(generate_series(1, 1000));
ANALYZE TABLE ${case_db}.oq12_stats_or;

-- @explain_contains=oq12_stats_or
-- @explain_contains=OR
-- @explain_not_contains=stats={rows=1}
EXPLAIN VERBOSE SELECT b
FROM ${case_db}.oq12_stats_or
WHERE a = 1 OR a = 2;
