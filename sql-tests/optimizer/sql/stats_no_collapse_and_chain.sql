-- @tags=optimizer,stats,oq12
-- Test Objective:
-- Q85 proxy: many AND predicates must not collapse selectivity to one row.
DROP TABLE IF EXISTS ${case_db}.oq12_stats_and_chain;
CREATE TABLE ${case_db}.oq12_stats_and_chain (
    c1 INT,
    c2 INT,
    c3 INT,
    c4 INT,
    c5 INT,
    c6 INT,
    payload INT
);
INSERT INTO ${case_db}.oq12_stats_and_chain
    SELECT
        generate_series % 100,
        generate_series % 90,
        generate_series % 80,
        generate_series % 70,
        generate_series % 60,
        generate_series % 50,
        generate_series
    FROM TABLE(generate_series(1, 10000));
ANALYZE TABLE ${case_db}.oq12_stats_and_chain;

-- @explain_contains=oq12_stats_and_chain
-- @explain_not_contains=stats={rows=1}
EXPLAIN VERBOSE SELECT payload
FROM ${case_db}.oq12_stats_and_chain
WHERE c1 BETWEEN 0 AND 80
  AND c2 BETWEEN 0 AND 75
  AND c3 BETWEEN 0 AND 65
  AND c4 BETWEEN 0 AND 55
  AND c5 BETWEEN 0 AND 45
  AND c6 BETWEEN 0 AND 35;
