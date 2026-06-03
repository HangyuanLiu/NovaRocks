-- @tags=optimizer,stats,oq12
-- Test Objective:
-- Capture the current multi-key join cardinality estimate with moderate NDV.
DROP TABLE IF EXISTS ${case_db}.oq12_stats_mj_l;
DROP TABLE IF EXISTS ${case_db}.oq12_stats_mj_r;
CREATE TABLE ${case_db}.oq12_stats_mj_l (k1 INT, k2 INT, payload INT);
CREATE TABLE ${case_db}.oq12_stats_mj_r (k1 INT, k2 INT, payload INT);
INSERT INTO ${case_db}.oq12_stats_mj_l
    SELECT generate_series % 50, generate_series % 20, generate_series
    FROM TABLE(generate_series(1, 1000));
INSERT INTO ${case_db}.oq12_stats_mj_r
    SELECT generate_series % 50, generate_series % 20, generate_series * 10
    FROM TABLE(generate_series(1, 1000));
ANALYZE TABLE ${case_db}.oq12_stats_mj_l;
ANALYZE TABLE ${case_db}.oq12_stats_mj_r;

-- @explain_contains=HASH JOIN
-- @explain_contains=eq: [
-- @explain_contains=k1
-- @explain_contains=k2
-- @explain_contains=oq12_stats_mj_l
-- @explain_contains=oq12_stats_mj_r
EXPLAIN VERBOSE SELECT l.k1, l.k2, COUNT(*) AS match_count
FROM ${case_db}.oq12_stats_mj_l l
INNER JOIN ${case_db}.oq12_stats_mj_r r
    ON l.k1 = r.k1 AND l.k2 = r.k2
GROUP BY l.k1, l.k2;
