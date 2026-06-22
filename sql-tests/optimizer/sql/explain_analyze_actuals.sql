-- @tags=optimizer,explain_analyze,actuals
-- Test Objective:
-- 1. EXPLAIN ANALYZE fails fast until RemoteDispatcher can collect profiles.
DROP TABLE IF EXISTS ${case_db}.explain_analyze_actuals_l;
DROP TABLE IF EXISTS ${case_db}.explain_analyze_actuals_r;
CREATE TABLE ${case_db}.explain_analyze_actuals_l (k INT, v INT);
CREATE TABLE ${case_db}.explain_analyze_actuals_r (k INT, v INT);
INSERT INTO ${case_db}.explain_analyze_actuals_l VALUES (1, 10), (2, 20), (3, 30);
INSERT INTO ${case_db}.explain_analyze_actuals_r VALUES (1, 100), (2, 200), (4, 400);

-- @expect_error=EXPLAIN ANALYZE requires remote fragment profile collection
EXPLAIN ANALYZE
SELECT COUNT(*)
FROM ${case_db}.explain_analyze_actuals_l l
INNER JOIN ${case_db}.explain_analyze_actuals_r r ON l.k = r.k;
