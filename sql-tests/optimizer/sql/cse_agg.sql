-- @tags=optimizer,cse
-- Test Objective:
-- Repeated aggregate argument scalar subexpressions are materialized once below
-- the Aggregate as an internal CSE Project item while aggregate results stay stable.
DROP TABLE IF EXISTS ${case_db}.cse_agg_t;
CREATE TABLE ${case_db}.cse_agg_t (a BIGINT, b BIGINT);
INSERT INTO ${case_db}.cse_agg_t VALUES (2, 3), (4, 5), (6, 7);

SELECT SUM(a * b) AS sum_ab, AVG(a * b) AS avg_ab
FROM ${case_db}.cse_agg_t;

-- @explain_contains=__cse_0
-- @result_not_contains=__cse_
SELECT SUM(a * b) AS sum_ab, AVG(a * b) AS avg_ab
FROM ${case_db}.cse_agg_t;
