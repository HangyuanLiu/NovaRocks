-- @tags=optimizer,cse
-- Test Objective:
-- Repeated projection-list scalar subexpressions are materialized once as an
-- internal CSE Project item while user-visible query output stays unchanged.
DROP TABLE IF EXISTS ${case_db}.cse_projection_t;
CREATE TABLE ${case_db}.cse_projection_t (a BIGINT, b BIGINT);
INSERT INTO ${case_db}.cse_projection_t VALUES (3, 4), (5, 6);

SELECT (a + b) AS x, (a + b) + a AS y
FROM ${case_db}.cse_projection_t
ORDER BY a;

-- @explain_contains=__cse_0
-- @result_not_contains=__cse_
SELECT (a + b) AS x, (a + b) + a AS y
FROM ${case_db}.cse_projection_t
ORDER BY a;
