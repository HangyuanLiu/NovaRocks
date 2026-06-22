-- @tags=optimizer,explain_analyze
-- Test Objective:
-- 1. EXPLAIN ANALYZE fails fast until RemoteDispatcher can collect profiles.
DROP TABLE IF EXISTS ${case_db}.t_analyze_header;
CREATE TABLE ${case_db}.t_analyze_header (k INT);
INSERT INTO ${case_db}.t_analyze_header VALUES (1), (2), (3);
ANALYZE TABLE ${case_db}.t_analyze_header;

-- @expect_error=EXPLAIN ANALYZE requires remote fragment profile collection
EXPLAIN ANALYZE
SELECT COUNT(*) FROM ${case_db}.t_analyze_header;
