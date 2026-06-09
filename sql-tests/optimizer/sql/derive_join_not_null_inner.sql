-- @tags=optimizer,derive_join_not_null
-- Test Objective:
-- INNER JOIN on nullable keys derives IS NOT NULL on BOTH scan sides.
DROP TABLE IF EXISTS ${case_db}.t_dnn_l;
DROP TABLE IF EXISTS ${case_db}.t_dnn_r;
CREATE TABLE ${case_db}.t_dnn_l (k INT, v INT);
CREATE TABLE ${case_db}.t_dnn_r (k INT, v INT);
INSERT INTO ${case_db}.t_dnn_l
    SELECT CASE WHEN generate_series % 12 = 0 THEN generate_series ELSE NULL END, generate_series
    FROM TABLE(generate_series(1, 2000));
INSERT INTO ${case_db}.t_dnn_r
    SELECT CASE WHEN generate_series % 12 = 0 THEN generate_series ELSE NULL END, generate_series
    FROM TABLE(generate_series(1, 2000));
ANALYZE TABLE ${case_db}.t_dnn_l;
ANALYZE TABLE ${case_db}.t_dnn_r;
-- @explain_contains=IS NOT NULL
EXPLAIN VERBOSE SELECT l.v, r.v
FROM ${case_db}.t_dnn_l l
INNER JOIN ${case_db}.t_dnn_r r ON l.k = r.k;
