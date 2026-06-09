-- @tags=optimizer,derive_join_not_null
-- Test Objective:
-- LEFT ANTI JOIN must NOT derive any IS NOT NULL (left NULL keys are emitted).
-- The recorded golden is the regression guard for absence.
DROP TABLE IF EXISTS ${case_db}.t_dnn_al;
DROP TABLE IF EXISTS ${case_db}.t_dnn_ar;
CREATE TABLE ${case_db}.t_dnn_al (k INT, v INT);
CREATE TABLE ${case_db}.t_dnn_ar (k INT);
INSERT INTO ${case_db}.t_dnn_al
    SELECT CASE WHEN generate_series % 12 = 0 THEN generate_series ELSE NULL END, generate_series
    FROM TABLE(generate_series(1, 2000));
INSERT INTO ${case_db}.t_dnn_ar
    SELECT CASE WHEN generate_series % 12 = 0 THEN generate_series ELSE NULL END
    FROM TABLE(generate_series(1, 2000));
ANALYZE TABLE ${case_db}.t_dnn_al;
ANALYZE TABLE ${case_db}.t_dnn_ar;
EXPLAIN VERBOSE
SELECT l.v
FROM ${case_db}.t_dnn_al l
LEFT ANTI JOIN ${case_db}.t_dnn_ar r ON l.k = r.k;
