-- @tags=optimizer,derive_join_not_null
-- Test Objective:
-- LEFT SEMI JOIN on nullable keys derives IS NOT NULL on the RIGHT (build)
-- side only; the left (probe) side is unchanged (StarRocks-faithful).
DROP TABLE IF EXISTS ${case_db}.t_dnn_sl;
DROP TABLE IF EXISTS ${case_db}.t_dnn_sr;
CREATE TABLE ${case_db}.t_dnn_sl (k INT, v INT);
CREATE TABLE ${case_db}.t_dnn_sr (k INT);
INSERT INTO ${case_db}.t_dnn_sl
    SELECT CASE WHEN generate_series % 12 = 0 THEN generate_series ELSE NULL END, generate_series
    FROM TABLE(generate_series(1, 2000));
INSERT INTO ${case_db}.t_dnn_sr
    SELECT CASE WHEN generate_series % 12 = 0 THEN generate_series ELSE NULL END
    FROM TABLE(generate_series(1, 2000));
-- @explain_contains=IS NOT NULL
EXPLAIN VERBOSE SELECT l.v
FROM ${case_db}.t_dnn_sl l
LEFT SEMI JOIN ${case_db}.t_dnn_sr r ON l.k = r.k;
