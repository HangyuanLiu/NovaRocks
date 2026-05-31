-- @tags=optimizer,derive_join_not_null,session_rule_disable
-- Test Objective:
-- SET disable_optimizer_rules='DeriveJoinNotNullPredicate' suppresses the
-- derivation. The two EXPLAIN VERBOSE outputs around the SET must differ:
-- first has IS NOT NULL on the scans, second does not.
DROP TABLE IF EXISTS ${case_db}.t_dnn_dl;
DROP TABLE IF EXISTS ${case_db}.t_dnn_dr;
CREATE TABLE ${case_db}.t_dnn_dl (k INT, v INT);
CREATE TABLE ${case_db}.t_dnn_dr (k INT, v INT);
INSERT INTO ${case_db}.t_dnn_dl
    SELECT CASE WHEN generate_series % 12 = 0 THEN generate_series ELSE NULL END, generate_series
    FROM TABLE(generate_series(1, 2000));
INSERT INTO ${case_db}.t_dnn_dr
    SELECT CASE WHEN generate_series % 12 = 0 THEN generate_series ELSE NULL END, generate_series
    FROM TABLE(generate_series(1, 2000));

EXPLAIN VERBOSE
SELECT l.v, r.v
FROM ${case_db}.t_dnn_dl l
INNER JOIN ${case_db}.t_dnn_dr r ON l.k = r.k;

SET disable_optimizer_rules = 'DeriveJoinNotNullPredicate';

EXPLAIN VERBOSE
SELECT l.v, r.v
FROM ${case_db}.t_dnn_dl l
INNER JOIN ${case_db}.t_dnn_dr r ON l.k = r.k;

SET disable_optimizer_rules = '';
