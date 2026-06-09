-- @tags=optimizer,oq10,runtime_filter
-- Test Objective:
-- Runtime filters keep the LEFT SEMI join build/probe orientation even when
-- the ON predicate is written in reverse order. LEFT ANTI remains RF-free.
DROP TABLE IF EXISTS ${case_db}.rf_side_l;
DROP TABLE IF EXISTS ${case_db}.rf_side_r;
CREATE TABLE ${case_db}.rf_side_l (k INT, v INT);
CREATE TABLE ${case_db}.rf_side_r (k INT, v INT);
INSERT INTO ${case_db}.rf_side_l
    SELECT generate_series, generate_series
    FROM TABLE(generate_series(1, 100000));
INSERT INTO ${case_db}.rf_side_r VALUES (1, 10), (2, 20), (3, 30);
ANALYZE TABLE ${case_db}.rf_side_l;
ANALYZE TABLE ${case_db}.rf_side_r;

-- @explain_contains=HASH JOIN (BROADCAST, LEFT SEMI
-- @explain_contains=build runtime filters:
-- @explain_contains=build_expr = (r.k)
-- @explain_contains=probe runtime filters:
-- @explain_contains=probe_expr = (l.k)
SELECT count(*)
FROM ${case_db}.rf_side_l l
LEFT SEMI JOIN ${case_db}.rf_side_r r ON r.k = l.k;

-- @explain_contains=HASH JOIN (BROADCAST, LEFT ANTI
-- @explain_not_contains=build runtime filters:
SELECT count(*)
FROM ${case_db}.rf_side_l l
LEFT ANTI JOIN ${case_db}.rf_side_r r ON r.k = l.k;
