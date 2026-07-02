-- @tags=optimizer,oq10,runtime_filter
-- Test Objective:
-- LEFT SEMI builds runtime filters now that the complete-only lifecycle and
-- presence-only build-key collection make them safe; LEFT ANTI stays RF-free.
-- The reversed ON predicate still preserves join shape.
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
-- @explain_contains=probe runtime filters:
SELECT count(*)
FROM ${case_db}.rf_side_l l
LEFT SEMI JOIN ${case_db}.rf_side_r r ON r.k = l.k;

-- @explain_contains=HASH JOIN (BROADCAST, LEFT ANTI
-- @explain_not_contains=build runtime filters:
-- @explain_not_contains=probe runtime filters:
SELECT count(*)
FROM ${case_db}.rf_side_l l
LEFT ANTI JOIN ${case_db}.rf_side_r r ON r.k = l.k;
