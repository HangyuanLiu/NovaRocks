-- @tags=optimizer,oq10,runtime_filter
-- Test Objective:
-- Runtime filters remap probe expressions through projection aliases and keep
-- conservative set-operation plan coverage.
DROP TABLE IF EXISTS ${case_db}.rf_remap_a;
DROP TABLE IF EXISTS ${case_db}.rf_remap_b;
DROP TABLE IF EXISTS ${case_db}.rf_remap_c;
CREATE TABLE ${case_db}.rf_remap_a (k INT, v INT);
CREATE TABLE ${case_db}.rf_remap_b (k INT, v INT);
CREATE TABLE ${case_db}.rf_remap_c (k INT, v INT);
INSERT INTO ${case_db}.rf_remap_a
    SELECT generate_series, generate_series
    FROM TABLE(generate_series(1, 100000));
INSERT INTO ${case_db}.rf_remap_b VALUES (1, 10), (2, 20), (3, 30);
INSERT INTO ${case_db}.rf_remap_c VALUES (4, 40), (5, 50), (6, 60);
ANALYZE TABLE ${case_db}.rf_remap_a;
ANALYZE TABLE ${case_db}.rf_remap_b;
ANALYZE TABLE ${case_db}.rf_remap_c;

-- @explain_contains=HASH JOIN (BROADCAST, LEFT SEMI
-- @explain_not_contains=build runtime filters:
-- @explain_not_contains=probe runtime filters:
SELECT count(*)
FROM (
    SELECT k AS ak, v
    FROM ${case_db}.rf_remap_a
) pa
LEFT SEMI JOIN ${case_db}.rf_remap_b b ON pa.ak = b.k;

-- @explain_contains=UNION ALL
-- @explain_contains=build runtime filters:
SELECT count(*)
FROM (
    SELECT k
    FROM ${case_db}.rf_remap_a
    UNION ALL
    SELECT k
    FROM ${case_db}.rf_remap_c
) u
JOIN ${case_db}.rf_remap_b b ON u.k = b.k;
