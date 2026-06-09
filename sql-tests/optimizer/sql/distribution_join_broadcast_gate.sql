-- @tags=optimizer,oq8,distribution
-- With real iceberg stats a small build side joins via BROADCAST. Plan-shape
-- golden over iceberg base tables; broadcast-gate behavior at scale is covered
-- by the ssb/tpc-* benchmark suites.
DROP TABLE IF EXISTS ${case_db}.oq8_probe_big;
DROP TABLE IF EXISTS ${case_db}.oq8_build_big;
CREATE TABLE ${case_db}.oq8_probe_big (k INT, v INT);
CREATE TABLE ${case_db}.oq8_build_big (k INT, v INT);
INSERT INTO ${case_db}.oq8_probe_big
    SELECT generate_series, generate_series FROM TABLE(generate_series(1, 1000));
INSERT INTO ${case_db}.oq8_build_big
    SELECT generate_series, generate_series FROM TABLE(generate_series(1, 1000));
ANALYZE TABLE ${case_db}.oq8_probe_big;
ANALYZE TABLE ${case_db}.oq8_build_big;
EXPLAIN VERBOSE
SELECT COUNT(*) AS cnt
FROM ${case_db}.oq8_probe_big p
INNER JOIN ${case_db}.oq8_build_big b ON p.k = b.k;
