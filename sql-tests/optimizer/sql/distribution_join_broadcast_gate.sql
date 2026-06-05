-- @tags=optimizer,oq8,distribution
DROP TABLE IF EXISTS ${case_db}.oq8_probe_big;
DROP TABLE IF EXISTS ${case_db}.oq8_build_big;
CREATE TABLE ${case_db}.oq8_probe_big (k INT, v INT);
CREATE TABLE ${case_db}.oq8_build_big (k INT, v INT);
INSERT INTO ${case_db}.oq8_probe_big
    SELECT generate_series, generate_series
    FROM TABLE(generate_series(1, 1000));
INSERT INTO ${case_db}.oq8_build_big
    SELECT generate_series, generate_series
    FROM TABLE(generate_series(1, 1000));

-- @explain_contains=HASH JOIN (PARTITIONED
-- @explain_not_contains=HASH JOIN (BROADCAST
SELECT COUNT(*) AS cnt
FROM ${case_db}.oq8_probe_big p
INNER JOIN ${case_db}.oq8_build_big b ON p.k = b.k;
