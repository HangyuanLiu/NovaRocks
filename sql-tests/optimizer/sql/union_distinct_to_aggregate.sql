-- @tags=optimizer,union_distinct,aggregate
DROP TABLE IF EXISTS ${case_db}.union_distinct_l;
DROP TABLE IF EXISTS ${case_db}.union_distinct_r;
CREATE TABLE ${case_db}.union_distinct_l (k INT, s VARCHAR);
CREATE TABLE ${case_db}.union_distinct_r (k INT, s VARCHAR);
INSERT INTO ${case_db}.union_distinct_l VALUES
    (1, 'a'),
    (1, 'a'),
    (2, NULL);
INSERT INTO ${case_db}.union_distinct_r VALUES
    (1, 'a'),
    (3, 'c'),
    (NULL, 'n');
ANALYZE TABLE ${case_db}.union_distinct_l;
ANALYZE TABLE ${case_db}.union_distinct_r;

-- @skip_result_check=true
-- @explain_contains=HASH AGGREGATE (LOCAL,
-- @explain_contains=HASH AGGREGATE (GLOBAL,
-- @explain_contains=UNION ALL
-- @explain_not_contains=UNION DISTINCT
SELECT k, s
FROM ${case_db}.union_distinct_l
UNION
SELECT k, s
FROM ${case_db}.union_distinct_r;

SELECT COUNT(*) AS distinct_rows
FROM (
    SELECT k, s
    FROM ${case_db}.union_distinct_l
    UNION
    SELECT k, s
    FROM ${case_db}.union_distinct_r
) u;
