-- @tags=optimizer,union_distinct,aggregate,dist-only
DROP TABLE IF EXISTS ${case_db}.union_distinct_dist_l;
DROP TABLE IF EXISTS ${case_db}.union_distinct_dist_r;
CREATE TABLE ${case_db}.union_distinct_dist_l (k INT, s VARCHAR);
CREATE TABLE ${case_db}.union_distinct_dist_r (k INT, s VARCHAR);
INSERT INTO ${case_db}.union_distinct_dist_l VALUES
    (1, 'a'),
    (1, 'a'),
    (2, NULL);
INSERT INTO ${case_db}.union_distinct_dist_r VALUES
    (1, 'a'),
    (3, 'c'),
    (NULL, 'n');
ANALYZE TABLE ${case_db}.union_distinct_dist_l;
ANALYZE TABLE ${case_db}.union_distinct_dist_r;

-- @skip_result_check=true
-- @explain_contains=HASH AGGREGATE (LOCAL,
-- @explain_contains=HASH AGGREGATE (GLOBAL,
-- @explain_contains=UNION ALL
-- @explain_not_contains=UNION DISTINCT
SELECT k, s
FROM ${case_db}.union_distinct_dist_l
UNION
SELECT k, s
FROM ${case_db}.union_distinct_dist_r;

SELECT COUNT(*) AS distinct_rows
FROM (
    SELECT k, s
    FROM ${case_db}.union_distinct_dist_l
    UNION
    SELECT k, s
    FROM ${case_db}.union_distinct_dist_r
) u;
