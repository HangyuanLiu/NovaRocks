-- @tags=optimizer,topn,compactness
-- Test Objective:
-- Lock in TopN pushdown behavior for UNION ALL while preserving fail-closed
-- guards for Aggregate and Join.
DROP TABLE IF EXISTS ${case_db}.topn_compactness_left_src;
DROP TABLE IF EXISTS ${case_db}.topn_compactness_right_src;
CREATE TABLE ${case_db}.topn_compactness_left_src (id INT, score INT);
CREATE TABLE ${case_db}.topn_compactness_right_src (id INT, score INT);
INSERT INTO ${case_db}.topn_compactness_left_src
    SELECT generate_series, generate_series * 10
    FROM TABLE(generate_series(1, 3));
INSERT INTO ${case_db}.topn_compactness_right_src
    SELECT generate_series, generate_series * 10 + 5
    FROM TABLE(generate_series(2, 4));

EXPLAIN VERBOSE
SELECT id, score
FROM (
    SELECT id, score
    FROM ${case_db}.topn_compactness_left_src
    UNION ALL
    SELECT id, score
    FROM ${case_db}.topn_compactness_right_src
) u
ORDER BY score DESC, id ASC
LIMIT 2;

EXPLAIN VERBOSE
SELECT id, SUM(score) AS total_score
FROM (
    SELECT id, score
    FROM ${case_db}.topn_compactness_left_src
    UNION ALL
    SELECT id, score
    FROM ${case_db}.topn_compactness_right_src
) u
GROUP BY id
ORDER BY total_score DESC
LIMIT 1;

EXPLAIN VERBOSE
SELECT l.id, l.score, r.score AS rhs_score
FROM ${case_db}.topn_compactness_left_src l
INNER JOIN ${case_db}.topn_compactness_right_src r ON l.id = r.id
ORDER BY l.score DESC
LIMIT 1;
