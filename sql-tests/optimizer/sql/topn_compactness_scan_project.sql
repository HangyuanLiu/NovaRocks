-- @tags=optimizer,topn,compactness
-- Test Objective:
-- Lock in TopN pushdown through Project alias remapping and the scan-pushdown
-- guard that keeps the final TopN visible.
DROP TABLE IF EXISTS ${case_db}.topn_compactness_project_src;
CREATE TABLE ${case_db}.topn_compactness_project_src (id INT, score INT);
INSERT INTO ${case_db}.topn_compactness_project_src
    SELECT generate_series, generate_series * 10
    FROM TABLE(generate_series(1, 3));

EXPLAIN VERBOSE
SELECT alias_id, alias_score
FROM (
    SELECT id AS alias_id, score AS alias_score
    FROM ${case_db}.topn_compactness_project_src
) p
ORDER BY alias_score DESC, alias_id ASC
LIMIT 2;

SET disable_optimizer_rules = 'PushTopNIntoScan,PushTopNThroughProject';

EXPLAIN VERBOSE
SELECT alias_id, alias_score
FROM (
    SELECT id AS alias_id, score AS alias_score
    FROM ${case_db}.topn_compactness_project_src
) p
ORDER BY alias_score DESC, alias_id ASC
LIMIT 2;

SET disable_optimizer_rules = '';
