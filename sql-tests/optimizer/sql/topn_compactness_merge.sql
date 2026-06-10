-- @tags=optimizer,topn,compactness
-- Test Objective:
-- Lock in compact consecutive TopN planning and the rule-disable escape hatch.

-- query 1
-- @skip_result_check=true
-- @explain_contains=TOP-N
-- @explain_contains=stats={rows=
EXPLAIN VERBOSE SELECT *
FROM (
    SELECT id, score
    FROM (
        SELECT 1 AS id, 10 AS score
        UNION ALL SELECT 2 AS id, 20 AS score
        UNION ALL SELECT 3 AS id, 20 AS score
        UNION ALL SELECT 4 AS id, 5 AS score
    ) t
    ORDER BY score DESC, id ASC
    LIMIT 3
) s
ORDER BY score DESC, id ASC
LIMIT 2;

-- query 2
SET disable_optimizer_rules = 'MergeConsecutiveTopN';

-- query 3
-- @skip_result_check=true
-- @explain_contains=TOP-N
EXPLAIN VERBOSE SELECT *
FROM (
    SELECT id, score
    FROM (
        SELECT 1 AS id, 10 AS score
        UNION ALL SELECT 2 AS id, 20 AS score
        UNION ALL SELECT 3 AS id, 20 AS score
        UNION ALL SELECT 4 AS id, 5 AS score
    ) t
    ORDER BY score DESC, id ASC
    LIMIT 3
) s
ORDER BY score DESC, id ASC
LIMIT 2;

-- query 4
SET disable_optimizer_rules = '';
