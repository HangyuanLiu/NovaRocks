-- @tags=optimizer,topn,sort,compactness
-- Test Objective:
-- Lock in redundant Sort elision under TopN and the rule-disable escape hatch.

-- query 1
-- @explain_contains=TOP-N
EXPLAIN VERBOSE SELECT id, score
FROM (
    SELECT id, score
    FROM (
        SELECT 1 AS id, 10 AS score
        UNION ALL SELECT 2 AS id, 20 AS score
        UNION ALL SELECT 3 AS id, 30 AS score
    ) t
    ORDER BY score DESC, id ASC
) sorted_t
ORDER BY score DESC, id ASC
LIMIT 2;

-- query 2
SET disable_optimizer_rules = 'RemoveRedundantSortUnderTopN';

-- query 3
-- @explain_contains=SORT
EXPLAIN VERBOSE SELECT id, score
FROM (
    SELECT id, score
    FROM (
        SELECT 1 AS id, 10 AS score
        UNION ALL SELECT 2 AS id, 20 AS score
        UNION ALL SELECT 3 AS id, 30 AS score
    ) t
    ORDER BY score DESC, id ASC
) sorted_t
ORDER BY score DESC, id ASC
LIMIT 2;

-- query 4
SET disable_optimizer_rules = '';
