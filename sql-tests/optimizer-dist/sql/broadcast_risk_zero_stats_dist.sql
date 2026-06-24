-- @tags=optimizer,bc1,distribution,dist-only
CREATE DATABASE IF NOT EXISTS ${case_db};
USE ${case_db};
SET disable_optimizer_rules = 'JoinReorder,JoinCommutativity';
EXPLAIN VERBOSE
WITH big_probe AS (
    SELECT generate_series AS k
    FROM TABLE(generate_series(1, 1000000))
),
no_stats AS (
    SELECT k
    FROM (
        SELECT generate_series + 0 AS k
        FROM TABLE(generate_series(1, 100000000))
    ) projected
    WHERE k > 0
)
SELECT COUNT(*) AS cnt
FROM big_probe p JOIN no_stats b ON p.k = b.k;
