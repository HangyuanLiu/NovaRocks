-- @tags=optimizer,bc1,distribution
CREATE DATABASE IF NOT EXISTS ${case_db};
USE ${case_db};
SET cbo_broadcast_backend_count = 3;
SET disable_optimizer_rules = 'JoinReorder,JoinCommutativity';
EXPLAIN COSTS
WITH probe_1m AS (
    SELECT generate_series AS k
    FROM TABLE(generate_series(1, 1000000))
),
build_fallback_risky AS (
    SELECT generate_series AS k
    FROM TABLE(generate_series(1, 100000000))
)
SELECT COUNT(*) AS cnt
FROM probe_1m p JOIN build_fallback_risky b ON p.k = b.k;
