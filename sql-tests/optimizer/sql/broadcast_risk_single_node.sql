-- @tags=optimizer,bc1,distribution
CREATE DATABASE IF NOT EXISTS ${case_db};
USE ${case_db};
SET cbo_broadcast_backend_count = 1;
EXPLAIN COSTS
WITH sp AS (
    SELECT generate_series AS k
    FROM TABLE(generate_series(1, 1000000))
),
sb AS (
    SELECT generate_series AS k
    FROM TABLE(generate_series(1, 10000))
)
SELECT COUNT(*) AS cnt
FROM sp p JOIN sb b ON p.k = b.k;
