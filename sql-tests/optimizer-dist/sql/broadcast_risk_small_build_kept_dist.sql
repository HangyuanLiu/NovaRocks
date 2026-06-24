-- @tags=optimizer,bc1,distribution,dist-only
CREATE DATABASE IF NOT EXISTS ${case_db};
USE ${case_db};
CREATE TABLE probe_1m (k INT, v BIGINT);
CREATE TABLE build_1000 (k INT);
INSERT INTO probe_1m
    SELECT generate_series, generate_series FROM TABLE(generate_series(1, 1000000));
INSERT INTO build_1000
    SELECT generate_series FROM TABLE(generate_series(1, 1000));
ANALYZE TABLE probe_1m;
ANALYZE TABLE build_1000;
SET cbo_broadcast_node_mem_budget_bytes = 268435456;
-- @explain_contains=HASH JOIN (BROADCAST
-- @explain_contains=bcast_verdict=feasible
SELECT COUNT(*) AS cnt FROM probe_1m p JOIN build_1000 b ON p.k = b.k;

EXPLAIN COSTS
WITH p AS (
    SELECT generate_series AS k
    FROM TABLE(generate_series(1, 1000))
),
b AS (
    SELECT generate_series AS k
    FROM TABLE(generate_series(1, 10))
)
SELECT COUNT(*) AS cnt
FROM p JOIN b ON p.k = b.k;
