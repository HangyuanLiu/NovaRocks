-- @tags=optimizer,bc1,distribution,dist-only
CREATE DATABASE IF NOT EXISTS ${case_db};
USE ${case_db};
CREATE TABLE probe_1m_exact (k INT);
CREATE TABLE build_wide_unanalyzed (k INT, pad VARCHAR(500));
INSERT INTO probe_1m_exact
    SELECT generate_series FROM TABLE(generate_series(1, 1000000));
INSERT INTO build_wide_unanalyzed
    SELECT generate_series, repeat('z', 500) FROM TABLE(generate_series(1, 200000));
ANALYZE TABLE probe_1m_exact;
SET cbo_broadcast_node_mem_budget_bytes = 268435456;
-- @explain_contains=HASH JOIN (PARTITIONED
-- @explain_not_contains=BROADCAST, INNER
-- @explain_not_contains=memory=inf
SELECT COUNT(*) AS cnt FROM probe_1m_exact p JOIN build_wide_unanalyzed b ON p.k = b.k;

EXPLAIN VERBOSE
SELECT COUNT(*) AS cnt FROM probe_1m_exact p JOIN build_wide_unanalyzed b ON p.k = b.k;

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
