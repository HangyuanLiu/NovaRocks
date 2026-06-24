-- @tags=optimizer,bc1,distribution,dist-only
CREATE DATABASE IF NOT EXISTS ${case_db};
USE ${case_db};
CREATE TABLE probe_1m (k INT);
CREATE TABLE build_fallback_risky (k INT, pad VARCHAR(250));
INSERT INTO probe_1m
    SELECT generate_series FROM TABLE(generate_series(1, 1000000));
INSERT INTO build_fallback_risky
    SELECT generate_series, repeat('w', 250) FROM TABLE(generate_series(1, 1000));
ANALYZE TABLE probe_1m;
SET cbo_broadcast_node_mem_budget_bytes = 65536;
-- @explain_contains=HASH JOIN (PARTITIONED
-- @explain_not_contains=BROADCAST, INNER
SELECT COUNT(*) AS cnt
FROM probe_1m p JOIN build_fallback_risky b ON p.k = b.k;

EXPLAIN COSTS
SELECT COUNT(*) AS cnt
FROM probe_1m p JOIN build_fallback_risky b ON p.k = b.k;
