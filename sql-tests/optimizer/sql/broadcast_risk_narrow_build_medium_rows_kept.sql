-- @tags=optimizer,bc1,distribution
CREATE DATABASE IF NOT EXISTS ${case_db};
USE ${case_db};
CREATE TABLE probe_5m (k INT, pad1 VARCHAR(100), pad2 VARCHAR(100));
-- The unit-level q9 canary covers the 2M-row arithmetic. This SQL golden uses
-- a 500k narrow build so the all-in-one optimizer fixture keeps BROADCAST under
-- be=3 with the suite's actual persisted column-width statistics.
CREATE TABLE build_500k_int (k INT);
INSERT INTO probe_5m
    SELECT generate_series, repeat('x', 100), repeat('y', 100)
    FROM TABLE(generate_series(1, 5000000));
INSERT INTO build_500k_int
    SELECT generate_series FROM TABLE(generate_series(1, 500000));
ANALYZE TABLE probe_5m;
ANALYZE TABLE build_500k_int;
SET cbo_broadcast_backend_count = 3;
SET cbo_broadcast_node_mem_budget_bytes = 268435456;
-- @explain_contains=HASH JOIN (BROADCAST
-- @explain_contains=bcast_verdict=feasible
-- @explain_not_contains=PARTITIONED, INNER
SELECT COUNT(p.pad1) AS c1, COUNT(p.pad2) AS c2
FROM probe_5m p JOIN build_500k_int b ON p.k = b.k;

EXPLAIN VERBOSE
SELECT COUNT(p.pad1) AS c1, COUNT(p.pad2) AS c2
FROM probe_5m p JOIN build_500k_int b ON p.k = b.k;
