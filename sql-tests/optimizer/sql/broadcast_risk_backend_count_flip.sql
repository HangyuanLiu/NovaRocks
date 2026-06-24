-- @tags=optimizer,bc1,distribution
CREATE DATABASE IF NOT EXISTS ${case_db};
USE ${case_db};
CREATE TABLE probe_flip (k INT, pad1 VARCHAR(100), pad2 VARCHAR(100));
CREATE TABLE build_flip (k INT);
INSERT INTO probe_flip
    SELECT generate_series, repeat('p', 100), repeat('q', 100)
    FROM TABLE(generate_series(1, 5000000));
INSERT INTO build_flip
    SELECT generate_series FROM TABLE(generate_series(1, 500000));
ANALYZE TABLE probe_flip;
ANALYZE TABLE build_flip;
-- Recorded sizing: 5M wide probe + 500k narrow build. be=2 keeps broadcast;
-- be=16 flips to partitioned from backend-scaled memory/network cost.
SET cbo_broadcast_node_mem_budget_bytes = 536870912;
SET cbo_broadcast_backend_count = 2;
-- @explain_contains=HASH JOIN (BROADCAST
-- @explain_contains=bcast_verdict=feasible
SELECT COUNT(p.pad1) AS c1, COUNT(p.pad2) AS c2
FROM probe_flip p JOIN build_flip b ON p.k = b.k;

EXPLAIN VERBOSE
SELECT COUNT(p.pad1) AS c1, COUNT(p.pad2) AS c2
FROM probe_flip p JOIN build_flip b ON p.k = b.k;

SET cbo_broadcast_backend_count = 16;
-- @explain_contains=HASH JOIN (PARTITIONED
-- @explain_not_contains=BROADCAST, INNER
SELECT COUNT(p.pad1) AS c1, COUNT(p.pad2) AS c2
FROM probe_flip p JOIN build_flip b ON p.k = b.k;

EXPLAIN VERBOSE
SELECT COUNT(p.pad1) AS c1, COUNT(p.pad2) AS c2
FROM probe_flip p JOIN build_flip b ON p.k = b.k;
