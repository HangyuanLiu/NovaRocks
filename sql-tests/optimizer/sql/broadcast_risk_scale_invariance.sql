-- @tags=optimizer,bc1,distribution
CREATE DATABASE IF NOT EXISTS ${case_db};
USE ${case_db};
CREATE TABLE probe_1x (k INT);
CREATE TABLE build_1x (k INT);
CREATE TABLE probe_10x (k INT);
CREATE TABLE build_10x (k INT);
INSERT INTO probe_1x
    SELECT generate_series FROM TABLE(generate_series(1, 100000));
INSERT INTO build_1x
    SELECT generate_series FROM TABLE(generate_series(1, 10000));
INSERT INTO probe_10x
    SELECT generate_series FROM TABLE(generate_series(1, 1000000));
INSERT INTO build_10x
    SELECT generate_series FROM TABLE(generate_series(1, 100000));
ANALYZE TABLE probe_1x;
ANALYZE TABLE build_1x;
ANALYZE TABLE probe_10x;
ANALYZE TABLE build_10x;
SET cbo_broadcast_backend_count = 3;
SET cbo_broadcast_node_mem_budget_bytes = 268435456;
-- @explain_contains=HASH JOIN (BROADCAST
SELECT COUNT(*) AS cnt FROM probe_1x p JOIN build_1x b ON p.k = b.k;

EXPLAIN VERBOSE
SELECT COUNT(*) AS cnt FROM probe_1x p JOIN build_1x b ON p.k = b.k;

-- @explain_contains=HASH JOIN (BROADCAST
SELECT COUNT(*) AS cnt FROM probe_10x p JOIN build_10x b ON p.k = b.k;

EXPLAIN VERBOSE
SELECT COUNT(*) AS cnt FROM probe_10x p JOIN build_10x b ON p.k = b.k;
