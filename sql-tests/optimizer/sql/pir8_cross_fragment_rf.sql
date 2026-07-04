-- @tags=pir8,optimizer,runtime_filter,distributed
-- PIR-8 M3 guard: a partitioned hash join must keep the runtime-filter probe
-- target visible across the shuffle exchange after planner/codegen layering
-- guards are tightened.

CREATE TABLE ${case_db}.pir8_rf_probe (k INT, v INT);
CREATE TABLE ${case_db}.pir8_rf_build (k INT, v INT);

INSERT INTO ${case_db}.pir8_rf_probe VALUES
  (1, 1),
  (2, 2),
  (3, 3),
  (4, 4);
INSERT INTO ${case_db}.pir8_rf_build VALUES
  (1, 10),
  (2, 20),
  (3, 30),
  (4, 40);

ANALYZE TABLE ${case_db}.pir8_rf_probe;
ANALYZE TABLE ${case_db}.pir8_rf_build;

SET global_runtime_filter_build_max_size = 10737418240;
SET global_runtime_filter_probe_min_selectivity = 0.0;
SET cbo_broadcast_node_mem_budget_bytes = 0;

-- @explain_contains=HASH JOIN (PARTITIONED, INNER
-- @explain_contains=PARTITION: HASH_PARTITIONED (k)
-- @explain_contains=build runtime filters:
-- @explain_contains=probe runtime filters:
-- @explain_contains=probe_expr = (p.k)
-- @explain_not_contains=HASH JOIN (BROADCAST
SELECT count(*) AS cnt
FROM ${case_db}.pir8_rf_probe p
JOIN ${case_db}.pir8_rf_build b ON p.k = b.k;

EXPLAIN VERBOSE
SELECT count(*) AS cnt
FROM ${case_db}.pir8_rf_probe p
JOIN ${case_db}.pir8_rf_build b ON p.k = b.k;
