-- @order_sensitive=true
-- @tags=runtime_filter,cross_process,distributed,self-subtree,topn
-- Validate that local TopN-over-preaggregate pruning publishes a self-subtree
-- runtime filter, the scan re-polls it late, and the filter does not enter the
-- remote runtime-filter schedule.

CREATE TABLE ${case_db}.rf_self_topn_local (
    k INT,
    payload INT
)
TBLPROPERTIES ("format-version" = "3");

INSERT INTO ${case_db}.rf_self_topn_local
SELECT generate_series % 100 AS k, generate_series AS payload
FROM TABLE(generate_series(1, 4096));

INSERT INTO ${case_db}.rf_self_topn_local
SELECT generate_series % 100 AS k, generate_series AS payload
FROM TABLE(generate_series(1, 4096));

INSERT INTO ${case_db}.rf_self_topn_local
SELECT generate_series % 100 AS k, generate_series AS payload
FROM TABLE(generate_series(1, 4096));

INSERT INTO ${case_db}.rf_self_topn_local
SELECT generate_series % 100 AS k, generate_series AS payload
FROM TABLE(generate_series(1, 4096));

INSERT INTO ${case_db}.rf_self_topn_local
SELECT generate_series % 100 AS k, generate_series AS payload
FROM TABLE(generate_series(1, 4096));

INSERT INTO ${case_db}.rf_self_topn_local
SELECT generate_series % 100 AS k, generate_series AS payload
FROM TABLE(generate_series(1, 4096));

INSERT INTO ${case_db}.rf_self_topn_local
SELECT 10000 + generate_series AS k, generate_series AS payload
FROM TABLE(generate_series(1, 8192));

INSERT INTO ${case_db}.rf_self_topn_local
SELECT 20000 + generate_series AS k, generate_series AS payload
FROM TABLE(generate_series(1, 8192));

INSERT INTO ${case_db}.rf_self_topn_local
SELECT 30000 + generate_series AS k, generate_series AS payload
FROM TABLE(generate_series(1, 8192));

INSERT INTO ${case_db}.rf_self_topn_local
SELECT 40000 + generate_series AS k, generate_series AS payload
FROM TABLE(generate_series(1, 8192));

INSERT INTO ${case_db}.rf_self_topn_local
SELECT 50000 + generate_series AS k, generate_series AS payload
FROM TABLE(generate_series(1, 8192));

INSERT INTO ${case_db}.rf_self_topn_local
SELECT 60000 + generate_series AS k, generate_series AS payload
FROM TABLE(generate_series(1, 8192));

INSERT INTO ${case_db}.rf_self_topn_local
SELECT 70000 + generate_series AS k, generate_series AS payload
FROM TABLE(generate_series(1, 8192));

INSERT INTO ${case_db}.rf_self_topn_local
SELECT 80000 + generate_series AS k, generate_series AS payload
FROM TABLE(generate_series(1, 8192));

INSERT INTO ${case_db}.rf_self_topn_local
SELECT 90000 + generate_series AS k, generate_series AS payload
FROM TABLE(generate_series(1, 8192));

INSERT INTO ${case_db}.rf_self_topn_local
SELECT 100000 + generate_series AS k, generate_series AS payload
FROM TABLE(generate_series(1, 8192));

INSERT INTO ${case_db}.rf_self_topn_local
SELECT 110000 + generate_series AS k, generate_series AS payload
FROM TABLE(generate_series(1, 8192));

INSERT INTO ${case_db}.rf_self_topn_local
SELECT 120000 + generate_series AS k, generate_series AS payload
FROM TABLE(generate_series(1, 8192));

INSERT INTO ${case_db}.rf_self_topn_local
SELECT 130000 + generate_series AS k, generate_series AS payload
FROM TABLE(generate_series(1, 8192));

INSERT INTO ${case_db}.rf_self_topn_local
SELECT 140000 + generate_series AS k, generate_series AS payload
FROM TABLE(generate_series(1, 8192));

INSERT INTO ${case_db}.rf_self_topn_local
SELECT 150000 + generate_series AS k, generate_series AS payload
FROM TABLE(generate_series(1, 8192));

INSERT INTO ${case_db}.rf_self_topn_local
SELECT 160000 + generate_series AS k, generate_series AS payload
FROM TABLE(generate_series(1, 8192));

INSERT INTO ${case_db}.rf_self_topn_local
SELECT 170000 + generate_series AS k, generate_series AS payload
FROM TABLE(generate_series(1, 8192));

INSERT INTO ${case_db}.rf_self_topn_local
SELECT 180000 + generate_series AS k, generate_series AS payload
FROM TABLE(generate_series(1, 8192));

INSERT INTO ${case_db}.rf_self_topn_local
SELECT 190000 + generate_series AS k, generate_series AS payload
FROM TABLE(generate_series(1, 8192));

INSERT INTO ${case_db}.rf_self_topn_local
SELECT 200000 + generate_series AS k, generate_series AS payload
FROM TABLE(generate_series(1, 8192));

INSERT INTO ${case_db}.rf_self_topn_local
SELECT 210000 + generate_series AS k, generate_series AS payload
FROM TABLE(generate_series(1, 8192));

INSERT INTO ${case_db}.rf_self_topn_local
SELECT 220000 + generate_series AS k, generate_series AS payload
FROM TABLE(generate_series(1, 8192));

INSERT INTO ${case_db}.rf_self_topn_local
SELECT 230000 + generate_series AS k, generate_series AS payload
FROM TABLE(generate_series(1, 8192));

INSERT INTO ${case_db}.rf_self_topn_local
SELECT 240000 + generate_series AS k, generate_series AS payload
FROM TABLE(generate_series(1, 8192));

ANALYZE TABLE ${case_db}.rf_self_topn_local;

-- @explain_contains=LOCAL TOP-N
-- @explain_contains=build runtime filters:
-- @explain_contains=probe runtime filters:
-- @explain_contains=type = TOPN
SELECT k, COUNT(*) AS cnt, SUM(payload) AS payload_sum
FROM ${case_db}.rf_self_topn_local
GROUP BY k
ORDER BY k
LIMIT 5;

-- @skip_result_check=true
-- @result_contains=SelfSubtreeRuntimeFilterPruningCounters: SelfSubtreeRuntimeFilterPrunedRows=
EXPLAIN ANALYZE
SELECT k, COUNT(*) AS cnt, SUM(payload) AS payload_sum
FROM ${case_db}.rf_self_topn_local
GROUP BY k
ORDER BY k
LIMIT 5;
