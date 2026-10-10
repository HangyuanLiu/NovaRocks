-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- @tags=optimizer,bc1,distribution,dist-only
CREATE DATABASE IF NOT EXISTS ${case_db};
USE ${case_db};
CREATE TABLE probe_5m_wide (k INT, pad1 VARCHAR(100), pad2 VARCHAR(100));
CREATE TABLE build_500k (k INT);
INSERT INTO probe_5m_wide
    SELECT generate_series, repeat('x', 100), repeat('y', 100)
    FROM TABLE(generate_series(1, 5000000));
INSERT INTO build_500k
    SELECT generate_series FROM TABLE(generate_series(1, 500000));
ANALYZE TABLE build_500k;
-- Wait for this case's own job. SHOW ANALYZE JOBS lists every job the
-- frontend holds, so whole-output checks would let another case's job satisfy
-- or fail this wait. The table is analyzed exactly once in this case database.
-- @retry_count=120
-- @retry_interval_ms=500
-- @result_rows_where=catalog=iceberg_opt
-- @result_rows_where=namespace=${case_db}
-- @result_rows_where=table=build_500k
-- @result_rows_count=1
-- @result_rows_expect=state=SUCCEEDED
-- @skip_result_check=true
SHOW ANALYZE JOBS;
SET cbo_broadcast_node_mem_budget_bytes = 268435456;
-- @explain_contains=HASH JOIN (BROADCAST
-- @explain_contains=bcast_verdict=feasible
-- @explain_not_contains=PARTITIONED, INNER
SELECT COUNT(p.pad1) AS cnt
FROM probe_5m_wide p JOIN build_500k b ON p.k = b.k;

-- Manifest column widths are measured compressed-file facts, not fixed SQL widths.
-- Bind table provenance and finite resource bounds to this same COSTS response.
-- @query_stats_contract={"tables":[{"table_suffix":".${case_db}.probe_5m_wide","rows":5000000,"confidence":"Exact","source":"IcebergManifest"},{"table_suffix":".${case_db}.build_500k","rows":500000,"confidence":"Exact","source":"IcebergManifest"}],"broadcast":{"distribution":"BROADCAST","join_kind":"INNER","verdict":"feasible","forced":false,"backends":3,"risk_multiplier":2,"per_node_budget_bytes":268435456,"cluster_network_budget_bytes":268435456},"payload":{"kind":"positive_range"},"hash_table":{"kind":"bounded","max_build_rows":500000,"load_factor":0.75,"per_row_overhead_bytes":16}}
-- @result_contains=HASH JOIN (BROADCAST, INNER
-- @result_contains=BROADCAST EXCHANGE
-- @result_contains=PARTITION: BROADCAST
-- @result_not_contains=HASH JOIN (PARTITIONED
-- @skip_result_check=true
EXPLAIN COSTS
SELECT COUNT(p.pad1) AS cnt
FROM probe_5m_wide p JOIN build_500k b ON p.k = b.k;
