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
ANALYZE TABLE probe_5m_wide;
ANALYZE TABLE build_500k;
SET cbo_broadcast_node_mem_budget_bytes = 268435456;
-- @explain_contains=HASH JOIN (BROADCAST
-- @explain_contains=bcast_verdict=feasible
-- @explain_not_contains=PARTITIONED, INNER
SELECT COUNT(p.pad1) AS cnt
FROM probe_5m_wide p JOIN build_500k b ON p.k = b.k;

EXPLAIN COSTS
SELECT COUNT(p.pad1) AS cnt
FROM probe_5m_wide p JOIN build_500k b ON p.k = b.k;
