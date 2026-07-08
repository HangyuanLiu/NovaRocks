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

-- @order_sensitive=true
-- @tags=join,multiway
-- Test Objective:
-- 1. Validate multi-join chain correctness across three tables.
-- 2. Prevent regressions in join key propagation between consecutive joins.
-- Test Flow:
-- 1. Create/reset three related tables.
-- 2. Insert deterministic keys for partial overlaps.
-- 3. Execute chained INNER JOINs and assert ordered output.
DROP TABLE IF EXISTS ${case_db}.t_join_chain_a;
DROP TABLE IF EXISTS ${case_db}.t_join_chain_b;
DROP TABLE IF EXISTS ${case_db}.t_join_chain_c;
CREATE TABLE ${case_db}.t_join_chain_a (
  id INT,
  av STRING
)
TBLPROPERTIES ("format-version" = "3");
CREATE TABLE ${case_db}.t_join_chain_b (
  id INT,
  mid INT
)
TBLPROPERTIES ("format-version" = "3");
CREATE TABLE ${case_db}.t_join_chain_c (
  mid INT,
  cv STRING
)
TBLPROPERTIES ("format-version" = "3");
INSERT INTO ${case_db}.t_join_chain_a VALUES
  (1, 'A1'),
  (2, 'A2'),
  (3, 'A3');
INSERT INTO ${case_db}.t_join_chain_b VALUES
  (1, 10),
  (2, 20),
  (4, 40);
INSERT INTO ${case_db}.t_join_chain_c VALUES
  (10, 'C10'),
  (20, 'C20'),
  (30, 'C30');
SELECT a.id, a.av, c.cv
FROM ${case_db}.t_join_chain_a a
INNER JOIN ${case_db}.t_join_chain_b b
  ON a.id = b.id
INNER JOIN ${case_db}.t_join_chain_c c
  ON b.mid = c.mid
ORDER BY a.id;
