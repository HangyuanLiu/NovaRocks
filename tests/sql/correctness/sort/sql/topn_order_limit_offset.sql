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
-- @tags=sort,limit,offset
-- Test Objective:
-- 1. Validate LIMIT with OFFSET under deterministic ordering.
-- 2. Prevent regressions in page slicing for ordered result sets.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert deterministic rows with descending sort semantics.
-- 3. Query with ORDER BY + LIMIT + OFFSET and assert output.
DROP TABLE IF EXISTS ${case_db}.t_topn_order_limit_offset;
CREATE TABLE ${case_db}.t_topn_order_limit_offset (
  id INT,
  val INT
)
TBLPROPERTIES ("format-version" = "3");
INSERT INTO ${case_db}.t_topn_order_limit_offset VALUES
  (1, 100),
  (2, 90),
  (3, 90),
  (4, 80),
  (5, 70);
SELECT id, val
FROM ${case_db}.t_topn_order_limit_offset
ORDER BY val DESC, id ASC
LIMIT 2 OFFSET 2;
