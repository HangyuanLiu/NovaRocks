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

-- @tags=low-cardinality,dictionary,rewrite
-- Verify ANALYZE FULL keeps standalone SQL on plain string plan shape while
-- GROUP BY results remain correct over dictionary metadata.
CREATE TABLE ${case_db}.dict_rewrite_t (
  k INT,
  s STRING,
  v INT
) TBLPROPERTIES ("format-version" = "3");
INSERT INTO ${case_db}.dict_rewrite_t VALUES
  (1, 'a', 10), (2, 'b', 20), (3, 'a', 30), (4, 'c', 40);
ANALYZE FULL TABLE ${case_db}.dict_rewrite_t;
-- @explain_not_contains=DECODE
-- @explain_not_contains=dict=[
SELECT s, SUM(v) FROM ${case_db}.dict_rewrite_t GROUP BY s ORDER BY s;
