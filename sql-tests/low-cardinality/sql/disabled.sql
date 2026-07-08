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

-- @tags=low-cardinality,dictionary,disable
-- Verify the retired standalone rewrite no longer needs a session disable rule:
-- fresh ANALYZE FULL plans stay free of native dictionary plan nodes.
CREATE TABLE ${case_db}.dict_disabled_t (
  k INT,
  s STRING
) TBLPROPERTIES ("format-version" = "3");
INSERT INTO ${case_db}.dict_disabled_t VALUES (1, 'a'), (2, 'b'), (3, 'a');
ANALYZE FULL TABLE ${case_db}.dict_disabled_t;
-- @explain_not_contains=DECODE
-- @explain_not_contains=dict=[
-- @skip_result_check=true
SELECT DISTINCT s FROM ${case_db}.dict_disabled_t;
