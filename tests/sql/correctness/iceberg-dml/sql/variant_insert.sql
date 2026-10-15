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
-- Variant writes fail before staging; scalar expressions retain their value domains.
-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_v3_variant FORCE;
CREATE TABLE ${case_db}.t_v3_variant (id INT, v VARIANT)
TBLPROPERTIES ("format-version" = "3");

-- query 2
-- @expect_error_tier=drift
-- @expect_error=Iceberg Variant writes are not supported
INSERT INTO ${case_db}.t_v3_variant VALUES
  (1, parse_json('{"a":1,"b":"x"}')),
  (2, parse_json('[10,20,30]')),
  (3, parse_json('null')),
  (4, NULL);

-- query 3
SELECT variant_typeof(parse_json('{"a":1}')) AS object_kind,
       variant_typeof(parse_json('[10,20,30]')) AS array_kind,
       variant_typeof(parse_json('null')) AS variant_null_kind,
       variant_typeof(CAST(NULL AS VARIANT)) AS sql_null_kind,
       parse_json('null') IS NULL AS variant_is_sql_null,
       CAST(NULL AS VARIANT) IS NULL AS sql_is_null;

-- query 4
SELECT get_json_string(parse_json('{"a":1,"b":"x"}'), '$.b') AS b;

-- query 5
SELECT COUNT(*) AS remaining_rows FROM ${case_db}.t_v3_variant;

-- query 6
-- @skip_result_check=true
DROP TABLE ${case_db}.t_v3_variant FORCE;
