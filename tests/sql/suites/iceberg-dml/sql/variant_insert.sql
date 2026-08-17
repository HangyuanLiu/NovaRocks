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
-- Test Point: INSERT into a v3 iceberg table with a VARIANT column
-- round-trips through parquet write + read.
-- Method: CREATE … (id INT, v VARIANT) USING iceberg WITH ("format-version"="3"),
--         INSERT VALUES with parse_json, SELECT id, v.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_v3_variant FORCE;
CREATE TABLE ${case_db}.t_v3_variant (
  id INT,
  v VARIANT
)
TBLPROPERTIES (
  "format-version" = "3"
);
INSERT INTO ${case_db}.t_v3_variant VALUES
  (1, parse_json('{"a":1,"b":"x"}')),
  (2, parse_json('[10, 20, 30]')),
  (3, parse_json('null')),
  (4, NULL);

-- query 2 — round-trip through parquet: assert each row's variant
-- payload decodes to the expected logical type. Selecting `v` directly
-- would emit raw [size|metadata|value] bytes whose embedded \n breaks
-- the MySQL row delimiter, so we use variant_typeof for a stable display.
SELECT id, variant_typeof(v) FROM ${case_db}.t_v3_variant ORDER BY id;

-- query 3 — concrete payload probe via the JSON-path accessor.
SELECT id, get_json_string(v, '$.b') FROM ${case_db}.t_v3_variant WHERE id = 1;

-- query 4
-- @skip_result_check=true
DROP TABLE ${case_db}.t_v3_variant FORCE;
