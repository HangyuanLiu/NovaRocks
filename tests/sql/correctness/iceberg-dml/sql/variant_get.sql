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
-- Test Point: Spark-aligned variant_get / try_variant_get over constant expressions and an empty v3 iceberg
-- variant column: typed extraction, 2-arg variant return, try_ cast-failure
-- NULL, strict cast-failure error, missing-path NULL, WHERE usage, and
-- VariantPathPushdown enabled/disabled result equivalence.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_variant_get FORCE;
CREATE TABLE ${case_db}.t_variant_get (
  id INT,
  v VARIANT
)
TBLPROPERTIES (
  "format-version" = "3"
);

-- query 2
-- typed extraction; missing path / SQL NULL / unconvertible-cast
-- rows are NULL; numeric narrowing (1.5) truncates per Spark CAST semantics
-- (IV3-6 cast-semantics decision 2026-06-11).
WITH source_values AS (
  SELECT 1 AS id, parse_json('{"a":1,"b":"x"}') AS v
  UNION ALL SELECT 2, parse_json('{"a":99,"b":"y"}')
  UNION ALL SELECT 3, parse_json('{"b":"no-a"}')
  UNION ALL SELECT 4, parse_json('{"a":1.5}')
  UNION ALL SELECT 5, CAST(NULL AS VARIANT)
  UNION ALL SELECT 6, parse_json('{"a":"abc"}')
)
SELECT id, try_variant_get(v, '$.a', 'bigint') FROM source_values ORDER BY id;

-- query 3
-- strict extraction; row 4 truncates (1.5 -> 1), not an error.
WITH source_values AS (
  SELECT 1 AS id, parse_json('{"a":1,"b":"x"}') AS v
  UNION ALL SELECT 2, parse_json('{"a":99,"b":"y"}')
  UNION ALL SELECT 3, parse_json('{"b":"no-a"}')
  UNION ALL SELECT 4, parse_json('{"a":1.5}')
  UNION ALL SELECT 5, CAST(NULL AS VARIANT)
  UNION ALL SELECT 6, parse_json('{"a":"abc"}')
)
SELECT id, variant_get(v, '$.a', 'bigint') FROM source_values WHERE id <= 4 ORDER BY id;

-- query 4
-- strict extraction over a genuinely unconvertible row must fail.
-- @expect_error=cast
WITH source_values AS (
  SELECT 1 AS id, parse_json('{"a":1,"b":"x"}') AS v
  UNION ALL SELECT 2, parse_json('{"a":99,"b":"y"}')
  UNION ALL SELECT 3, parse_json('{"b":"no-a"}')
  UNION ALL SELECT 4, parse_json('{"a":1.5}')
  UNION ALL SELECT 5, CAST(NULL AS VARIANT)
  UNION ALL SELECT 6, parse_json('{"a":"abc"}')
)
SELECT variant_get(v, '$.a', 'bigint') FROM source_values WHERE id = 6;

-- query 5
-- WHERE usage through the PR-4 pushdown target shape.
WITH source_values AS (
  SELECT 1 AS id, parse_json('{"a":1,"b":"x"}') AS v
  UNION ALL SELECT 2, parse_json('{"a":99,"b":"y"}')
  UNION ALL SELECT 3, parse_json('{"b":"no-a"}')
  UNION ALL SELECT 4, parse_json('{"a":1.5}')
  UNION ALL SELECT 5, CAST(NULL AS VARIANT)
  UNION ALL SELECT 6, parse_json('{"a":"abc"}')
)
SELECT id FROM source_values WHERE try_variant_get(v, '$.a', 'bigint') = 99 ORDER BY id;

-- query 6
SET disable_optimizer_rules = 'VariantPathPushdown';

-- query 7
-- Same result with the rewrite disabled.
WITH source_values AS (
  SELECT 1 AS id, parse_json('{"a":1,"b":"x"}') AS v
  UNION ALL SELECT 2, parse_json('{"a":99,"b":"y"}')
  UNION ALL SELECT 3, parse_json('{"b":"no-a"}')
  UNION ALL SELECT 4, parse_json('{"a":1.5}')
  UNION ALL SELECT 5, CAST(NULL AS VARIANT)
  UNION ALL SELECT 6, parse_json('{"a":"abc"}')
)
SELECT id FROM source_values WHERE try_variant_get(v, '$.a', 'bigint') = 99 ORDER BY id;

-- query 8
SET disable_optimizer_rules = '';

-- query 9
-- 2-arg form returns variant; display via variant_typeof.
WITH source_values AS (
  SELECT 1 AS id, parse_json('{"a":1,"b":"x"}') AS v
  UNION ALL SELECT 2, parse_json('{"a":99,"b":"y"}')
  UNION ALL SELECT 3, parse_json('{"b":"no-a"}')
  UNION ALL SELECT 4, parse_json('{"a":1.5}')
  UNION ALL SELECT 5, CAST(NULL AS VARIANT)
  UNION ALL SELECT 6, parse_json('{"a":"abc"}')
)
SELECT id, variant_typeof(variant_get(v, '$.a')) FROM source_values WHERE id <= 2 ORDER BY id;

-- query 10
-- try_ predicate usage across missing-path / NULL rows.
WITH source_values AS (
  SELECT 1 AS id, parse_json('{"a":1,"b":"x"}') AS v
  UNION ALL SELECT 2, parse_json('{"a":99,"b":"y"}')
  UNION ALL SELECT 3, parse_json('{"b":"no-a"}')
  UNION ALL SELECT 4, parse_json('{"a":1.5}')
  UNION ALL SELECT 5, CAST(NULL AS VARIANT)
  UNION ALL SELECT 6, parse_json('{"a":"abc"}')
)
SELECT id FROM source_values WHERE try_variant_get(v, '$.a', 'bigint') > 5 ORDER BY id;

-- query 11
-- string extraction.
WITH source_values AS (
  SELECT 1 AS id, parse_json('{"a":1,"b":"x"}') AS v
  UNION ALL SELECT 2, parse_json('{"a":99,"b":"y"}')
  UNION ALL SELECT 3, parse_json('{"b":"no-a"}')
  UNION ALL SELECT 4, parse_json('{"a":1.5}')
  UNION ALL SELECT 5, CAST(NULL AS VARIANT)
  UNION ALL SELECT 6, parse_json('{"a":"abc"}')
)
SELECT id, variant_get(v, '$.b', 'string') FROM source_values WHERE id <= 3 ORDER BY id;

-- query 12
-- @skip_result_check=true
-- @explain_contains=variant columns:
-- @explain_contains=try_variant_get(v, '$.a', 'bigint')
SELECT id FROM ${case_db}.t_variant_get WHERE try_variant_get(v, '$.a', 'bigint') = 99;

-- query 13
SET disable_optimizer_rules = 'VariantPathPushdown';

-- query 14
-- @skip_result_check=true
-- @explain_not_contains=variant columns:
SELECT id FROM ${case_db}.t_variant_get WHERE try_variant_get(v, '$.a', 'bigint') = 99;

-- query 15
SET disable_optimizer_rules = '';

-- query 16
-- @skip_result_check=true
DROP TABLE ${case_db}.t_variant_get FORCE;
