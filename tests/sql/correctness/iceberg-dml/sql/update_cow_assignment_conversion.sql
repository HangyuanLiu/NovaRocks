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
-- @tags=iceberg_dml,cow,cast
-- Compare COW assignment conversion with the same SELECT CAST value author.
DROP TABLE IF EXISTS ${case_db}.t_cow_assignment_conversion FORCE;
CREATE TABLE ${case_db}.t_cow_assignment_conversion (
  id INT,
  d DECIMAL(8,2),
  dt DATE,
  ts DATETIME,
  n INT,
  nullable_text STRING,
  required_text STRING NOT NULL
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true",
  "write.update.mode" = "copy-on-write"
);
INSERT INTO ${case_db}.t_cow_assignment_conversion VALUES
  (1, 0, '2020-01-01', '2020-01-01 00:00:00', 0, 'before', 'required'),
  (2, 0, '2020-01-01', '2020-01-01 00:00:00', 0, 'before', 'required');

SELECT CAST(CAST(12.345 AS DECIMAL(8,3)) AS DECIMAL(8,2)) AS d,
       CAST('2024-02-29' AS DATE) AS dt,
       CAST('2024-02-29 12:34:56.123456' AS DATETIME) AS ts,
       CAST('123' AS INT) AS n,
       CAST(NULL AS STRING) AS nullable_text;
UPDATE ${case_db}.t_cow_assignment_conversion
SET d = CAST(12.345 AS DECIMAL(8,3)),
    dt = '2024-02-29',
    ts = '2024-02-29 12:34:56.123456',
    n = '123',
    nullable_text = NULL
WHERE id = 1;
SELECT d, dt, ts, n, nullable_text
FROM ${case_db}.t_cow_assignment_conversion WHERE id = 1;

SELECT CAST(CAST(-12.345 AS DECIMAL(8,3)) AS DECIMAL(8,2)) AS d,
       CAST(CAST(2147483648 AS BIGINT) AS INT) AS n;
UPDATE ${case_db}.t_cow_assignment_conversion
SET d = CAST(-12.345 AS DECIMAL(8,3)), n = CAST(2147483648 AS BIGINT)
WHERE id = 2;
SELECT d, n FROM ${case_db}.t_cow_assignment_conversion WHERE id = 2;

SELECT CAST(NULL AS STRING) AS required_text;
-- @expect_error=signed COW required field contains NULL
UPDATE ${case_db}.t_cow_assignment_conversion SET required_text = NULL WHERE id = 1;
SELECT id, required_text FROM ${case_db}.t_cow_assignment_conversion ORDER BY id;
DROP TABLE ${case_db}.t_cow_assignment_conversion FORCE;
