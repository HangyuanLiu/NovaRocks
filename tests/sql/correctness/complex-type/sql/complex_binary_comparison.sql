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

-- Test Objective:
-- 1. Validate binary comparison operators (=, !=, <=>) on complex types:
--    ARRAY, MAP, STRUCT (including nested, with NULLs).
-- 2. Cover WHERE clause filtering and SELECT expression evaluation.
-- 3. Validate type mismatch errors for incompatible operands.

-- Keep NULL Map keys in SQL computation. Persist only the seed fields that
-- Iceberg supports; the ordinal preserves each original input occurrence.
-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.sc2_seed (
  fixture_row_ordinal BIGINT NOT NULL,
  `v1` bigint(20) NULL COMMENT "",
  `array1` ARRAY<INT> NULL,
  `array3` ARRAY<STRUCT<a INT, b INT>> NULL,
  `map3` MAP<INT, STRUCT<c INT, b INT>> NULL
) TBLPROPERTIES ("format-version" = "3");

INSERT INTO ${case_db}.sc2_seed VALUES (1, 1, [11,NULL,31,41], [row(11, 12), row(12, 13)], map{101: row(NULL, 12)});
INSERT INTO ${case_db}.sc2_seed VALUES (2, 2, [12,22,32,42], [row(21, 22), NULL], map{202: row(21, 22)});
INSERT INTO ${case_db}.sc2_seed VALUES (3, 3, NULL, [row(31, 32), row(NULL, 33)], map{303: row(31, 32)});
INSERT INTO ${case_db}.sc2_seed VALUES (4, 4, [14,24,NULL,44], [row(41, 42), row(42, 43)], map{404: row(41, 42)});
INSERT INTO ${case_db}.sc2_seed VALUES (5, 5, [15,25,35,45], [row(51, 52), row(NULL, 53)], map{505: row(NULL, 52)});
INSERT INTO ${case_db}.sc2_seed VALUES (6, 1, [11,3,31,41], [row(11, 12), row(12, 13)], map{101: row(3, 12)});
INSERT INTO ${case_db}.sc2_seed VALUES (7, 2, [12,22,32,42], [row(21, 22), row(12, 13)], map{202: row(21, 22)});
INSERT INTO ${case_db}.sc2_seed VALUES (8, 3, [12,22,32,42], [row(31, 32), row(3, 33)], map{303: row(31, 32)});
INSERT INTO ${case_db}.sc2_seed VALUES (9, 4, [14,24,3,44], [row(41, 42), row(42, 43)], map{404: row(41, 42)});
INSERT INTO ${case_db}.sc2_seed VALUES (10, 5, [15,25,35,45], [row(51, 52), row(3, 53)], map{505: row(3, 52)});

-- query 2

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select * from sc2 where array1 is null order by v1;

-- query 3

-- Ordinary equality is UNKNOWN for internal NULLs; WHERE retains only TRUE.
WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select * from sc2 where array1 = [11,null,31,41] order by v1;

-- query 4

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select * from sc2 where array1 = [15,25,35,45] order by v1;

-- query 5

-- Type mismatch: array<int> vs array<map<varchar,tinyint>>
-- @expect_error=does not support binary predicate operation
WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select * from sc2 where array1 = [map{"a":21,"b":22},null] order by v1;

-- query 6

-- Type mismatch: array<struct> vs array<map>
-- @expect_error=does not support binary predicate operation
WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select * from sc2 where array3 = [map{"a":21,"b":22},null] order by v1;

-- query 7

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select * from sc2 where array3 = [named_struct("a",21,"b",22),null] order by v1;

-- query 8

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select * from sc2 where array3 = [row(21,22),null] order by v1;

-- query 9

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select * from sc2 where array2 = [map{null:550},map{505:501}] order by v1;

-- query 10

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select * from sc2 where array2 = [map{44:440},map{404:401}] order by v1;

-- query 11

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select * from sc2 where map1 = null order by v1;

-- query 12

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select * from sc2 where map1 = map{14:41,null:11,12:31} order by v1;

-- query 13

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select * from sc2 where map1 = map{44:44,46:14,42:34} order by v1;

-- query 14

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select * from sc2 where map1 = map{34:43,36:13,32:null} order by v1;

-- query 15

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select * from sc2 where map2 = map{505:[5,50,51,23]} order by v1;

-- query 16

select cast(row(1, null) as struct<a int, b array<int>>);

-- query 17

select cast(map{1: null} as map<int, array<int>>);

-- query 18

-- Casting BOOLEAN (TRUE) to MAP<INT,INT> field of STRUCT: runtime error
-- @expect_error=CAST failed
select cast(row(1, TRUE) as struct<a int, b map<int, int>>);

-- query 19

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select * from sc2 where map2 = map{null:null} order by v1;

-- query 20

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select * from sc2 where map2 = map{null:[4,null,41,23]} order by v1;

-- query 21

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select * from sc2 where map2 = map{101:[1,10,11,23]} order by v1;

-- query 22

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select * from sc2 where map1 = map{46:14,42:34,44:44} order by v1;

-- query 23

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select * from sc2 where map1 = map{54:45,56:15} order by v1;

-- query 24

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select * from sc2 where map1 = map{null:null, 54:45,56:15} order by v1;

-- query 25

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select * from sc2 where map3 = map{505:row(null, 52)} order by v1;

-- query 26

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select * from sc2 where st1 = row(2,null, map{202:null}, row(222,220)) order by v1;

-- query 27

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select * from sc2 where st1 = named_struct("s1", 5,"s2",[5,50,51],"s3",null,"s4", row(null,550)) order by v1;

-- query 28

-- = null is always NULL
WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select * from sc2 where array1 = null order by v1;

-- query 29

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select * from sc2 where st1 = null order by v1;

-- query 30

-- WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
SELECT expression: = in projection
select array1 = [11,null,31,41] from sc2 order by v1;

-- query 31

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select array1 = [15,25,35,45] from sc2 order by v1;

-- query 32

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select array3 = [named_struct("a",21,"b",22),null] from sc2 order by v1;

-- query 33

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select array3 = [row(21,22),null] from sc2 order by v1;

-- query 34

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select array2 = [map{null:550},map{505:501}] from sc2 order by v1;

-- query 35

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select array2 = [map{44:440},map{404:401}] from sc2 order by v1;

-- query 36

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select map1 = null from sc2 order by v1;

-- query 37

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select map1 = map{14:41,null:11,12:31} from sc2 order by v1;

-- query 38

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select map1 = map{44:44,46:14,42:34} from sc2 order by v1;

-- query 39

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select map1 = map{34:43,36:13,32:null} from sc2 order by v1;

-- query 40

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select map2 = map{505:[5,50,51,23]} from sc2 order by v1;

-- query 41

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select map2 = map{null:null} from sc2 order by v1;

-- query 42

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select map2 = map{null:[4,null,41,23]} from sc2 order by v1;

-- query 43

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select map2 = map{101:[1,10,11,23]} from sc2 order by v1;

-- query 44

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select map1 = map{46:14,42:34,44:44} from sc2 order by v1;

-- query 45

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select map1 = map{54:45,56:15} from sc2 order by v1;

-- query 46

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select map1 = map{null:null, 54:45,56:15} from sc2 order by v1;

-- query 47

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select map3 = map{505:row(null, 52)} from sc2 order by v1;

-- query 48

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select st1 = row(2,null, map{202:null}, row(222,220)) from sc2 order by v1;

-- query 49

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select st1 = named_struct("s1", 5,"s2",[5,50,51],"s3",null,"s4", row(null,550)) from sc2 order by v1;

-- query 50

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select array1 = null from sc2 order by v1;

-- query 51

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select st1 = null from sc2 order by v1;

-- query 52

-- Non-null variant of WHERE clauses
WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select * from sc2 where array1 = [11,3,31,41] order by v1;

-- query 53

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select * from sc2 where array1 = [15,25,35,45] order by v1;

-- query 54

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select * from sc2 where array3 = [named_struct("a",21,"b",22), row(12, 13)] order by v1;

-- query 55

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select * from sc2 where array3 = [row(21,22)] order by v1;

-- query 56

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select * from sc2 where array2 = [map{3:550},map{505:501}] order by v1;

-- query 57

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select * from sc2 where array2 = [map{44:440},map{404:401}] order by v1;

-- query 58

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select * from sc2 where map1 = map{14:41,3:11,12:31} order by v1;

-- query 59

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select * from sc2 where map1 = map{44:44,46:14,42:34} order by v1;

-- query 60

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select * from sc2 where map1 = map{34:43,36:13,32:3} order by v1;

-- query 61

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select * from sc2 where map2 = map{505:[5,50,51,23]} order by v1;

-- query 62

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select * from sc2 where map2 = map{3:[4,3,41,23]} order by v1;

-- query 63

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select * from sc2 where map2 = map{101:[1,10,11,44]} order by v1;

-- query 64

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select * from sc2 where map1 = map{46:14,42:34,44:44} order by v1;

-- query 65

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select * from sc2 where map3 = map{505:row(3, 52)} order by v1;

-- query 66

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select * from sc2 where st1 = row(2,[2,3,4], map{202:3}, row(222,220)) order by v1;

-- query 67

-- Type mismatch: st1.s3 is MAP but literal has scalar int for s3
-- @expect_error=does not support binary predicate operation
WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select * from sc2 where st1 = named_struct("s1", 5,"s2",[5,50,51],"s3",3,"s4", row(3,550)) order by v1;

-- query 68

-- <=> (null-safe equality)
WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select array1 <=> [11,null,31,41] from sc2 order by v1;

-- query 69

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select array1 <=> [15,25,35,45] from sc2 order by v1;

-- query 70

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select array3 <=> [named_struct("a",21,"b",22),null] from sc2 order by v1;

-- query 71

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select array3 <=> [row(21,22),null] from sc2 order by v1;

-- query 72

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select array2 <=> [map{null:550},map{505:501}] from sc2 order by v1;

-- query 73

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select array2 <=> [map{44:440},map{404:401}] from sc2 order by v1;

-- query 74

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select map1 <=> null from sc2 order by v1;

-- query 75

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select map1 <=> map{14:41,null:11,12:31} from sc2 order by v1;

-- query 76

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select map1 <=> map{44:44,46:14,42:34} from sc2 order by v1;

-- query 77

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select map1 <=> map{34:43,36:13,32:null} from sc2 order by v1;

-- query 78

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select map2 <=> map{505:[5,50,51,23]} from sc2 order by v1;

-- query 79

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select map2 <=> map{null:null} from sc2 order by v1;

-- query 80

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select map2 <=> map{null:[4,null,41,23]} from sc2 order by v1;

-- query 81

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select map2 <=> map{101:[1,10,11,23]} from sc2 order by v1;

-- query 82

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select map1 <=> map{46:14,42:34,44:44} from sc2 order by v1;

-- query 83

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select map1 <=> map{54:45,56:15} from sc2 order by v1;

-- query 84

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select map1 <=> map{null:null, 54:45,56:15} from sc2 order by v1;

-- query 85

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select map3 <=> map{505:row(null, 52)} from sc2 order by v1;

-- query 86

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select st1 <=> row(2,null, map{202:null}, row(222,220)) from sc2 order by v1;

-- query 87

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select st1 <=> named_struct("s1", 5,"s2",[5,50,51],"s3",null,"s4", row(null,550)) from sc2 order by v1;

-- query 88

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select array1 <=> null from sc2 order by v1;

-- query 89

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select st1 <=> null from sc2 order by v1;

-- query 90

-- != operator
WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select array1 != [11,null,31,41] from sc2 order by v1;

-- query 91

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select array1 != [15,25,35,45] from sc2 order by v1;

-- query 92

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select array3 != [named_struct("a",21,"b",22),null] from sc2 order by v1;

-- query 93

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select array3 != [row(21,22),null] from sc2 order by v1;

-- query 94

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select array2 != [map{null:550},map{505:501}] from sc2 order by v1;

-- query 95

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select array2 != [map{44:440},map{404:401}] from sc2 order by v1;

-- query 96

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select map1 != null from sc2 order by v1;

-- query 97

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select map1 != map{14:41,null:11,12:31} from sc2 order by v1;

-- query 98

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select map1 != map{44:44,46:14,42:34} from sc2 order by v1;

-- query 99

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select map1 != map{34:43,36:13,32:null} from sc2 order by v1;

-- query 100

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select map2 != map{505:[5,50,51,23]} from sc2 order by v1;

-- query 101

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select map2 != map{null:null} from sc2 order by v1;

-- query 102

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select map2 != map{null:[4,null,41,23]} from sc2 order by v1;

-- query 103

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select map2 != map{101:[1,10,11,23]} from sc2 order by v1;

-- query 104

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select map1 != map{46:14,42:34,44:44} from sc2 order by v1;

-- query 105

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select map1 != map{54:45,56:15} from sc2 order by v1;

-- query 106

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select map1 != map{null:null, 54:45,56:15} from sc2 order by v1;

-- query 107

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select map3 != map{505:row(null, 52)} from sc2 order by v1;

-- query 108

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select st1 != row(2,null, map{202:null}, row(222,220)) from sc2 order by v1;

-- query 109

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select st1 != named_struct("s1", 5,"s2",[5,50,51],"s3",null,"s4", row(null,550)) from sc2 order by v1;

-- query 110

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select array1 != null from sc2 order by v1;

-- query 111

WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select st1 != null from sc2 order by v1;

-- query 112

-- Scalar map comparisons with NULL keys/values
-- result NULL: maps with null keys/values and different key sets
select map{1:1,null:2} = map{1:1,2:null};

-- query 113

select map{1:1,null:2} != map{1:1,2:null};

-- query 114

-- result not equal
select map{1:1,null:2} != map{null:2,3:null};

-- query 115

select map{1:1,null:2} = map{null:2,3:null};

-- query 116

select map{1:1,null:2} = map{null:3,1:null};

-- query 117

select map{1:1,null:2} != map{null:3,1:null};

-- query 118

-- A definite mismatch dominates UNKNOWN, even after a NULL field or value.
select row(cast(null as int), 1) = row(cast(null as int), 2) as struct_eq,
       row(cast(null as int), 1) != row(cast(null as int), 2) as struct_ne,
       map{1:null,2:3} = map{1:null,2:4} as map_eq,
       map{1:null,2:3} != map{1:null,2:4} as map_ne,
       row(1,cast(null as int)) = row(1,cast(null as int)) as struct_unknown,
       row(1,cast(null as int)) <=> row(1,cast(null as int)) as struct_safe,
       map{1:null,2:3} <=> map{1:null,2:3} as map_safe;

-- query 119

-- The stored column is an independent oracle for both NULL depths.
WITH sc2 AS (
  SELECT
    v1,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{22: 220}, map{NULL: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{33: NULL}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{NULL: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{11: 110}, map{101: 101}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{22: 220}, map{3: 201}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{33: 3}, map{303: 301}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{44: 440}, map{404: 401}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{3: 550}, map{505: 501}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{14: 41, NULL: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(NULL AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{34: 43, 36: 13, 32: NULL} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{54: 45, 56: 15, NULL: NULL} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{14: 41, 3: 11, 12: 31} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{34: 43, 36: 13, 32: 3} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{44: 44, 46: 14, 42: 34} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{54: 45, 56: 15, 3: 3} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{202: [2, 20, NULL, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{303: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{NULL: [4, NULL, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{NULL: NULL} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{101: [1, 10, 11, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{202: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{303: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{3: [4, 3, 41, 23]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{3: [2, 20, 3, 23]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [1, 10, 11], map{101: 111, NULL: NULL}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(2, NULL, map{202: NULL}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(NULL AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(NULL, [4, NULL, 41], map{NULL: 444}, NULL) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(5, [5, 50, 51], NULL, row(NULL, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [1, 10, 11], map{101: 111, 3: 3}, row(111, 110)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(2, [2, 3, 4], map{202: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(2, [2, 3, 4], map{201: 3}, row(222, 220)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(3, [4, 3, 41], map{3: 444}, row(110, 330)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(5, [5, 50, 51], map{4:555}, row(3, 550)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select v1, array1 = array1 as eq, array1 != array1 as ne, array1 <=> array1 as safe
from sc2 order by v1, eq;
