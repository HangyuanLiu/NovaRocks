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
-- 1. Validate GROUP BY with complex types: ARRAY, MAP, STRUCT (including nested variants).
-- 2. Cover single-column group by, multi-column group by, and group by with duplicates.
-- 3. Include NULL values in complex type elements to test equality/hashing correctness.

-- Keep NULL Map keys in SQL computation. Persist only the seed fields that
-- Iceberg supports; the ordinal preserves each original input occurrence.
-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.sc2_seed (
  fixture_row_ordinal BIGINT NOT NULL,
  `v1` bigint(20) NULL COMMENT "",
  `s2` string NULL,
  `array1` ARRAY<INT> NULL,
  `array3` ARRAY<STRUCT<a INT, b INT>> NULL,
  `map2` MAP<INT, ARRAY<INT>> NULL,
  `map3` MAP<INT, STRUCT<c INT, b INT>> NULL
) TBLPROPERTIES ("format-version" = "3");

INSERT INTO ${case_db}.sc2_seed VALUES (1, 0, "abc", [1,2,3], [row(1, 2)], map{2:[2,3,4], 1:[1,2,3]}, map{2:row(2,4), 1:row(1,2)});
INSERT INTO ${case_db}.sc2_seed VALUES (2, 1, "abc", [2,1,3], [row(1, 3)], map{2:[2,3,4], 1:[1,2,3]}, map{2:row(2,4), 1:row(1,2)});
INSERT INTO ${case_db}.sc2_seed VALUES (3, 2, "abc", [1,3,2], [row(1, 2)], map{2:[2,3,4], 1:[1,2,3]}, map{2:row(2,4), 1:row(1,2)});
INSERT INTO ${case_db}.sc2_seed VALUES (4, 3, "abc", [1,2,3], [row(1, 2)], map{2:[3,2,4], 1:[1,2,3]}, map{2:row(2,4), 1:row(1,2)});
INSERT INTO ${case_db}.sc2_seed VALUES (5, 4, "abc", [1,2,3], [row(1, 3)], map{2:[3,2,4], 1:[1,2,3]}, map{2:row(2,4), 1:row(1,2)});
INSERT INTO ${case_db}.sc2_seed VALUES (6, 5, "abd", [1,2,3], [row(1, 2)], map{1:[1,2,3], 2:[2,3,4]}, map{1:row(1,3), 2:row(2,3)});
INSERT INTO ${case_db}.sc2_seed VALUES (7, 6, "abd", [1,3,2], [row(1, 2)], map{1:[2,1,3], 2:[2,3,4]}, map{1:row(2,3), 2:row(2,3)});
INSERT INTO ${case_db}.sc2_seed VALUES (8, 7, "abd", [2,1,3], [row(2, 1)], map{1:[2,1,3], 2:[2,4,3]}, map{1:row(2,3), 2:row(2,4)});
INSERT INTO ${case_db}.sc2_seed VALUES (9, 8, "abd", [2,1,3], [row(2, 1)], map{1:[1,2,3], 2:[2,4,3]}, map{1:row(2,3), 2:row(2,4)});
INSERT INTO ${case_db}.sc2_seed VALUES (10, 9, "abd", [1,2,3], [row(1, 2)], map{1:[1,2,3], 2:[2,4,3]}, map{1:row(1,3), 2:row(2,4)});
INSERT INTO ${case_db}.sc2_seed VALUES (11, 0, "abc", [1,2,null], [row(1, 2)], map{2:[2,null,4], 1:[1,2,null]}, map{2:row(2,4), 1:row(1,2)});
INSERT INTO ${case_db}.sc2_seed VALUES (12, 1, "abc", [2,1,null], [row(1, null)], map{2:[2,null,4], 1:[1,2,null]}, map{2:row(2,4), 1:row(1,2)});
INSERT INTO ${case_db}.sc2_seed VALUES (13, 2, "abc", [1,null,2], [row(1, 2)], map{2:[2,null,4], 1:[1,2,null]}, map{2:row(2,4), 1:row(1,2)});
INSERT INTO ${case_db}.sc2_seed VALUES (14, 3, "abc", [1,2,null], [row(1, 2)], map{2:[null,2,4], 1:[1,2,null]}, map{2:row(2,4), 1:row(1,2)});
INSERT INTO ${case_db}.sc2_seed VALUES (15, 4, "abc", [1,2,null], [row(1, null)], map{2:[null,2,4], 1:[1,2,null]}, map{2:row(2,4), 1:row(1,2)});
INSERT INTO ${case_db}.sc2_seed VALUES (16, 5, "abd", [1,2,null], [row(1, 2)], map{1:[1,2,null], 2:[2,null,4]}, map{1:row(1,null), 2:row(2,null)});
INSERT INTO ${case_db}.sc2_seed VALUES (17, 6, "abd", [1,null,2], [row(1, 2)], map{1:[2,1,null], 2:[2,null,4]}, map{1:row(2,null), 2:row(2,null)});
INSERT INTO ${case_db}.sc2_seed VALUES (18, 7, "abd", [2,1,null], [row(2, 1)], map{1:[2,1,null], 2:[2,4,null]}, map{1:row(2,null), 2:row(2,4)});
INSERT INTO ${case_db}.sc2_seed VALUES (19, 8, "abd", [2,1,null], [row(2, 1)], map{1:[1,2,null], 2:[2,4,null]}, map{1:row(2,null), 2:row(2,4)});
INSERT INTO ${case_db}.sc2_seed VALUES (20, 9, "abd", [1,2,null], [row(1, 2)], map{1:[1,2,null], 2:[2,4,null]}, map{1:row(1,null), 2:row(2,4)});

-- query 2

WITH sc2 AS (
  SELECT
    v1,
    s2,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 14 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 15 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 16 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 17 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 18 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 19 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 20 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 14 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 15 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 16 THEN CAST(map{1:10, 2:20, null:30} AS MAP<INT, INT>)
      WHEN 17 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 18 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 19 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 20 THEN CAST(map{1:10, 2:20, null:30} AS MAP<INT, INT>)
    END AS map1,
    map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:2, 1:1, 3:3}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, [1,2,3], map{2:2, 1:1, 3:3}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 14 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 15 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 16 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 17 THEN CAST(row(1, [1,2,null], map{2:2, 1:1, null:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 18 THEN CAST(row(1, [1,2,null], map{2:2, 1:1, null:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 19 THEN CAST(row(1, [1,2,null], map{2:2, null:null, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 20 THEN CAST(row(1, [1,2,null], map{2:2, null:null, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select array1, count(distinct s2) from sc2 group by array1;

-- query 3

WITH sc2 AS (
  SELECT
    v1,
    s2,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 14 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 15 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 16 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 17 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 18 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 19 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 20 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 14 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 15 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 16 THEN CAST(map{1:10, 2:20, null:30} AS MAP<INT, INT>)
      WHEN 17 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 18 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 19 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 20 THEN CAST(map{1:10, 2:20, null:30} AS MAP<INT, INT>)
    END AS map1,
    map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:2, 1:1, 3:3}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, [1,2,3], map{2:2, 1:1, 3:3}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 14 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 15 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 16 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 17 THEN CAST(row(1, [1,2,null], map{2:2, 1:1, null:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 18 THEN CAST(row(1, [1,2,null], map{2:2, 1:1, null:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 19 THEN CAST(row(1, [1,2,null], map{2:2, null:null, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 20 THEN CAST(row(1, [1,2,null], map{2:2, null:null, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select array2, count(distinct s2) from sc2 group by array2;

-- query 4

WITH sc2 AS (
  SELECT
    v1,
    s2,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 14 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 15 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 16 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 17 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 18 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 19 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 20 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 14 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 15 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 16 THEN CAST(map{1:10, 2:20, null:30} AS MAP<INT, INT>)
      WHEN 17 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 18 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 19 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 20 THEN CAST(map{1:10, 2:20, null:30} AS MAP<INT, INT>)
    END AS map1,
    map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:2, 1:1, 3:3}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, [1,2,3], map{2:2, 1:1, 3:3}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 14 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 15 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 16 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 17 THEN CAST(row(1, [1,2,null], map{2:2, 1:1, null:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 18 THEN CAST(row(1, [1,2,null], map{2:2, 1:1, null:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 19 THEN CAST(row(1, [1,2,null], map{2:2, null:null, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 20 THEN CAST(row(1, [1,2,null], map{2:2, null:null, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select array3, count(distinct s2) from sc2 group by array3;

-- query 5

WITH sc2 AS (
  SELECT
    v1,
    s2,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 14 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 15 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 16 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 17 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 18 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 19 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 20 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 14 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 15 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 16 THEN CAST(map{1:10, 2:20, null:30} AS MAP<INT, INT>)
      WHEN 17 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 18 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 19 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 20 THEN CAST(map{1:10, 2:20, null:30} AS MAP<INT, INT>)
    END AS map1,
    map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:2, 1:1, 3:3}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, [1,2,3], map{2:2, 1:1, 3:3}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 14 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 15 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 16 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 17 THEN CAST(row(1, [1,2,null], map{2:2, 1:1, null:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 18 THEN CAST(row(1, [1,2,null], map{2:2, 1:1, null:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 19 THEN CAST(row(1, [1,2,null], map{2:2, null:null, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 20 THEN CAST(row(1, [1,2,null], map{2:2, null:null, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select map1, count(distinct s2) from sc2 group by map1;

-- query 6

WITH sc2 AS (
  SELECT
    v1,
    s2,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 14 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 15 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 16 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 17 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 18 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 19 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 20 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 14 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 15 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 16 THEN CAST(map{1:10, 2:20, null:30} AS MAP<INT, INT>)
      WHEN 17 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 18 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 19 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 20 THEN CAST(map{1:10, 2:20, null:30} AS MAP<INT, INT>)
    END AS map1,
    map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:2, 1:1, 3:3}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, [1,2,3], map{2:2, 1:1, 3:3}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 14 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 15 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 16 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 17 THEN CAST(row(1, [1,2,null], map{2:2, 1:1, null:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 18 THEN CAST(row(1, [1,2,null], map{2:2, 1:1, null:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 19 THEN CAST(row(1, [1,2,null], map{2:2, null:null, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 20 THEN CAST(row(1, [1,2,null], map{2:2, null:null, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select map2, count(distinct s2) from sc2 group by map2;

-- query 7

WITH sc2 AS (
  SELECT
    v1,
    s2,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 14 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 15 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 16 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 17 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 18 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 19 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 20 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 14 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 15 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 16 THEN CAST(map{1:10, 2:20, null:30} AS MAP<INT, INT>)
      WHEN 17 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 18 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 19 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 20 THEN CAST(map{1:10, 2:20, null:30} AS MAP<INT, INT>)
    END AS map1,
    map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:2, 1:1, 3:3}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, [1,2,3], map{2:2, 1:1, 3:3}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 14 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 15 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 16 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 17 THEN CAST(row(1, [1,2,null], map{2:2, 1:1, null:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 18 THEN CAST(row(1, [1,2,null], map{2:2, 1:1, null:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 19 THEN CAST(row(1, [1,2,null], map{2:2, null:null, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 20 THEN CAST(row(1, [1,2,null], map{2:2, null:null, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select map3, count(distinct s2) from sc2 group by map3;

-- query 8

WITH sc2 AS (
  SELECT
    v1,
    s2,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 14 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 15 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 16 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 17 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 18 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 19 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 20 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 14 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 15 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 16 THEN CAST(map{1:10, 2:20, null:30} AS MAP<INT, INT>)
      WHEN 17 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 18 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 19 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 20 THEN CAST(map{1:10, 2:20, null:30} AS MAP<INT, INT>)
    END AS map1,
    map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:2, 1:1, 3:3}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, [1,2,3], map{2:2, 1:1, 3:3}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 14 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 15 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 16 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 17 THEN CAST(row(1, [1,2,null], map{2:2, 1:1, null:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 18 THEN CAST(row(1, [1,2,null], map{2:2, 1:1, null:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 19 THEN CAST(row(1, [1,2,null], map{2:2, null:null, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 20 THEN CAST(row(1, [1,2,null], map{2:2, null:null, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select st1, count(distinct s2) from sc2 group by st1;

-- query 9

WITH sc2 AS (
  SELECT
    v1,
    s2,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 14 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 15 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 16 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 17 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 18 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 19 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 20 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 14 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 15 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 16 THEN CAST(map{1:10, 2:20, null:30} AS MAP<INT, INT>)
      WHEN 17 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 18 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 19 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 20 THEN CAST(map{1:10, 2:20, null:30} AS MAP<INT, INT>)
    END AS map1,
    map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:2, 1:1, 3:3}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, [1,2,3], map{2:2, 1:1, 3:3}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 14 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 15 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 16 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 17 THEN CAST(row(1, [1,2,null], map{2:2, 1:1, null:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 18 THEN CAST(row(1, [1,2,null], map{2:2, 1:1, null:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 19 THEN CAST(row(1, [1,2,null], map{2:2, null:null, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 20 THEN CAST(row(1, [1,2,null], map{2:2, null:null, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select s2, s2, count(distinct s2) from sc2 group by s2, s2;

-- query 10

WITH sc2 AS (
  SELECT
    v1,
    s2,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 14 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 15 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 16 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 17 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 18 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 19 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 20 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 14 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 15 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 16 THEN CAST(map{1:10, 2:20, null:30} AS MAP<INT, INT>)
      WHEN 17 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 18 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 19 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 20 THEN CAST(map{1:10, 2:20, null:30} AS MAP<INT, INT>)
    END AS map1,
    map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:2, 1:1, 3:3}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, [1,2,3], map{2:2, 1:1, 3:3}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 14 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 15 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 16 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 17 THEN CAST(row(1, [1,2,null], map{2:2, 1:1, null:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 18 THEN CAST(row(1, [1,2,null], map{2:2, 1:1, null:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 19 THEN CAST(row(1, [1,2,null], map{2:2, null:null, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 20 THEN CAST(row(1, [1,2,null], map{2:2, null:null, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select s2, array1, count(distinct s2) from sc2 group by s2, array1;

-- query 11

WITH sc2 AS (
  SELECT
    v1,
    s2,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 14 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 15 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 16 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 17 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 18 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 19 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 20 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 14 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 15 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 16 THEN CAST(map{1:10, 2:20, null:30} AS MAP<INT, INT>)
      WHEN 17 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 18 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 19 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 20 THEN CAST(map{1:10, 2:20, null:30} AS MAP<INT, INT>)
    END AS map1,
    map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:2, 1:1, 3:3}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, [1,2,3], map{2:2, 1:1, 3:3}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 14 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 15 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 16 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 17 THEN CAST(row(1, [1,2,null], map{2:2, 1:1, null:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 18 THEN CAST(row(1, [1,2,null], map{2:2, 1:1, null:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 19 THEN CAST(row(1, [1,2,null], map{2:2, null:null, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 20 THEN CAST(row(1, [1,2,null], map{2:2, null:null, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select s2, array2, count(distinct s2) from sc2 group by s2, array2;

-- query 12

WITH sc2 AS (
  SELECT
    v1,
    s2,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 14 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 15 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 16 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 17 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 18 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 19 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 20 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 14 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 15 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 16 THEN CAST(map{1:10, 2:20, null:30} AS MAP<INT, INT>)
      WHEN 17 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 18 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 19 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 20 THEN CAST(map{1:10, 2:20, null:30} AS MAP<INT, INT>)
    END AS map1,
    map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:2, 1:1, 3:3}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, [1,2,3], map{2:2, 1:1, 3:3}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 14 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 15 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 16 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 17 THEN CAST(row(1, [1,2,null], map{2:2, 1:1, null:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 18 THEN CAST(row(1, [1,2,null], map{2:2, 1:1, null:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 19 THEN CAST(row(1, [1,2,null], map{2:2, null:null, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 20 THEN CAST(row(1, [1,2,null], map{2:2, null:null, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select s2, array3, count(distinct s2) from sc2 group by s2, array3;

-- query 13

WITH sc2 AS (
  SELECT
    v1,
    s2,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 14 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 15 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 16 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 17 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 18 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 19 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 20 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 14 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 15 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 16 THEN CAST(map{1:10, 2:20, null:30} AS MAP<INT, INT>)
      WHEN 17 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 18 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 19 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 20 THEN CAST(map{1:10, 2:20, null:30} AS MAP<INT, INT>)
    END AS map1,
    map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:2, 1:1, 3:3}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, [1,2,3], map{2:2, 1:1, 3:3}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 14 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 15 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 16 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 17 THEN CAST(row(1, [1,2,null], map{2:2, 1:1, null:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 18 THEN CAST(row(1, [1,2,null], map{2:2, 1:1, null:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 19 THEN CAST(row(1, [1,2,null], map{2:2, null:null, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 20 THEN CAST(row(1, [1,2,null], map{2:2, null:null, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select s2, map1, count(distinct s2) from sc2 group by s2, map1;

-- query 14

WITH sc2 AS (
  SELECT
    v1,
    s2,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 14 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 15 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 16 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 17 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 18 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 19 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 20 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 14 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 15 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 16 THEN CAST(map{1:10, 2:20, null:30} AS MAP<INT, INT>)
      WHEN 17 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 18 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 19 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 20 THEN CAST(map{1:10, 2:20, null:30} AS MAP<INT, INT>)
    END AS map1,
    map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:2, 1:1, 3:3}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, [1,2,3], map{2:2, 1:1, 3:3}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 14 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 15 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 16 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 17 THEN CAST(row(1, [1,2,null], map{2:2, 1:1, null:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 18 THEN CAST(row(1, [1,2,null], map{2:2, 1:1, null:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 19 THEN CAST(row(1, [1,2,null], map{2:2, null:null, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 20 THEN CAST(row(1, [1,2,null], map{2:2, null:null, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select s2, map2, count(distinct s2) from sc2 group by s2, map2;

-- query 15

WITH sc2 AS (
  SELECT
    v1,
    s2,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 14 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 15 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 16 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 17 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 18 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 19 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 20 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 14 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 15 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 16 THEN CAST(map{1:10, 2:20, null:30} AS MAP<INT, INT>)
      WHEN 17 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 18 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 19 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 20 THEN CAST(map{1:10, 2:20, null:30} AS MAP<INT, INT>)
    END AS map1,
    map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:2, 1:1, 3:3}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, [1,2,3], map{2:2, 1:1, 3:3}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 14 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 15 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 16 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 17 THEN CAST(row(1, [1,2,null], map{2:2, 1:1, null:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 18 THEN CAST(row(1, [1,2,null], map{2:2, 1:1, null:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 19 THEN CAST(row(1, [1,2,null], map{2:2, null:null, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 20 THEN CAST(row(1, [1,2,null], map{2:2, null:null, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select s2, map3, count(distinct s2) from sc2 group by s2, map3;

-- query 16

WITH sc2 AS (
  SELECT
    v1,
    s2,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 14 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 15 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 16 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 17 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 18 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 19 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 20 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 14 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 15 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 16 THEN CAST(map{1:10, 2:20, null:30} AS MAP<INT, INT>)
      WHEN 17 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 18 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 19 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 20 THEN CAST(map{1:10, 2:20, null:30} AS MAP<INT, INT>)
    END AS map1,
    map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:2, 1:1, 3:3}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, [1,2,3], map{2:2, 1:1, 3:3}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 14 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 15 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 16 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 17 THEN CAST(row(1, [1,2,null], map{2:2, 1:1, null:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 18 THEN CAST(row(1, [1,2,null], map{2:2, 1:1, null:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 19 THEN CAST(row(1, [1,2,null], map{2:2, null:null, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 20 THEN CAST(row(1, [1,2,null], map{2:2, null:null, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select s2, st1, count(distinct s2) from sc2 group by s2, st1;

-- query 17

WITH sc2 AS (
  SELECT
    v1,
    s2,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 14 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 15 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 16 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 17 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 18 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 19 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 20 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 14 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 15 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 16 THEN CAST(map{1:10, 2:20, null:30} AS MAP<INT, INT>)
      WHEN 17 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 18 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 19 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 20 THEN CAST(map{1:10, 2:20, null:30} AS MAP<INT, INT>)
    END AS map1,
    map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:2, 1:1, 3:3}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, [1,2,3], map{2:2, 1:1, 3:3}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 14 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 15 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 16 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 17 THEN CAST(row(1, [1,2,null], map{2:2, 1:1, null:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 18 THEN CAST(row(1, [1,2,null], map{2:2, 1:1, null:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 19 THEN CAST(row(1, [1,2,null], map{2:2, null:null, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 20 THEN CAST(row(1, [1,2,null], map{2:2, null:null, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select array1, array1, count(distinct s2) from sc2 group by array1, array1;

-- query 18

WITH sc2 AS (
  SELECT
    v1,
    s2,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 14 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 15 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 16 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 17 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 18 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 19 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 20 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 14 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 15 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 16 THEN CAST(map{1:10, 2:20, null:30} AS MAP<INT, INT>)
      WHEN 17 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 18 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 19 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 20 THEN CAST(map{1:10, 2:20, null:30} AS MAP<INT, INT>)
    END AS map1,
    map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:2, 1:1, 3:3}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, [1,2,3], map{2:2, 1:1, 3:3}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 14 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 15 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 16 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 17 THEN CAST(row(1, [1,2,null], map{2:2, 1:1, null:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 18 THEN CAST(row(1, [1,2,null], map{2:2, 1:1, null:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 19 THEN CAST(row(1, [1,2,null], map{2:2, null:null, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 20 THEN CAST(row(1, [1,2,null], map{2:2, null:null, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select array1, map3, count(distinct s2) from sc2 group by array1, map3;

-- query 19

WITH sc2 AS (
  SELECT
    v1,
    s2,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 14 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 15 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 16 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 17 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 18 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 19 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 20 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 14 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 15 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 16 THEN CAST(map{1:10, 2:20, null:30} AS MAP<INT, INT>)
      WHEN 17 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 18 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 19 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 20 THEN CAST(map{1:10, 2:20, null:30} AS MAP<INT, INT>)
    END AS map1,
    map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:2, 1:1, 3:3}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, [1,2,3], map{2:2, 1:1, 3:3}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 14 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 15 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 16 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 17 THEN CAST(row(1, [1,2,null], map{2:2, 1:1, null:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 18 THEN CAST(row(1, [1,2,null], map{2:2, 1:1, null:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 19 THEN CAST(row(1, [1,2,null], map{2:2, null:null, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 20 THEN CAST(row(1, [1,2,null], map{2:2, null:null, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select array1, st1, count(distinct s2) from sc2 group by array1, st1;

-- query 20

WITH sc2 AS (
  SELECT
    v1,
    s2,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 14 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 15 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 16 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 17 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 18 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 19 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 20 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 14 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 15 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 16 THEN CAST(map{1:10, 2:20, null:30} AS MAP<INT, INT>)
      WHEN 17 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 18 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 19 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 20 THEN CAST(map{1:10, 2:20, null:30} AS MAP<INT, INT>)
    END AS map1,
    map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:2, 1:1, 3:3}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, [1,2,3], map{2:2, 1:1, 3:3}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 14 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 15 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 16 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 17 THEN CAST(row(1, [1,2,null], map{2:2, 1:1, null:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 18 THEN CAST(row(1, [1,2,null], map{2:2, 1:1, null:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 19 THEN CAST(row(1, [1,2,null], map{2:2, null:null, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 20 THEN CAST(row(1, [1,2,null], map{2:2, null:null, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select array2, s2, count(distinct s2) from sc2 group by array2, s2;

-- query 21

WITH sc2 AS (
  SELECT
    v1,
    s2,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 14 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 15 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 16 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 17 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 18 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 19 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 20 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 14 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 15 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 16 THEN CAST(map{1:10, 2:20, null:30} AS MAP<INT, INT>)
      WHEN 17 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 18 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 19 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 20 THEN CAST(map{1:10, 2:20, null:30} AS MAP<INT, INT>)
    END AS map1,
    map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:2, 1:1, 3:3}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, [1,2,3], map{2:2, 1:1, 3:3}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 14 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 15 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 16 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 17 THEN CAST(row(1, [1,2,null], map{2:2, 1:1, null:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 18 THEN CAST(row(1, [1,2,null], map{2:2, 1:1, null:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 19 THEN CAST(row(1, [1,2,null], map{2:2, null:null, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 20 THEN CAST(row(1, [1,2,null], map{2:2, null:null, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select map1, s2, count(distinct s2) from sc2 group by map1, s2;

-- query 22

WITH sc2 AS (
  SELECT
    v1,
    s2,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 14 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 15 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 16 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 17 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 18 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 19 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 20 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 14 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 15 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 16 THEN CAST(map{1:10, 2:20, null:30} AS MAP<INT, INT>)
      WHEN 17 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 18 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 19 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 20 THEN CAST(map{1:10, 2:20, null:30} AS MAP<INT, INT>)
    END AS map1,
    map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:2, 1:1, 3:3}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, [1,2,3], map{2:2, 1:1, 3:3}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 14 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 15 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 16 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 17 THEN CAST(row(1, [1,2,null], map{2:2, 1:1, null:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 18 THEN CAST(row(1, [1,2,null], map{2:2, 1:1, null:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 19 THEN CAST(row(1, [1,2,null], map{2:2, null:null, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 20 THEN CAST(row(1, [1,2,null], map{2:2, null:null, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select map1, map3, count(distinct s2) from sc2 group by map1, map3;

-- query 23

WITH sc2 AS (
  SELECT
    v1,
    s2,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 14 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 15 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 16 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 17 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 18 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 19 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 20 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 14 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 15 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 16 THEN CAST(map{1:10, 2:20, null:30} AS MAP<INT, INT>)
      WHEN 17 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 18 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 19 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 20 THEN CAST(map{1:10, 2:20, null:30} AS MAP<INT, INT>)
    END AS map1,
    map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:2, 1:1, 3:3}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, [1,2,3], map{2:2, 1:1, 3:3}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 14 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 15 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 16 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 17 THEN CAST(row(1, [1,2,null], map{2:2, 1:1, null:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 18 THEN CAST(row(1, [1,2,null], map{2:2, 1:1, null:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 19 THEN CAST(row(1, [1,2,null], map{2:2, null:null, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 20 THEN CAST(row(1, [1,2,null], map{2:2, null:null, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select map1, st1, count(distinct s2) from sc2 group by map1, st1;

-- query 24

WITH sc2 AS (
  SELECT
    v1,
    s2,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 14 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 15 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 16 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 17 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 18 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 19 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 20 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 14 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 15 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 16 THEN CAST(map{1:10, 2:20, null:30} AS MAP<INT, INT>)
      WHEN 17 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 18 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 19 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 20 THEN CAST(map{1:10, 2:20, null:30} AS MAP<INT, INT>)
    END AS map1,
    map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:2, 1:1, 3:3}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, [1,2,3], map{2:2, 1:1, 3:3}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 14 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 15 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 16 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 17 THEN CAST(row(1, [1,2,null], map{2:2, 1:1, null:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 18 THEN CAST(row(1, [1,2,null], map{2:2, 1:1, null:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 19 THEN CAST(row(1, [1,2,null], map{2:2, null:null, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 20 THEN CAST(row(1, [1,2,null], map{2:2, null:null, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select map2, s2, count(distinct s2) from sc2 group by map2, s2;

-- query 25

WITH sc2 AS (
  SELECT
    v1,
    s2,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 14 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 15 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 16 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 17 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 18 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 19 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 20 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 14 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 15 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 16 THEN CAST(map{1:10, 2:20, null:30} AS MAP<INT, INT>)
      WHEN 17 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 18 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 19 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 20 THEN CAST(map{1:10, 2:20, null:30} AS MAP<INT, INT>)
    END AS map1,
    map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:2, 1:1, 3:3}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, [1,2,3], map{2:2, 1:1, 3:3}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 14 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 15 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 16 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 17 THEN CAST(row(1, [1,2,null], map{2:2, 1:1, null:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 18 THEN CAST(row(1, [1,2,null], map{2:2, 1:1, null:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 19 THEN CAST(row(1, [1,2,null], map{2:2, null:null, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 20 THEN CAST(row(1, [1,2,null], map{2:2, null:null, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select map2, map1, count(distinct s2) from sc2 group by map2, map1;

-- query 26

WITH sc2 AS (
  SELECT
    v1,
    s2,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 14 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 15 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 16 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 17 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 18 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 19 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 20 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 14 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 15 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 16 THEN CAST(map{1:10, 2:20, null:30} AS MAP<INT, INT>)
      WHEN 17 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 18 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 19 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 20 THEN CAST(map{1:10, 2:20, null:30} AS MAP<INT, INT>)
    END AS map1,
    map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:2, 1:1, 3:3}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, [1,2,3], map{2:2, 1:1, 3:3}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 14 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 15 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 16 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 17 THEN CAST(row(1, [1,2,null], map{2:2, 1:1, null:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 18 THEN CAST(row(1, [1,2,null], map{2:2, 1:1, null:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 19 THEN CAST(row(1, [1,2,null], map{2:2, null:null, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 20 THEN CAST(row(1, [1,2,null], map{2:2, null:null, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select map3, s2, count(distinct s2) from sc2 group by map3, s2;

-- query 27

WITH sc2 AS (
  SELECT
    v1,
    s2,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 14 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 15 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 16 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 17 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 18 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 19 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 20 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 14 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 15 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 16 THEN CAST(map{1:10, 2:20, null:30} AS MAP<INT, INT>)
      WHEN 17 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 18 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 19 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 20 THEN CAST(map{1:10, 2:20, null:30} AS MAP<INT, INT>)
    END AS map1,
    map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:2, 1:1, 3:3}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, [1,2,3], map{2:2, 1:1, 3:3}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 14 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 15 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 16 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 17 THEN CAST(row(1, [1,2,null], map{2:2, 1:1, null:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 18 THEN CAST(row(1, [1,2,null], map{2:2, 1:1, null:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 19 THEN CAST(row(1, [1,2,null], map{2:2, null:null, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 20 THEN CAST(row(1, [1,2,null], map{2:2, null:null, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select st1, s2, count(distinct s2) from sc2 group by st1, s2;

-- query 28

WITH sc2 AS (
  SELECT
    v1,
    s2,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 14 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 15 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 16 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 17 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 18 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 19 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 20 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 14 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 15 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 16 THEN CAST(map{1:10, 2:20, null:30} AS MAP<INT, INT>)
      WHEN 17 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 18 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 19 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 20 THEN CAST(map{1:10, 2:20, null:30} AS MAP<INT, INT>)
    END AS map1,
    map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:2, 1:1, 3:3}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, [1,2,3], map{2:2, 1:1, 3:3}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 14 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 15 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 16 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 17 THEN CAST(row(1, [1,2,null], map{2:2, 1:1, null:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 18 THEN CAST(row(1, [1,2,null], map{2:2, 1:1, null:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 19 THEN CAST(row(1, [1,2,null], map{2:2, null:null, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 20 THEN CAST(row(1, [1,2,null], map{2:2, null:null, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select st1, array2, count(distinct s2) from sc2 group by st1, array2;

-- query 29

WITH sc2 AS (
  SELECT
    v1,
    s2,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 14 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 15 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 16 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 17 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 18 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 19 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 20 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 14 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 15 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 16 THEN CAST(map{1:10, 2:20, null:30} AS MAP<INT, INT>)
      WHEN 17 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 18 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 19 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 20 THEN CAST(map{1:10, 2:20, null:30} AS MAP<INT, INT>)
    END AS map1,
    map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:2, 1:1, 3:3}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, [1,2,3], map{2:2, 1:1, 3:3}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 14 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 15 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 16 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 17 THEN CAST(row(1, [1,2,null], map{2:2, 1:1, null:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 18 THEN CAST(row(1, [1,2,null], map{2:2, 1:1, null:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 19 THEN CAST(row(1, [1,2,null], map{2:2, null:null, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 20 THEN CAST(row(1, [1,2,null], map{2:2, null:null, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select st1, map3, count(distinct s2) from sc2 group by st1, map3;

-- query 30

WITH sc2 AS (
  SELECT
    v1,
    s2,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([map{2:20, 1:10, 3:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 14 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 15 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 16 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 17 THEN CAST([map{1:10, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 18 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 19 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 20 THEN CAST([map{1:10, null:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, 4:40} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 14 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 15 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 16 THEN CAST(map{1:10, 2:20, null:30} AS MAP<INT, INT>)
      WHEN 17 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 18 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 19 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 20 THEN CAST(map{1:10, 2:20, null:30} AS MAP<INT, INT>)
    END AS map1,
    map2,
    map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:2, 1:1, 3:3}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, [1,2,3], map{2:2, 1:1, 3:3}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 14 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 15 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 16 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 17 THEN CAST(row(1, [1,2,null], map{2:2, 1:1, null:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 18 THEN CAST(row(1, [1,2,null], map{2:2, 1:1, null:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 19 THEN CAST(row(1, [1,2,null], map{2:2, null:null, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 20 THEN CAST(row(1, [1,2,null], map{2:2, null:null, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select st1, st1, count(distinct s2) from sc2 group by st1, st1;
