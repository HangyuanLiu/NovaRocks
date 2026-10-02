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
-- 1. Validate IN/NOT IN predicate with complex types: JSON, ARRAY, MAP, STRUCT.
-- 2. Cover NULL handling, empty arrays/maps, nested complex types in IN lists.
-- 3. Cover JSON subquery predicates and type mismatch error paths for each type.

-- Keep NULL Map keys in SQL computation. Persist only the seed fields that
-- Iceberg supports; the ordinal preserves each original input occurrence.
-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.sc2_seed (
  fixture_row_ordinal BIGINT NOT NULL,
  `v1` bigint(20) NULL COMMENT "",
  `js` json NULL,
  `array1` ARRAY<INT> NULL,
  `array3` ARRAY<STRUCT<a INT, b INT>> NULL
) TBLPROPERTIES ("format-version" = "3");

INSERT INTO ${case_db}.sc2_seed VALUES (1, 0, null, null, null);
INSERT INTO ${case_db}.sc2_seed VALUES (2, 2, json_object("a", null), [1,3,2], [row(1, 2)]);
INSERT INTO ${case_db}.sc2_seed VALUES (3, 3, json_object("a", 1,'b',2), [1,2,3], [row(1, 2)]);
INSERT INTO ${case_db}.sc2_seed VALUES (4, 4, json_object("a", 1,null,null), [1,2,3], [row(1, 3)]);
INSERT INTO ${case_db}.sc2_seed VALUES (5, 5, json_object("a", 1,'b',null), [1,2,3], [row(1, 2)]);
INSERT INTO ${case_db}.sc2_seed VALUES (6, 6, json_object("a", 1,'b',3), [1,3,2], [row(1, 2)]);
INSERT INTO ${case_db}.sc2_seed VALUES (7, 7, json_object("a", 1, 'b',4), [2,1,3], []);
INSERT INTO ${case_db}.sc2_seed VALUES (8, 8, json_object("a", 1, 'c', null), [2,1,3], [null]);
INSERT INTO ${case_db}.sc2_seed VALUES (9, 9, json_object("a", 1), [], [row(1, 2),null]);
INSERT INTO ${case_db}.sc2_seed VALUES (10, 10, json_object("a"), [1,2,null], [row(1, 2)]);
INSERT INTO ${case_db}.sc2_seed VALUES (11, 11, json_object("", 2), [2,1,null], [row(1, null)]);
INSERT INTO ${case_db}.sc2_seed VALUES (12, 12, parse_json('{}'), [1,2], [row(1, 2)]);
INSERT INTO ${case_db}.sc2_seed VALUES (13, 13, null, [], [row(1, 2)]);

-- query 2

-- JSON IN predicate
WITH sc2 AS (
  SELECT
    v1,
    js,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{null:null}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, null:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([null] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:null} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:null, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{1:10, 2:20, null:40} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 4:40} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{2:[2,3,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{1:[1,2,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{1:[2,1,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{1:[2,1,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{1:[1,2,3], null:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{1:[1,2,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 11 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 12 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 13 THEN CAST(map{2:[null,2,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 2 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 3 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 4 THEN CAST(map{2:row(2,4), 1:row(1,null)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 5 THEN CAST(map{} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 6 THEN CAST(map{1:row(2,3), 2:row(2,3)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 7 THEN CAST(map{1:row(2,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 8 THEN CAST(map{null:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 9 THEN CAST(map{1:row(1,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 10 THEN CAST(map{null:null} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 11 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 12 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 13 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
    END AS map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, null, map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [], map{1:1, 2:null}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [null], map{}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, null, map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], null, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [null], map{null:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select js in (parse_json('{}'),json_object('a',1)) from sc2 order by 1;

-- query 3

WITH sc2 AS (
  SELECT
    v1,
    js,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{null:null}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, null:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([null] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:null} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:null, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{1:10, 2:20, null:40} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 4:40} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{2:[2,3,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{1:[1,2,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{1:[2,1,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{1:[2,1,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{1:[1,2,3], null:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{1:[1,2,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 11 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 12 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 13 THEN CAST(map{2:[null,2,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 2 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 3 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 4 THEN CAST(map{2:row(2,4), 1:row(1,null)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 5 THEN CAST(map{} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 6 THEN CAST(map{1:row(2,3), 2:row(2,3)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 7 THEN CAST(map{1:row(2,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 8 THEN CAST(map{null:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 9 THEN CAST(map{1:row(1,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 10 THEN CAST(map{null:null} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 11 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 12 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 13 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
    END AS map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, null, map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [], map{1:1, 2:null}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [null], map{}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, null, map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], null, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [null], map{null:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select js not in (parse_json('{}'),json_object('a',1)) from sc2 order by 1;

-- query 4

WITH sc2 AS (
  SELECT
    v1,
    js,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{null:null}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, null:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([null] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:null} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:null, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{1:10, 2:20, null:40} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 4:40} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{2:[2,3,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{1:[1,2,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{1:[2,1,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{1:[2,1,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{1:[1,2,3], null:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{1:[1,2,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 11 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 12 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 13 THEN CAST(map{2:[null,2,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 2 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 3 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 4 THEN CAST(map{2:row(2,4), 1:row(1,null)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 5 THEN CAST(map{} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 6 THEN CAST(map{1:row(2,3), 2:row(2,3)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 7 THEN CAST(map{1:row(2,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 8 THEN CAST(map{null:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 9 THEN CAST(map{1:row(1,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 10 THEN CAST(map{null:null} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 11 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 12 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 13 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
    END AS map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, null, map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [], map{1:1, 2:null}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [null], map{}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, null, map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], null, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [null], map{null:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select parse_json('{}') in (js,json_object('a',1)) from sc2 order by 1;

-- query 5

WITH sc2 AS (
  SELECT
    v1,
    js,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{null:null}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, null:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([null] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:null} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:null, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{1:10, 2:20, null:40} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 4:40} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{2:[2,3,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{1:[1,2,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{1:[2,1,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{1:[2,1,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{1:[1,2,3], null:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{1:[1,2,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 11 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 12 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 13 THEN CAST(map{2:[null,2,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 2 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 3 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 4 THEN CAST(map{2:row(2,4), 1:row(1,null)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 5 THEN CAST(map{} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 6 THEN CAST(map{1:row(2,3), 2:row(2,3)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 7 THEN CAST(map{1:row(2,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 8 THEN CAST(map{null:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 9 THEN CAST(map{1:row(1,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 10 THEN CAST(map{null:null} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 11 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 12 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 13 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
    END AS map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, null, map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [], map{1:1, 2:null}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [null], map{}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, null, map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], null, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [null], map{null:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select parse_json('{}') not in (js,json_object('a',1)) from sc2 order by 1;

-- query 6

WITH sc2 AS (
  SELECT
    v1,
    js,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{null:null}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, null:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([null] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:null} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:null, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{1:10, 2:20, null:40} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 4:40} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{2:[2,3,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{1:[1,2,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{1:[2,1,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{1:[2,1,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{1:[1,2,3], null:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{1:[1,2,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 11 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 12 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 13 THEN CAST(map{2:[null,2,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 2 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 3 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 4 THEN CAST(map{2:row(2,4), 1:row(1,null)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 5 THEN CAST(map{} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 6 THEN CAST(map{1:row(2,3), 2:row(2,3)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 7 THEN CAST(map{1:row(2,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 8 THEN CAST(map{null:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 9 THEN CAST(map{1:row(1,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 10 THEN CAST(map{null:null} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 11 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 12 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 13 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
    END AS map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, null, map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [], map{1:1, 2:null}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [null], map{}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, null, map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], null, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [null], map{null:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select js in (null,json_object('a',1)) from sc2 order by 1;

-- query 7

WITH sc2 AS (
  SELECT
    v1,
    js,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{null:null}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, null:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([null] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:null} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:null, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{1:10, 2:20, null:40} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 4:40} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{2:[2,3,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{1:[1,2,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{1:[2,1,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{1:[2,1,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{1:[1,2,3], null:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{1:[1,2,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 11 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 12 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 13 THEN CAST(map{2:[null,2,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 2 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 3 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 4 THEN CAST(map{2:row(2,4), 1:row(1,null)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 5 THEN CAST(map{} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 6 THEN CAST(map{1:row(2,3), 2:row(2,3)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 7 THEN CAST(map{1:row(2,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 8 THEN CAST(map{null:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 9 THEN CAST(map{1:row(1,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 10 THEN CAST(map{null:null} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 11 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 12 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 13 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
    END AS map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, null, map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [], map{1:1, 2:null}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [null], map{}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, null, map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], null, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [null], map{null:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select js not in (null) from sc2 order by 1;

-- query 8

WITH sc2 AS (
  SELECT
    v1,
    js,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{null:null}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, null:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([null] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:null} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:null, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{1:10, 2:20, null:40} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 4:40} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{2:[2,3,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{1:[1,2,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{1:[2,1,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{1:[2,1,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{1:[1,2,3], null:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{1:[1,2,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 11 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 12 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 13 THEN CAST(map{2:[null,2,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 2 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 3 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 4 THEN CAST(map{2:row(2,4), 1:row(1,null)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 5 THEN CAST(map{} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 6 THEN CAST(map{1:row(2,3), 2:row(2,3)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 7 THEN CAST(map{1:row(2,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 8 THEN CAST(map{null:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 9 THEN CAST(map{1:row(1,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 10 THEN CAST(map{null:null} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 11 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 12 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 13 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
    END AS map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, null, map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [], map{1:1, 2:null}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [null], map{}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, null, map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], null, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [null], map{null:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select null in (js,json_object('a',1)) from sc2 order by 1;

-- query 9

WITH sc2 AS (
  SELECT
    v1,
    js,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{null:null}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, null:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([null] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:null} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:null, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{1:10, 2:20, null:40} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 4:40} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{2:[2,3,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{1:[1,2,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{1:[2,1,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{1:[2,1,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{1:[1,2,3], null:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{1:[1,2,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 11 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 12 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 13 THEN CAST(map{2:[null,2,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 2 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 3 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 4 THEN CAST(map{2:row(2,4), 1:row(1,null)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 5 THEN CAST(map{} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 6 THEN CAST(map{1:row(2,3), 2:row(2,3)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 7 THEN CAST(map{1:row(2,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 8 THEN CAST(map{null:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 9 THEN CAST(map{1:row(1,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 10 THEN CAST(map{null:null} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 11 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 12 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 13 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
    END AS map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, null, map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [], map{1:1, 2:null}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [null], map{}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, null, map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], null, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [null], map{null:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select null not in (js, json_object('a',3)) from sc2 order by 1;

-- query 10

WITH sc2 AS (
  SELECT
    v1,
    js,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{null:null}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, null:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([null] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:null} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:null, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{1:10, 2:20, null:40} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 4:40} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{2:[2,3,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{1:[1,2,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{1:[2,1,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{1:[2,1,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{1:[1,2,3], null:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{1:[1,2,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 11 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 12 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 13 THEN CAST(map{2:[null,2,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 2 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 3 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 4 THEN CAST(map{2:row(2,4), 1:row(1,null)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 5 THEN CAST(map{} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 6 THEN CAST(map{1:row(2,3), 2:row(2,3)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 7 THEN CAST(map{1:row(2,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 8 THEN CAST(map{null:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 9 THEN CAST(map{1:row(1,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 10 THEN CAST(map{null:null} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 11 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 12 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 13 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
    END AS map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, null, map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [], map{1:1, 2:null}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [null], map{}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, null, map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], null, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [null], map{null:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select js in (1,3,json_object('a',2)) from sc2 order by 1;

-- query 11

WITH sc2 AS (
  SELECT
    v1,
    js,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{null:null}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, null:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([null] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:null} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:null, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{1:10, 2:20, null:40} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 4:40} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{2:[2,3,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{1:[1,2,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{1:[2,1,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{1:[2,1,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{1:[1,2,3], null:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{1:[1,2,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 11 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 12 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 13 THEN CAST(map{2:[null,2,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 2 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 3 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 4 THEN CAST(map{2:row(2,4), 1:row(1,null)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 5 THEN CAST(map{} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 6 THEN CAST(map{1:row(2,3), 2:row(2,3)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 7 THEN CAST(map{1:row(2,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 8 THEN CAST(map{null:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 9 THEN CAST(map{1:row(1,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 10 THEN CAST(map{null:null} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 11 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 12 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 13 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
    END AS map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, null, map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [], map{1:1, 2:null}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [null], map{}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, null, map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], null, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [null], map{null:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select js not in (2,3) from sc2 order by 1;

-- query 12

WITH sc2 AS (
  SELECT
    v1,
    js,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{null:null}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, null:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([null] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:null} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:null, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{1:10, 2:20, null:40} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 4:40} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{2:[2,3,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{1:[1,2,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{1:[2,1,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{1:[2,1,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{1:[1,2,3], null:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{1:[1,2,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 11 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 12 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 13 THEN CAST(map{2:[null,2,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 2 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 3 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 4 THEN CAST(map{2:row(2,4), 1:row(1,null)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 5 THEN CAST(map{} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 6 THEN CAST(map{1:row(2,3), 2:row(2,3)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 7 THEN CAST(map{1:row(2,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 8 THEN CAST(map{null:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 9 THEN CAST(map{1:row(1,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 10 THEN CAST(map{null:null} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 11 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 12 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 13 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
    END AS map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, null, map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [], map{1:1, 2:null}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [null], map{}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, null, map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], null, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [null], map{null:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select js in (1,3,v1,json_object('a',11)) from sc2 order by 1;

-- query 13

WITH sc2 AS (
  SELECT
    v1,
    js,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{null:null}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, null:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([null] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:null} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:null, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{1:10, 2:20, null:40} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 4:40} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{2:[2,3,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{1:[1,2,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{1:[2,1,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{1:[2,1,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{1:[1,2,3], null:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{1:[1,2,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 11 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 12 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 13 THEN CAST(map{2:[null,2,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 2 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 3 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 4 THEN CAST(map{2:row(2,4), 1:row(1,null)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 5 THEN CAST(map{} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 6 THEN CAST(map{1:row(2,3), 2:row(2,3)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 7 THEN CAST(map{1:row(2,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 8 THEN CAST(map{null:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 9 THEN CAST(map{1:row(1,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 10 THEN CAST(map{null:null} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 11 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 12 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 13 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
    END AS map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, null, map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [], map{1:1, 2:null}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [null], map{}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, null, map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], null, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [null], map{null:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select js not in (2,3,v1) from sc2 order by 1;

-- query 14

WITH sc2 AS (
  SELECT
    v1,
    js,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{null:null}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, null:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([null] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:null} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:null, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{1:10, 2:20, null:40} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 4:40} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{2:[2,3,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{1:[1,2,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{1:[2,1,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{1:[2,1,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{1:[1,2,3], null:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{1:[1,2,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 11 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 12 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 13 THEN CAST(map{2:[null,2,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 2 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 3 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 4 THEN CAST(map{2:row(2,4), 1:row(1,null)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 5 THEN CAST(map{} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 6 THEN CAST(map{1:row(2,3), 2:row(2,3)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 7 THEN CAST(map{1:row(2,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 8 THEN CAST(map{null:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 9 THEN CAST(map{1:row(1,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 10 THEN CAST(map{null:null} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 11 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 12 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 13 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
    END AS map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, null, map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [], map{1:1, 2:null}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [null], map{}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, null, map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], null, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [null], map{null:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select js in (json_object("a", 1),json_object("ab", 12, 'bc',4),json_object("ab", 14),json_object("ac", 15, '6b',46)) from sc2 order by 1;

-- query 15

WITH sc2 AS (
  SELECT
    v1,
    js,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{null:null}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, null:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([null] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:null} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:null, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{1:10, 2:20, null:40} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 4:40} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{2:[2,3,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{1:[1,2,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{1:[2,1,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{1:[2,1,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{1:[1,2,3], null:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{1:[1,2,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 11 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 12 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 13 THEN CAST(map{2:[null,2,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 2 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 3 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 4 THEN CAST(map{2:row(2,4), 1:row(1,null)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 5 THEN CAST(map{} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 6 THEN CAST(map{1:row(2,3), 2:row(2,3)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 7 THEN CAST(map{1:row(2,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 8 THEN CAST(map{null:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 9 THEN CAST(map{1:row(1,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 10 THEN CAST(map{null:null} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 11 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 12 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 13 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
    END AS map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, null, map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [], map{1:1, 2:null}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [null], map{}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, null, map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], null, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [null], map{null:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select js not in (json_object("a", 1),json_object("ab", 13, 'b',4) ) from sc2 order by 1;

-- query 16

-- JSON subquery predicate.
WITH sc2 AS (
  SELECT
    v1,
    js,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{null:null}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, null:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([null] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:null} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:null, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{1:10, 2:20, null:40} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 4:40} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{2:[2,3,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{1:[1,2,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{1:[2,1,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{1:[2,1,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{1:[1,2,3], null:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{1:[1,2,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 11 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 12 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 13 THEN CAST(map{2:[null,2,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 2 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 3 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 4 THEN CAST(map{2:row(2,4), 1:row(1,null)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 5 THEN CAST(map{} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 6 THEN CAST(map{1:row(2,3), 2:row(2,3)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 7 THEN CAST(map{1:row(2,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 8 THEN CAST(map{null:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 9 THEN CAST(map{1:row(1,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 10 THEN CAST(map{null:null} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 11 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 12 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 13 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
    END AS map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, null, map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [], map{1:1, 2:null}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [null], map{}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, null, map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], null, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [null], map{null:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select js not in (select js from sc2 where v1>3) from sc2;

-- query 17

-- JSON subquery predicate.
WITH sc2 AS (
  SELECT
    v1,
    js,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{null:null}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, null:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([null] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:null} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:null, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{1:10, 2:20, null:40} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 4:40} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{2:[2,3,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{1:[1,2,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{1:[2,1,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{1:[2,1,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{1:[1,2,3], null:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{1:[1,2,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 11 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 12 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 13 THEN CAST(map{2:[null,2,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 2 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 3 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 4 THEN CAST(map{2:row(2,4), 1:row(1,null)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 5 THEN CAST(map{} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 6 THEN CAST(map{1:row(2,3), 2:row(2,3)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 7 THEN CAST(map{1:row(2,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 8 THEN CAST(map{null:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 9 THEN CAST(map{1:row(1,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 10 THEN CAST(map{null:null} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 11 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 12 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 13 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
    END AS map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, null, map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [], map{1:1, 2:null}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [null], map{}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, null, map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], null, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [null], map{null:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select js in (select js from sc2 where v1>3) from sc2;

-- query 18

-- ARRAY IN predicates
WITH sc2 AS (
  SELECT
    v1,
    js,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{null:null}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, null:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([null] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:null} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:null, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{1:10, 2:20, null:40} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 4:40} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{2:[2,3,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{1:[1,2,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{1:[2,1,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{1:[2,1,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{1:[1,2,3], null:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{1:[1,2,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 11 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 12 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 13 THEN CAST(map{2:[null,2,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 2 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 3 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 4 THEN CAST(map{2:row(2,4), 1:row(1,null)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 5 THEN CAST(map{} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 6 THEN CAST(map{1:row(2,3), 2:row(2,3)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 7 THEN CAST(map{1:row(2,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 8 THEN CAST(map{null:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 9 THEN CAST(map{1:row(1,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 10 THEN CAST(map{null:null} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 11 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 12 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 13 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
    END AS map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, null, map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [], map{1:1, 2:null}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [null], map{}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, null, map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], null, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [null], map{null:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select array1 in ([],[1]) from sc2 order by v1;

-- query 19

WITH sc2 AS (
  SELECT
    v1,
    js,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{null:null}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, null:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([null] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:null} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:null, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{1:10, 2:20, null:40} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 4:40} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{2:[2,3,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{1:[1,2,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{1:[2,1,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{1:[2,1,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{1:[1,2,3], null:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{1:[1,2,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 11 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 12 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 13 THEN CAST(map{2:[null,2,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 2 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 3 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 4 THEN CAST(map{2:row(2,4), 1:row(1,null)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 5 THEN CAST(map{} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 6 THEN CAST(map{1:row(2,3), 2:row(2,3)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 7 THEN CAST(map{1:row(2,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 8 THEN CAST(map{null:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 9 THEN CAST(map{1:row(1,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 10 THEN CAST(map{null:null} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 11 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 12 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 13 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
    END AS map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, null, map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [], map{1:1, 2:null}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [null], map{}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, null, map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], null, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [null], map{null:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select array1 not in ([],[1]) from sc2 order by v1;

-- query 20

WITH sc2 AS (
  SELECT
    v1,
    js,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{null:null}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, null:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([null] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:null} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:null, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{1:10, 2:20, null:40} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 4:40} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{2:[2,3,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{1:[1,2,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{1:[2,1,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{1:[2,1,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{1:[1,2,3], null:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{1:[1,2,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 11 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 12 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 13 THEN CAST(map{2:[null,2,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 2 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 3 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 4 THEN CAST(map{2:row(2,4), 1:row(1,null)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 5 THEN CAST(map{} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 6 THEN CAST(map{1:row(2,3), 2:row(2,3)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 7 THEN CAST(map{1:row(2,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 8 THEN CAST(map{null:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 9 THEN CAST(map{1:row(1,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 10 THEN CAST(map{null:null} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 11 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 12 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 13 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
    END AS map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, null, map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [], map{1:1, 2:null}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [null], map{}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, null, map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], null, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [null], map{null:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select array1 in ([null],[]) from sc2 order by v1;

-- query 21

WITH sc2 AS (
  SELECT
    v1,
    js,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{null:null}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, null:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([null] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:null} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:null, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{1:10, 2:20, null:40} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 4:40} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{2:[2,3,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{1:[1,2,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{1:[2,1,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{1:[2,1,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{1:[1,2,3], null:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{1:[1,2,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 11 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 12 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 13 THEN CAST(map{2:[null,2,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 2 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 3 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 4 THEN CAST(map{2:row(2,4), 1:row(1,null)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 5 THEN CAST(map{} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 6 THEN CAST(map{1:row(2,3), 2:row(2,3)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 7 THEN CAST(map{1:row(2,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 8 THEN CAST(map{null:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 9 THEN CAST(map{1:row(1,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 10 THEN CAST(map{null:null} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 11 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 12 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 13 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
    END AS map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, null, map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [], map{1:1, 2:null}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [null], map{}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, null, map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], null, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [null], map{null:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select array1 not in ([null],[3]) from sc2 order by v1;

-- query 22

WITH sc2 AS (
  SELECT
    v1,
    js,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{null:null}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, null:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([null] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:null} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:null, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{1:10, 2:20, null:40} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 4:40} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{2:[2,3,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{1:[1,2,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{1:[2,1,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{1:[2,1,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{1:[1,2,3], null:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{1:[1,2,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 11 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 12 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 13 THEN CAST(map{2:[null,2,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 2 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 3 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 4 THEN CAST(map{2:row(2,4), 1:row(1,null)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 5 THEN CAST(map{} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 6 THEN CAST(map{1:row(2,3), 2:row(2,3)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 7 THEN CAST(map{1:row(2,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 8 THEN CAST(map{null:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 9 THEN CAST(map{1:row(1,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 10 THEN CAST(map{null:null} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 11 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 12 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 13 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
    END AS map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, null, map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [], map{1:1, 2:null}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [null], map{}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, null, map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], null, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [null], map{null:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select array1 in ([1,null,2],[]) from sc2 order by v1;

-- query 23

WITH sc2 AS (
  SELECT
    v1,
    js,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{null:null}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, null:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([null] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:null} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:null, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{1:10, 2:20, null:40} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 4:40} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{2:[2,3,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{1:[1,2,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{1:[2,1,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{1:[2,1,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{1:[1,2,3], null:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{1:[1,2,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 11 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 12 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 13 THEN CAST(map{2:[null,2,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 2 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 3 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 4 THEN CAST(map{2:row(2,4), 1:row(1,null)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 5 THEN CAST(map{} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 6 THEN CAST(map{1:row(2,3), 2:row(2,3)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 7 THEN CAST(map{1:row(2,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 8 THEN CAST(map{null:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 9 THEN CAST(map{1:row(1,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 10 THEN CAST(map{null:null} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 11 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 12 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 13 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
    END AS map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, null, map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [], map{1:1, 2:null}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [null], map{}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, null, map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], null, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [null], map{null:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select array1 not in ([1,null,2],[]) from sc2 order by v1;

-- query 24

WITH sc2 AS (
  SELECT
    v1,
    js,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{null:null}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, null:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([null] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:null} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:null, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{1:10, 2:20, null:40} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 4:40} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{2:[2,3,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{1:[1,2,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{1:[2,1,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{1:[2,1,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{1:[1,2,3], null:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{1:[1,2,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 11 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 12 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 13 THEN CAST(map{2:[null,2,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 2 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 3 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 4 THEN CAST(map{2:row(2,4), 1:row(1,null)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 5 THEN CAST(map{} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 6 THEN CAST(map{1:row(2,3), 2:row(2,3)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 7 THEN CAST(map{1:row(2,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 8 THEN CAST(map{null:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 9 THEN CAST(map{1:row(1,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 10 THEN CAST(map{null:null} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 11 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 12 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 13 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
    END AS map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, null, map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [], map{1:1, 2:null}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [null], map{}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, null, map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], null, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [null], map{null:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select [] in (array1), [] not in (array1), [null] in (array1), [null] not in (array1),[] in (array2), [] not in (array2), [null] in (array2), [null] not in (array2),[] in (array3), [] not in (array3), [null] in (array3), [null] not in (array3) from sc2 order by 1;

-- query 25

WITH sc2 AS (
  SELECT
    v1,
    js,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{null:null}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, null:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([null] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:null} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:null, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{1:10, 2:20, null:40} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 4:40} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{2:[2,3,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{1:[1,2,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{1:[2,1,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{1:[2,1,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{1:[1,2,3], null:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{1:[1,2,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 11 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 12 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 13 THEN CAST(map{2:[null,2,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 2 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 3 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 4 THEN CAST(map{2:row(2,4), 1:row(1,null)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 5 THEN CAST(map{} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 6 THEN CAST(map{1:row(2,3), 2:row(2,3)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 7 THEN CAST(map{1:row(2,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 8 THEN CAST(map{null:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 9 THEN CAST(map{1:row(1,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 10 THEN CAST(map{null:null} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 11 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 12 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 13 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
    END AS map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, null, map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [], map{1:1, 2:null}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [null], map{}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, null, map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], null, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [null], map{null:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select array2 in ([],[map{}]),  array2 in ([null],[map{1:3}]), array2 in ([map{1:10, 3:30, 2:20}], [map{}])  from sc2 order by 1;

-- query 26

WITH sc2 AS (
  SELECT
    v1,
    js,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{null:null}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, null:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([null] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:null} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:null, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{1:10, 2:20, null:40} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 4:40} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{2:[2,3,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{1:[1,2,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{1:[2,1,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{1:[2,1,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{1:[1,2,3], null:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{1:[1,2,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 11 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 12 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 13 THEN CAST(map{2:[null,2,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 2 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 3 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 4 THEN CAST(map{2:row(2,4), 1:row(1,null)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 5 THEN CAST(map{} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 6 THEN CAST(map{1:row(2,3), 2:row(2,3)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 7 THEN CAST(map{1:row(2,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 8 THEN CAST(map{null:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 9 THEN CAST(map{1:row(1,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 10 THEN CAST(map{null:null} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 11 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 12 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 13 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
    END AS map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, null, map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [], map{1:1, 2:null}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [null], map{}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, null, map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], null, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [null], map{null:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select array2 not in ([],[map{}]),  array2 not in ([null],[map{1:3}]), array2 not in ([map{1:10, 3:30, 2:20}], [map{3:3}])  from sc2 order by 1;

-- query 27

WITH sc2 AS (
  SELECT
    v1,
    js,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{null:null}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, null:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([null] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:null} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:null, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{1:10, 2:20, null:40} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 4:40} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{2:[2,3,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{1:[1,2,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{1:[2,1,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{1:[2,1,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{1:[1,2,3], null:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{1:[1,2,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 11 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 12 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 13 THEN CAST(map{2:[null,2,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 2 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 3 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 4 THEN CAST(map{2:row(2,4), 1:row(1,null)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 5 THEN CAST(map{} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 6 THEN CAST(map{1:row(2,3), 2:row(2,3)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 7 THEN CAST(map{1:row(2,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 8 THEN CAST(map{null:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 9 THEN CAST(map{1:row(1,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 10 THEN CAST(map{null:null} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 11 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 12 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 13 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
    END AS map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, null, map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [], map{1:1, 2:null}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [null], map{}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, null, map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], null, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [null], map{null:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select array3 in ([]), array3 not in ([]), array3 in ([null]), array3 not in ([null]), array3 in ([row(1, 2)], [null]), array3 not in ([row(1, 2)], [null]) from sc2 order by 1,2,3,4,5,6;

-- query 28

WITH sc2 AS (
  SELECT
    v1,
    js,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{null:null}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, null:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([null] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:null} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:null, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{1:10, 2:20, null:40} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 4:40} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{2:[2,3,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{1:[1,2,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{1:[2,1,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{1:[2,1,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{1:[1,2,3], null:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{1:[1,2,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 11 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 12 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 13 THEN CAST(map{2:[null,2,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 2 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 3 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 4 THEN CAST(map{2:row(2,4), 1:row(1,null)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 5 THEN CAST(map{} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 6 THEN CAST(map{1:row(2,3), 2:row(2,3)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 7 THEN CAST(map{1:row(2,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 8 THEN CAST(map{null:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 9 THEN CAST(map{1:row(1,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 10 THEN CAST(map{null:null} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 11 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 12 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 13 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
    END AS map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, null, map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [], map{1:1, 2:null}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [null], map{}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, null, map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], null, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [null], map{null:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select array1 in (null), array1 not in (null), array2 in (null), array2 not in (null), array3 in (null), array3 not in (null) from sc2 order by 1;

-- query 29

WITH sc2 AS (
  SELECT
    v1,
    js,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{null:null}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, null:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([null] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:null} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:null, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{1:10, 2:20, null:40} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 4:40} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{2:[2,3,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{1:[1,2,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{1:[2,1,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{1:[2,1,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{1:[1,2,3], null:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{1:[1,2,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 11 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 12 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 13 THEN CAST(map{2:[null,2,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 2 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 3 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 4 THEN CAST(map{2:row(2,4), 1:row(1,null)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 5 THEN CAST(map{} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 6 THEN CAST(map{1:row(2,3), 2:row(2,3)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 7 THEN CAST(map{1:row(2,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 8 THEN CAST(map{null:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 9 THEN CAST(map{1:row(1,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 10 THEN CAST(map{null:null} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 11 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 12 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 13 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
    END AS map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, null, map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [], map{1:1, 2:null}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [null], map{}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, null, map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], null, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [null], map{null:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select null in (array1), null not in (array1), null in (array2), null not in (array2), null in (array3), null not in (array3) from sc2 order by 1;

-- query 30

-- Type mismatch: ARRAY<INT> vs type from array<map>
-- @expect_error=of in predict are not compatible
WITH sc2 AS (
  SELECT
    v1,
    js,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{null:null}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, null:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([null] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:null} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:null, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{1:10, 2:20, null:40} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 4:40} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{2:[2,3,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{1:[1,2,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{1:[2,1,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{1:[2,1,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{1:[1,2,3], null:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{1:[1,2,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 11 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 12 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 13 THEN CAST(map{2:[null,2,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 2 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 3 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 4 THEN CAST(map{2:row(2,4), 1:row(1,null)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 5 THEN CAST(map{} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 6 THEN CAST(map{1:row(2,3), 2:row(2,3)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 7 THEN CAST(map{1:row(2,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 8 THEN CAST(map{null:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 9 THEN CAST(map{1:row(1,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 10 THEN CAST(map{null:null} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 11 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 12 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 13 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
    END AS map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, null, map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [], map{1:1, 2:null}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [null], map{}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, null, map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], null, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [null], map{null:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select array2 in (map{}) from sc2 order by 1;

-- query 31

-- Type mismatch: ARRAY<STRUCT> vs INT
-- @expect_error=in predicate type
WITH sc2 AS (
  SELECT
    v1,
    js,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{null:null}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, null:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([null] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:null} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:null, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{1:10, 2:20, null:40} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 4:40} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{2:[2,3,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{1:[1,2,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{1:[2,1,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{1:[2,1,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{1:[1,2,3], null:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{1:[1,2,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 11 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 12 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 13 THEN CAST(map{2:[null,2,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 2 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 3 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 4 THEN CAST(map{2:row(2,4), 1:row(1,null)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 5 THEN CAST(map{} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 6 THEN CAST(map{1:row(2,3), 2:row(2,3)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 7 THEN CAST(map{1:row(2,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 8 THEN CAST(map{null:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 9 THEN CAST(map{1:row(1,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 10 THEN CAST(map{null:null} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 11 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 12 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 13 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
    END AS map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, null, map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [], map{1:1, 2:null}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [null], map{}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, null, map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], null, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [null], map{null:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select array3 in (33,1) from sc2 order by 1;

-- query 32

-- MAP IN predicates
WITH sc2 AS (
  SELECT
    v1,
    js,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{null:null}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, null:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([null] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:null} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:null, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{1:10, 2:20, null:40} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 4:40} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{2:[2,3,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{1:[1,2,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{1:[2,1,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{1:[2,1,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{1:[1,2,3], null:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{1:[1,2,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 11 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 12 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 13 THEN CAST(map{2:[null,2,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 2 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 3 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 4 THEN CAST(map{2:row(2,4), 1:row(1,null)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 5 THEN CAST(map{} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 6 THEN CAST(map{1:row(2,3), 2:row(2,3)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 7 THEN CAST(map{1:row(2,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 8 THEN CAST(map{null:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 9 THEN CAST(map{1:row(1,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 10 THEN CAST(map{null:null} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 11 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 12 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 13 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
    END AS map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, null, map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [], map{1:1, 2:null}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [null], map{}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, null, map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], null, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [null], map{null:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select map1 in (map{}), map1 not in (map{}), map2 in (map{}), map2 not in (map{}), map3 in (map{}), map3 not in (map{}) from sc2 order by 1;

-- query 33

WITH sc2 AS (
  SELECT
    v1,
    js,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{null:null}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, null:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([null] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:null} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:null, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{1:10, 2:20, null:40} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 4:40} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{2:[2,3,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{1:[1,2,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{1:[2,1,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{1:[2,1,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{1:[1,2,3], null:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{1:[1,2,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 11 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 12 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 13 THEN CAST(map{2:[null,2,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 2 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 3 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 4 THEN CAST(map{2:row(2,4), 1:row(1,null)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 5 THEN CAST(map{} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 6 THEN CAST(map{1:row(2,3), 2:row(2,3)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 7 THEN CAST(map{1:row(2,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 8 THEN CAST(map{null:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 9 THEN CAST(map{1:row(1,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 10 THEN CAST(map{null:null} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 11 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 12 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 13 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
    END AS map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, null, map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [], map{1:1, 2:null}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [null], map{}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, null, map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], null, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [null], map{null:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select map1 in (null), map1 not in (null), map2 in (null), map2 not in (null), map3 in (null), map3 not in (null) from sc2 order by 1;

-- query 34

WITH sc2 AS (
  SELECT
    v1,
    js,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{null:null}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, null:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([null] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:null} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:null, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{1:10, 2:20, null:40} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 4:40} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{2:[2,3,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{1:[1,2,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{1:[2,1,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{1:[2,1,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{1:[1,2,3], null:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{1:[1,2,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 11 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 12 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 13 THEN CAST(map{2:[null,2,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 2 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 3 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 4 THEN CAST(map{2:row(2,4), 1:row(1,null)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 5 THEN CAST(map{} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 6 THEN CAST(map{1:row(2,3), 2:row(2,3)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 7 THEN CAST(map{1:row(2,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 8 THEN CAST(map{null:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 9 THEN CAST(map{1:row(1,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 10 THEN CAST(map{null:null} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 11 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 12 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 13 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
    END AS map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, null, map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [], map{1:1, 2:null}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [null], map{}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, null, map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], null, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [null], map{null:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select null in (map1), null not in (map1), null in (map2), null not in (map2), null in (map3), null not in (map3) from sc2 order by 1;

-- query 35

WITH sc2 AS (
  SELECT
    v1,
    js,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{null:null}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, null:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([null] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:null} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:null, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{1:10, 2:20, null:40} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 4:40} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{2:[2,3,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{1:[1,2,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{1:[2,1,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{1:[2,1,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{1:[1,2,3], null:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{1:[1,2,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 11 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 12 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 13 THEN CAST(map{2:[null,2,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 2 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 3 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 4 THEN CAST(map{2:row(2,4), 1:row(1,null)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 5 THEN CAST(map{} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 6 THEN CAST(map{1:row(2,3), 2:row(2,3)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 7 THEN CAST(map{1:row(2,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 8 THEN CAST(map{null:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 9 THEN CAST(map{1:row(1,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 10 THEN CAST(map{null:null} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 11 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 12 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 13 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
    END AS map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, null, map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [], map{1:1, 2:null}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [null], map{}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, null, map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], null, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [null], map{null:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select map{} in (map1), map() not in (map1), map() in (map2), map() not in (map2), map() in (map3), map() not in (map3) from sc2 order by 1;

-- query 36

WITH sc2 AS (
  SELECT
    v1,
    js,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{null:null}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, null:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([null] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:null} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:null, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{1:10, 2:20, null:40} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 4:40} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{2:[2,3,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{1:[1,2,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{1:[2,1,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{1:[2,1,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{1:[1,2,3], null:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{1:[1,2,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 11 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 12 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 13 THEN CAST(map{2:[null,2,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 2 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 3 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 4 THEN CAST(map{2:row(2,4), 1:row(1,null)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 5 THEN CAST(map{} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 6 THEN CAST(map{1:row(2,3), 2:row(2,3)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 7 THEN CAST(map{1:row(2,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 8 THEN CAST(map{null:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 9 THEN CAST(map{1:row(1,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 10 THEN CAST(map{null:null} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 11 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 12 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 13 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
    END AS map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, null, map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [], map{1:1, 2:null}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [null], map{}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, null, map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], null, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [null], map{null:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select map1 in (map{1:10, 2:20, 4:40}, map{}), map2 in (map{2:[2,3,4], 1:[1,2,3]}, map{2:[2,3,4], 1:[1,22,3]}), map3 not in (map{2:row(2,4), 1:row(1,2)},map{2:row(2,4), 1:row(1,22)}) from sc2 order by 1;

-- query 37

WITH sc2 AS (
  SELECT
    v1,
    js,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{null:null}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, null:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([null] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:null} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:null, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{1:10, 2:20, null:40} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 4:40} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{2:[2,3,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{1:[1,2,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{1:[2,1,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{1:[2,1,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{1:[1,2,3], null:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{1:[1,2,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 11 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 12 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 13 THEN CAST(map{2:[null,2,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 2 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 3 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 4 THEN CAST(map{2:row(2,4), 1:row(1,null)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 5 THEN CAST(map{} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 6 THEN CAST(map{1:row(2,3), 2:row(2,3)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 7 THEN CAST(map{1:row(2,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 8 THEN CAST(map{null:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 9 THEN CAST(map{1:row(1,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 10 THEN CAST(map{null:null} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 11 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 12 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 13 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
    END AS map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, null, map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [], map{1:1, 2:null}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [null], map{}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, null, map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], null, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [null], map{null:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select map2 not in (map{2:[2,3,4], 1:[1,2,3]},null), map3 in (map{2:row(2,4), 1:row(1,2)}, null) from sc2 order by 1;

-- query 38

WITH sc2 AS (
  SELECT
    v1,
    js,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{null:null}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, null:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([null] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:null} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:null, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{1:10, 2:20, null:40} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 4:40} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{2:[2,3,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{1:[1,2,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{1:[2,1,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{1:[2,1,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{1:[1,2,3], null:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{1:[1,2,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 11 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 12 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 13 THEN CAST(map{2:[null,2,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 2 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 3 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 4 THEN CAST(map{2:row(2,4), 1:row(1,null)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 5 THEN CAST(map{} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 6 THEN CAST(map{1:row(2,3), 2:row(2,3)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 7 THEN CAST(map{1:row(2,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 8 THEN CAST(map{null:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 9 THEN CAST(map{1:row(1,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 10 THEN CAST(map{null:null} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 11 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 12 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 13 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
    END AS map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, null, map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [], map{1:1, 2:null}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [null], map{}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, null, map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], null, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [null], map{null:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select map2 in (map{1:[1,2,null],2:[2,4,null]},map{2:[2,3,4], 1:[1,2,3]}) from sc2 order by 1;

-- query 39

WITH sc2 AS (
  SELECT
    v1,
    js,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{null:null}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, null:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([null] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:null} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:null, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{1:10, 2:20, null:40} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 4:40} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{2:[2,3,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{1:[1,2,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{1:[2,1,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{1:[2,1,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{1:[1,2,3], null:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{1:[1,2,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 11 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 12 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 13 THEN CAST(map{2:[null,2,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 2 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 3 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 4 THEN CAST(map{2:row(2,4), 1:row(1,null)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 5 THEN CAST(map{} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 6 THEN CAST(map{1:row(2,3), 2:row(2,3)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 7 THEN CAST(map{1:row(2,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 8 THEN CAST(map{null:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 9 THEN CAST(map{1:row(1,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 10 THEN CAST(map{null:null} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 11 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 12 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 13 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
    END AS map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, null, map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [], map{1:1, 2:null}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [null], map{}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, null, map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], null, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [null], map{null:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select map2 not in (map{1:[1,2,null],2:[2,4,null]},map{2:[2,3,4], 1:[1,2,3]}) from sc2 order by 1;

-- query 40

-- MAP<INT,INT> vs MAP<VARCHAR,VARCHAR>: FE coerces, returns 0 (not equal)
WITH sc2 AS (
  SELECT
    v1,
    js,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{null:null}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, null:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([null] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:null} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:null, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{1:10, 2:20, null:40} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 4:40} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{2:[2,3,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{1:[1,2,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{1:[2,1,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{1:[2,1,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{1:[1,2,3], null:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{1:[1,2,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 11 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 12 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 13 THEN CAST(map{2:[null,2,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 2 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 3 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 4 THEN CAST(map{2:row(2,4), 1:row(1,null)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 5 THEN CAST(map{} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 6 THEN CAST(map{1:row(2,3), 2:row(2,3)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 7 THEN CAST(map{1:row(2,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 8 THEN CAST(map{null:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 9 THEN CAST(map{1:row(1,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 10 THEN CAST(map{null:null} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 11 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 12 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 13 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
    END AS map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, null, map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [], map{1:1, 2:null}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [null], map{}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, null, map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], null, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [null], map{null:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select map1 in (map{'a':'b'}) from sc2 order by 1;

-- query 41

-- MAP<INT,ARRAY<INT>> vs MAP<VARCHAR,ARRAY<TINYINT>>: FE coerces, returns 0
WITH sc2 AS (
  SELECT
    v1,
    js,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{null:null}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, null:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([null] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:null} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:null, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{1:10, 2:20, null:40} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 4:40} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{2:[2,3,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{1:[1,2,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{1:[2,1,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{1:[2,1,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{1:[1,2,3], null:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{1:[1,2,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 11 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 12 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 13 THEN CAST(map{2:[null,2,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 2 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 3 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 4 THEN CAST(map{2:row(2,4), 1:row(1,null)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 5 THEN CAST(map{} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 6 THEN CAST(map{1:row(2,3), 2:row(2,3)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 7 THEN CAST(map{1:row(2,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 8 THEN CAST(map{null:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 9 THEN CAST(map{1:row(1,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 10 THEN CAST(map{null:null} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 11 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 12 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 13 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
    END AS map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, null, map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [], map{1:1, 2:null}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [null], map{}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, null, map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], null, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [null], map{null:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select map2 in (map{'2':[3]}) from sc2 order by 1;

-- query 42

-- Type mismatch: MAP<INT,INT> vs double
-- @expect_error=in predicate type
WITH sc2 AS (
  SELECT
    v1,
    js,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{null:null}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, null:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([null] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:null} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:null, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{1:10, 2:20, null:40} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 4:40} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{2:[2,3,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{1:[1,2,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{1:[2,1,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{1:[2,1,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{1:[1,2,3], null:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{1:[1,2,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 11 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 12 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 13 THEN CAST(map{2:[null,2,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 2 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 3 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 4 THEN CAST(map{2:row(2,4), 1:row(1,null)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 5 THEN CAST(map{} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 6 THEN CAST(map{1:row(2,3), 2:row(2,3)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 7 THEN CAST(map{1:row(2,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 8 THEN CAST(map{null:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 9 THEN CAST(map{1:row(1,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 10 THEN CAST(map{null:null} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 11 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 12 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 13 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
    END AS map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, null, map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [], map{1:1, 2:null}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [null], map{}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, null, map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], null, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [null], map{null:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select map1 in (2,3) from sc2 order by 1;

-- query 43

-- STRUCT IN predicates
WITH sc2 AS (
  SELECT
    v1,
    js,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{null:null}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, null:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([null] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:null} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:null, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{1:10, 2:20, null:40} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 4:40} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{2:[2,3,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{1:[1,2,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{1:[2,1,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{1:[2,1,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{1:[1,2,3], null:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{1:[1,2,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 11 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 12 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 13 THEN CAST(map{2:[null,2,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 2 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 3 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 4 THEN CAST(map{2:row(2,4), 1:row(1,null)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 5 THEN CAST(map{} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 6 THEN CAST(map{1:row(2,3), 2:row(2,3)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 7 THEN CAST(map{1:row(2,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 8 THEN CAST(map{null:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 9 THEN CAST(map{1:row(1,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 10 THEN CAST(map{null:null} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 11 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 12 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 13 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
    END AS map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, null, map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [], map{1:1, 2:null}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [null], map{}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, null, map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], null, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [null], map{null:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select st1 in (null), st1 not in (null) from sc2 order by 1;

-- query 44

WITH sc2 AS (
  SELECT
    v1,
    js,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{null:null}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, null:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([null] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:null} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:null, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{1:10, 2:20, null:40} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 4:40} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{2:[2,3,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{1:[1,2,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{1:[2,1,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{1:[2,1,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{1:[1,2,3], null:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{1:[1,2,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 11 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 12 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 13 THEN CAST(map{2:[null,2,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 2 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 3 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 4 THEN CAST(map{2:row(2,4), 1:row(1,null)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 5 THEN CAST(map{} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 6 THEN CAST(map{1:row(2,3), 2:row(2,3)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 7 THEN CAST(map{1:row(2,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 8 THEN CAST(map{null:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 9 THEN CAST(map{1:row(1,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 10 THEN CAST(map{null:null} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 11 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 12 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 13 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
    END AS map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, null, map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [], map{1:1, 2:null}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [null], map{}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, null, map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], null, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [null], map{null:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select null in (st1), null not in (st1) from sc2 order by 1;

-- query 45

-- row(null,null) inferred as struct<col1 bool, col2 bool> (2 fields), incompatible with st1 (4 fields)
-- @expect_error=of in predict are not compatible
WITH sc2 AS (
  SELECT
    v1,
    js,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{null:null}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, null:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([null] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:null} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:null, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{1:10, 2:20, null:40} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 4:40} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{2:[2,3,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{1:[1,2,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{1:[2,1,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{1:[2,1,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{1:[1,2,3], null:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{1:[1,2,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 11 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 12 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 13 THEN CAST(map{2:[null,2,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 2 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 3 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 4 THEN CAST(map{2:row(2,4), 1:row(1,null)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 5 THEN CAST(map{} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 6 THEN CAST(map{1:row(2,3), 2:row(2,3)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 7 THEN CAST(map{1:row(2,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 8 THEN CAST(map{null:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 9 THEN CAST(map{1:row(1,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 10 THEN CAST(map{null:null} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 11 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 12 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 13 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
    END AS map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, null, map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [], map{1:1, 2:null}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [null], map{}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, null, map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], null, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [null], map{null:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select st1 in (row(null, null)), st1 not in (row(null,null)) from sc2 order by 1;

-- query 46

-- row(null,null) inferred as struct<col1 bool, col2 bool> (2 fields), incompatible with st1 (4 fields)
-- @expect_error=of in predict are not compatible
WITH sc2 AS (
  SELECT
    v1,
    js,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{null:null}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, null:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([null] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:null} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:null, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{1:10, 2:20, null:40} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 4:40} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{2:[2,3,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{1:[1,2,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{1:[2,1,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{1:[2,1,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{1:[1,2,3], null:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{1:[1,2,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 11 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 12 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 13 THEN CAST(map{2:[null,2,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 2 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 3 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 4 THEN CAST(map{2:row(2,4), 1:row(1,null)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 5 THEN CAST(map{} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 6 THEN CAST(map{1:row(2,3), 2:row(2,3)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 7 THEN CAST(map{1:row(2,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 8 THEN CAST(map{null:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 9 THEN CAST(map{1:row(1,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 10 THEN CAST(map{null:null} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 11 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 12 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 13 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
    END AS map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, null, map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [], map{1:1, 2:null}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [null], map{}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, null, map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], null, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [null], map{null:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select row(null,null) in (st1), row(null,null) not in (st1) from sc2 order by 1;

-- query 47

WITH sc2 AS (
  SELECT
    v1,
    js,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{null:null}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, null:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([null] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:null} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:null, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{1:10, 2:20, null:40} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 4:40} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{2:[2,3,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{1:[1,2,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{1:[2,1,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{1:[2,1,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{1:[1,2,3], null:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{1:[1,2,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 11 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 12 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 13 THEN CAST(map{2:[null,2,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 2 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 3 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 4 THEN CAST(map{2:row(2,4), 1:row(1,null)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 5 THEN CAST(map{} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 6 THEN CAST(map{1:row(2,3), 2:row(2,3)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 7 THEN CAST(map{1:row(2,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 8 THEN CAST(map{null:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 9 THEN CAST(map{1:row(1,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 10 THEN CAST(map{null:null} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 11 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 12 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 13 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
    END AS map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, null, map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [], map{1:1, 2:null}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [null], map{}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, null, map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], null, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [null], map{null:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select st1 in (row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)), row(12, [2,null,3], map{11:1, 2:2}, row(31, 2))) from sc2 order by 1;

-- query 48

WITH sc2 AS (
  SELECT
    v1,
    js,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{null:null}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, null:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([null] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:null} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:null, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{1:10, 2:20, null:40} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 4:40} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{2:[2,3,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{1:[1,2,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{1:[2,1,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{1:[2,1,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{1:[1,2,3], null:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{1:[1,2,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 11 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 12 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 13 THEN CAST(map{2:[null,2,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 2 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 3 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 4 THEN CAST(map{2:row(2,4), 1:row(1,null)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 5 THEN CAST(map{} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 6 THEN CAST(map{1:row(2,3), 2:row(2,3)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 7 THEN CAST(map{1:row(2,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 8 THEN CAST(map{null:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 9 THEN CAST(map{1:row(1,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 10 THEN CAST(map{null:null} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 11 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 12 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 13 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
    END AS map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, null, map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [], map{1:1, 2:null}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [null], map{}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, null, map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], null, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [null], map{null:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select st1 not in (row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)), row(12, [2,null,3], map{11:1, 2:2}, row(31, 2))) from sc2 order by 1;

-- query 49

WITH sc2 AS (
  SELECT
    v1,
    js,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{null:null}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, null:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([null] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:null} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:null, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{1:10, 2:20, null:40} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 4:40} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{2:[2,3,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{1:[1,2,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{1:[2,1,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{1:[2,1,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{1:[1,2,3], null:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{1:[1,2,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 11 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 12 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 13 THEN CAST(map{2:[null,2,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 2 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 3 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 4 THEN CAST(map{2:row(2,4), 1:row(1,null)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 5 THEN CAST(map{} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 6 THEN CAST(map{1:row(2,3), 2:row(2,3)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 7 THEN CAST(map{1:row(2,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 8 THEN CAST(map{null:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 9 THEN CAST(map{1:row(1,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 10 THEN CAST(map{null:null} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 11 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 12 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 13 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
    END AS map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, null, map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [], map{1:1, 2:null}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [null], map{}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, null, map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], null, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [null], map{null:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select st1 in (row(1, [2,1,null], map{1:1, 2:2}, row(1, 2)),row(1, [2,1,3], map{1:1, 2:2}, row(3, 2))) from sc2 order by 1;

-- query 50

WITH sc2 AS (
  SELECT
    v1,
    js,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{null:null}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, null:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([null] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:null} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:null, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{1:10, 2:20, null:40} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 4:40} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{2:[2,3,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{1:[1,2,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{1:[2,1,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{1:[2,1,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{1:[1,2,3], null:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{1:[1,2,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 11 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 12 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 13 THEN CAST(map{2:[null,2,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 2 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 3 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 4 THEN CAST(map{2:row(2,4), 1:row(1,null)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 5 THEN CAST(map{} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 6 THEN CAST(map{1:row(2,3), 2:row(2,3)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 7 THEN CAST(map{1:row(2,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 8 THEN CAST(map{null:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 9 THEN CAST(map{1:row(1,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 10 THEN CAST(map{null:null} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 11 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 12 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 13 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
    END AS map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, null, map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [], map{1:1, 2:null}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [null], map{}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, null, map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], null, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [null], map{null:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select st1 not in (row(1, [2,1,3], map{1:1, 2:2}, row(1, 2)),row(1, [2,1,3], map{1:1, 2:2}, row(3, 2))) from sc2 order by 1;

-- query 51

-- Type mismatch: STRUCT vs MAP
-- @expect_error=of in predict are not compatible
WITH sc2 AS (
  SELECT
    v1,
    js,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{null:null}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, null:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([null] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:null} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:null, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{1:10, 2:20, null:40} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 4:40} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{2:[2,3,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{1:[1,2,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{1:[2,1,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{1:[2,1,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{1:[1,2,3], null:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{1:[1,2,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 11 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 12 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 13 THEN CAST(map{2:[null,2,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 2 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 3 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 4 THEN CAST(map{2:row(2,4), 1:row(1,null)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 5 THEN CAST(map{} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 6 THEN CAST(map{1:row(2,3), 2:row(2,3)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 7 THEN CAST(map{1:row(2,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 8 THEN CAST(map{null:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 9 THEN CAST(map{1:row(1,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 10 THEN CAST(map{null:null} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 11 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 12 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 13 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
    END AS map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, null, map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [], map{1:1, 2:null}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [null], map{}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, null, map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], null, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [null], map{null:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select st1 in (map{}) from sc2 order by 1;

-- query 52

-- Type mismatch: STRUCT vs MAP (not in)
-- @expect_error=of in predict are not compatible
WITH sc2 AS (
  SELECT
    v1,
    js,
    array1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS ARRAY<MAP<INT, INT>>)
      WHEN 2 THEN CAST([map{null:null}] AS ARRAY<MAP<INT, INT>>)
      WHEN 3 THEN CAST([] AS ARRAY<MAP<INT, INT>>)
      WHEN 4 THEN CAST([map{2:20, null:10}] AS ARRAY<MAP<INT, INT>>)
      WHEN 5 THEN CAST([null] AS ARRAY<MAP<INT, INT>>)
      WHEN 6 THEN CAST([map{}] AS ARRAY<MAP<INT, INT>>)
      WHEN 7 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 8 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 9 THEN CAST([map{1:10, 3:30, 2:20}] AS ARRAY<MAP<INT, INT>>)
      WHEN 10 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 11 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 12 THEN CAST([map{2:20, 1:10, null:30}] AS ARRAY<MAP<INT, INT>>)
      WHEN 13 THEN CAST([map{2:20, 1:10}] AS ARRAY<MAP<INT, INT>>)
    END AS array2,
    array3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, INT>)
      WHEN 2 THEN CAST(map{2:null} AS MAP<INT, INT>)
      WHEN 3 THEN CAST(map{2:20, 1:null, 3:30} AS MAP<INT, INT>)
      WHEN 4 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 5 THEN CAST(map{1:10, 2:20, null:40} AS MAP<INT, INT>)
      WHEN 6 THEN CAST(map{1:10, 2:20, 4:40} AS MAP<INT, INT>)
      WHEN 7 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 8 THEN CAST(map{2:20, 1:10, 3:30} AS MAP<INT, INT>)
      WHEN 9 THEN CAST(map{1:10, 2:20, 3:30} AS MAP<INT, INT>)
      WHEN 10 THEN CAST(map{1:10, 4:40} AS MAP<INT, INT>)
      WHEN 11 THEN CAST(map{} AS MAP<INT, INT>)
      WHEN 12 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
      WHEN 13 THEN CAST(map{2:20, 1:10, null:30} AS MAP<INT, INT>)
    END AS map1,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, ARRAY<INT>>)
      WHEN 2 THEN CAST(map{2:[2,3,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 3 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 4 THEN CAST(map{2:[3,2,4], 1:[1,2,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 5 THEN CAST(map{1:[1,2,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 6 THEN CAST(map{1:[2,1,3], 2:[2,3,4]} AS MAP<INT, ARRAY<INT>>)
      WHEN 7 THEN CAST(map{1:[2,1,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 8 THEN CAST(map{1:[1,2,3], null:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 9 THEN CAST(map{1:[1,2,3], 2:[2,4,3]} AS MAP<INT, ARRAY<INT>>)
      WHEN 10 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 11 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 12 THEN CAST(map{2:[2,null,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
      WHEN 13 THEN CAST(map{2:[null,2,4], 1:[1,2,null]} AS MAP<INT, ARRAY<INT>>)
    END AS map2,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 2 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 3 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 4 THEN CAST(map{2:row(2,4), 1:row(1,null)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 5 THEN CAST(map{} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 6 THEN CAST(map{1:row(2,3), 2:row(2,3)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 7 THEN CAST(map{1:row(2,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 8 THEN CAST(map{null:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 9 THEN CAST(map{1:row(1,3), 2:row(2,4)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 10 THEN CAST(map{null:null} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 11 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 12 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
      WHEN 13 THEN CAST(map{2:row(2,4), 1:row(1,2)} AS MAP<INT, STRUCT<c INT, b INT>>)
    END AS map3,
    CASE fixture_row_ordinal
      WHEN 1 THEN CAST(null AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 2 THEN CAST(row(1, [2,1,3], map{2:2, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 3 THEN CAST(row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 4 THEN CAST(row(1, null, map{1:1, 2:2}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 5 THEN CAST(row(1, [], map{1:1, 2:null}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 6 THEN CAST(row(1, [null], map{}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 7 THEN CAST(row(1, [1,2,3], map{2:null}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 8 THEN CAST(row(1, null, map{2:2, 3:3, 1:1}, row(1, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 9 THEN CAST(row(1, [1,2,3], map{2:2, 3:3, 1:1}, null) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 10 THEN CAST(row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 11 THEN CAST(row(1, [2,1,null], null, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 12 THEN CAST(row(1, [null], map{null:2, 1:1}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
      WHEN 13 THEN CAST(row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)) AS STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>)
    END AS st1
  FROM ${case_db}.sc2_seed
)
select st1 not in (map{}) from sc2 order by 1;
