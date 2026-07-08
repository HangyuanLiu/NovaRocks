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
-- 1. Validate CREATE MATERIALIZED VIEW rejects incompatible output types.
-- 2. Cover planner/type validation failures during MV definition.
-- Source: dev/test/sql/test_materialized_view/T/test_create_mv_with_type_incompatible

-- query 1
CREATE TABLE t1 (dt date, val int, col1 char(8), col2 varchar(8))
TBLPROPERTIES ("format-version" = "3");

-- query 2
insert into t1 values
  ('2023-12-01', 100, 'a', 'b'),
  ('2023-12-01', 200, 'c', 'd'),
  ('2023-12-02', 300, 'e', 'f'),
  ('2023-12-02', 400, 'g', 'h'),
  ('2023-12-03', 500, 'i', 'j');

-- query 3
CREATE MATERIALIZED VIEW test_mv1 PARTITION BY dt
REFRESH DEFERRED MANUAL
AS SELECT   CASE WHEN (`col1` = 'a') THEN '床前明月光'
   WHEN (`col1` = 'b') THEN '疑是地上霜'
   WHEN (`col1` = 'c') THEN '举头望明月'
   WHEN (`col1` = 'd') THEN '低头思故乡'
   WHEN (`col1` = 'e') THEN '一二三四五六七八九十'
   WHEN (`col1` = 'f') THEN '百千万亿ABCDEFG'
   ELSE col1 END AS new_col1,
   col1, col2, dt from t1;

-- query 4
refresh materialized view test_mv1 with sync mode;

-- query 5
select count(*) from test_mv1;

-- query 6
select * from test_mv1 order by dt, new_col1;
