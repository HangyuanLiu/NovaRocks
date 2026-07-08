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
-- 1. Validate refresh and rewrite remain correct over Iceberg append base tables
--    when rows span month/day MV expression boundaries.
-- 2. Cover manual MV refresh after incremental inserts and filtered rewrite on
--    date ranges.

-- query 1
create database db_${uuid0};

-- query 2
use db_${uuid0};

-- query 3
-- Scenario 1: month-granularity MV expression over append-table rows spanning multiple months.
CREATE TABLE mock_tbl_many (
  k1 date,
  k2 int,
  v1 int
)
TBLPROPERTIES ("format-version" = "3");

-- query 4
insert into mock_tbl_many values
  ('2021-07-23',2,10),
  ('2021-07-27',2,10),
  ('2021-07-29',2,10),
  ('2021-08-02',2,10);

-- query 5
create materialized view mv_many_to_many
partition by date_trunc('month', k1)
distributed by hash(k2) buckets 3
refresh deferred manual
properties('replication_num' = '1', 'partition_refresh_number' = '1')
as select k1, k2, v1 from mock_tbl_many;

-- query 6
refresh materialized view mv_many_to_many with sync mode;

-- query 7
-- @result_contains=mv_many_to_many
SET enable_materialized_view_rewrite = true;
EXPLAIN select k1, k2, v1 from mock_tbl_many order by k1, k2;

-- query 8
-- @result_contains=mv_many_to_many
SET enable_materialized_view_rewrite = true;
EXPLAIN
select k1, k2, v1 from mock_tbl_many
where k1 >= '2021-07-23' and k1 < '2021-07-26'
order by k1, k2;

-- query 9
select * from mv_many_to_many order by k1, k2;

-- query 10
insert into mock_tbl_many values ('2021-07-29',3,10), ('2021-08-02',3,10);

-- query 11
refresh materialized view mv_many_to_many with sync mode;

-- query 12
-- @result_contains=mv_many_to_many
SET enable_materialized_view_rewrite = true;
EXPLAIN select k1, k2, v1 from mock_tbl_many order by k1, k2;

-- query 13
select * from mv_many_to_many order by k1, k2;

-- query 14
drop materialized view mv_many_to_many;

-- query 15
drop table mock_tbl_many;

-- query 16
-- Scenario 2: day-granularity MV expression over append-table rows spanning multiple days.
CREATE TABLE mock_tbl_one (
  k1 date,
  k2 int,
  v1 int
)
TBLPROPERTIES ("format-version" = "3");

-- query 17
insert into mock_tbl_one values
  ('2021-07-01',2,10),
  ('2021-08-01',2,10),
  ('2021-08-02',2,10),
  ('2021-09-03',2,10);

-- query 18
create materialized view mv_one_to_many
partition by date_trunc('day', k1)
distributed by hash(k2) buckets 3
refresh deferred manual
properties('replication_num' = '1', 'partition_refresh_number' = '1')
as select k1, k2, v1 from mock_tbl_one;

-- query 19
refresh materialized view mv_one_to_many with sync mode;

-- query 20
-- @result_contains=mv_one_to_many
SET enable_materialized_view_rewrite = true;
EXPLAIN select k1, k2, v1 from mock_tbl_one order by k1, k2;

-- query 21
-- @result_contains=mv_one_to_many
SET enable_materialized_view_rewrite = true;
EXPLAIN
select k1, k2, v1 from mock_tbl_one
where k1 >= '2021-08-01' and k1 < '2021-09-01'
order by k1, k2;

-- query 22
select * from mv_one_to_many order by k1, k2;

-- query 23
insert into mock_tbl_one values ('2021-08-02',3,10), ('2021-09-03',3,10);

-- query 24
refresh materialized view mv_one_to_many with sync mode;

-- query 25
-- @result_contains=mv_one_to_many
SET enable_materialized_view_rewrite = true;
EXPLAIN select k1, k2, v1 from mock_tbl_one order by k1, k2;

-- query 26
select * from mv_one_to_many order by k1, k2;

-- query 27
drop database db_${uuid0} force;
