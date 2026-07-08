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
-- 1. Validate MV status reporting through SHOW or information_schema metadata.
-- 2. Cover state transitions after refresh or base-object changes.
-- Source: dev/test/sql/test_materialized_view/T/test_materialized_view_status

-- query 1
CREATE TABLE `t1` (
  `k1` date NULL COMMENT "",
  `v1` int(11) NULL COMMENT "",
  `v2` int(11) NULL COMMENT ""
)
TBLPROPERTIES ("format-version" = "3");

-- query 2
insert into t1 values ("2019-01-01",1,1),("2019-01-01",1,2),("2019-01-01",2,1),("2019-01-01",2,2),
                      ("2023-01-11",1,1),("2023-01-11",1,2),("2023-02-11",2,1),("2023-01-11",2,2),
                      ("2023-03-22",1,1),("2023-05-22",1,2),("2023-04-22",2,1),("2023-05-01",2,2);

-- query 3
CREATE MATERIALIZED VIEW mv1
               PARTITION BY k1
               DISTRIBUTED BY HASH(k1) BUCKETS 10
               REFRESH ASYNC
               AS SELECT k1, sum(v1) as sum_v1 FROM t1 group by k1;

-- query 4
drop table t1;

-- query 5
CREATE TABLE `t1` (
  `k1` date NULL COMMENT "",
  `v1` int(11) NULL COMMENT "",
  `v2` int(11) NULL COMMENT ""
)
TBLPROPERTIES ("format-version" = "3");

-- query 6
ALTER MATERIALIZED VIEW mv1 ACTIVE;

-- query 7
REFRESH MATERIALIZED VIEW mv1  with sync mode;

-- query 8
select * from mv1 order by k1;

-- query 9
insert into t1 values ("2019-01-01",1,1),("2019-01-01",1,2),("2019-01-01",2,1),("2019-01-01",2,2),
                                         ("2023-01-11",1,1),("2023-01-11",1,2),("2023-02-11",2,1),("2023-01-11",2,2),
                                         ("2023-03-11",1,1),("2023-05-11",1,2),("2023-04-11",2,1),("2023-05-01",2,2);

-- query 10
REFRESH MATERIALIZED VIEW mv1  with sync mode;

-- query 11
select * from mv1 order by k1;

-- query 12
ALTER MATERIALIZED VIEW mv1 INACTIVE;

-- query 13
CREATE MATERIALIZED VIEW mv2
DISTRIBUTED BY HASH(k1) BUCKETS 10
REFRESH MANUAL
AS SELECT k1,v1 FROM t1;

-- query 14
drop table t1;

-- query 15
CREATE TABLE `t1` (
  `k1` date NULL COMMENT "",
  `v1` int(11) NULL COMMENT "",
  `v2` int(11) NULL COMMENT ""
)
TBLPROPERTIES ("format-version" = "3");

-- query 16
alter materialized view mv2 active;

-- query 17
refresh materialized view mv2  with sync mode;

-- query 18
select * from mv2 order by k1;

-- query 19
drop table t1;

-- query 20
CREATE TABLE `t1` (
  `k1` date NULL COMMENT "",
  `v1` int(11) NULL COMMENT "",
  `v2` int(11) NULL COMMENT ""
)
TBLPROPERTIES ("format-version" = "3");

-- query 21
insert into t1 values ("2019-01-01",1,1),("2019-01-01",1,2),("2019-01-01",2,1),("2019-01-01",2,2),
                                         ("2023-01-11",1,1),("2023-01-11",1,2),("2023-02-11",2,1),("2023-01-11",2,2),
                                         ("2023-03-11",1,1),("2023-05-11",1,2),("2023-04-11",2,1),("2023-05-01",2,2);

-- query 22
CREATE MATERIALIZED VIEW mv3
PARTITION BY (k1)
REFRESH DEFERRED MANUAL
AS SELECT k1,v1 FROM t1;

-- query 23
refresh MATERIALIZED VIEW mv3 PARTITION start ('2023-02-01') end ('2023-05-01') with sync mode;

-- query 24
ALTER MATERIALIZED VIEW mv3 INACTIVE;

-- query 25
ALTER MATERIALIZED VIEW mv3 ACTIVE;

-- query 26
-- Show partitions includes runtime-generated identifiers such as PartitionId and VersionEpoch.
-- The stable contract we care about is that the expected refreshed partitions are present after
-- re-activating the MV.
-- @skip_result_check=true
-- @result_contains=p20230201
-- @result_contains=p20230301
-- @result_contains=p20230401
show partitions from mv3
