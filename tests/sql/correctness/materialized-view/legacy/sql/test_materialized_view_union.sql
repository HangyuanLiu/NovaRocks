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
-- 1. Validate MV behavior for UNION and UNION ALL query patterns.
-- 2. Cover union result correctness together with rewrite planning.
-- Source: dev/test/sql/test_materialized_view/T/test_materialized_view_union

-- query 1
CREATE TABLE `t1` (
  `k1` int(11) NULL COMMENT "",
  `v1` int(11) NULL COMMENT "",
  `v2` int(11) NULL COMMENT ""
)
TBLPROPERTIES ("format-version" = "3");

-- query 2
CREATE TABLE `t2` (
  `k1` int(11) NULL COMMENT "",
  `v1` int(11) NULL COMMENT "",
  `v2` int(11) NULL COMMENT ""
)
TBLPROPERTIES ("format-version" = "3");

-- query 3
CREATE MATERIALIZED VIEW `test_union_mv1_${uuid0}`
PARTITION BY (`k1`)
DISTRIBUTED BY HASH(`k1`) BUCKETS 2
REFRESH DEFERRED ASYNC
PROPERTIES (
"replication_num" = "1",
"storage_medium" = "HDD"
)
AS select k1, v1, v2 from t1
union all
select k1, v1, v2 from t2;

-- query 4
insert into t1 values ("202301",1,1),("202305",1,2);

-- query 5
refresh materialized view test_union_mv1_${uuid0} with sync mode;

-- query 6
select * from test_union_mv1_${uuid0} order by k1, v1;

-- query 7
drop table t1;

-- query 8
drop table t2;

-- query 9
drop materialized view test_union_mv1_${uuid0};

-- query 10
CREATE TABLE `t1` (
  `k1` int(11) NULL COMMENT "",
  `v1` int(11) NULL COMMENT "",
  `v2` int(11) NULL COMMENT ""
)
TBLPROPERTIES ("format-version" = "3");

-- query 11
CREATE TABLE `t2` (
  `k1` int(11) NULL COMMENT "",
  `v1` int(11) NULL COMMENT "",
  `v2` int(11) NULL COMMENT ""
)
TBLPROPERTIES ("format-version" = "3");

-- query 12
insert into t1 values (202301,1,1),(202305,1,2);

-- query 13
CREATE MATERIALIZED VIEW `test_union_mv2_${uuid0}`
PARTITION BY (`k1`)
DISTRIBUTED BY HASH(`k1`) BUCKETS 2
REFRESH DEFERRED ASYNC
PROPERTIES (
"replication_num" = "1",
"storage_medium" = "HDD",
"auto_refresh_partitions_limit" = "1"
)
AS select k1, v1, v2 from t1
union
select k1, v1, v2 from t2;

-- query 14
refresh materialized view test_union_mv2_${uuid0} with sync mode;

-- query 15
select * from test_union_mv2_${uuid0} order by k1, v1;

-- query 16
drop table t1;

-- query 17
drop table t2;

-- query 18
drop materialized view test_union_mv2_${uuid0};
