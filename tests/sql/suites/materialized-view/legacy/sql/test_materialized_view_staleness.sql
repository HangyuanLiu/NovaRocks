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
-- 1. Validate MV staleness configuration gates rewrite eligibility.
-- 2. Cover stale and fresh rewrite behavior on the same MV.
-- Source: dev/test/sql/test_materialized_view/T/test_materialized_view_staleness

-- query 1
drop table if exists t1;

-- query 2
CREATE TABLE t1 (
    k1 int,
    k2 int
)
TBLPROPERTIES ("format-version" = "3");

-- query 3
INSERT INTO t1 VALUES (1,1),(1,2),(null,null);

-- query 4
drop materialized view if exists mv1;

-- query 5
CREATE MATERIALIZED VIEW mv1 REFRESH MANUAL
properties (
    "replication_num" = "1",
    "mv_rewrite_staleness_second" = "10"
)
 AS SELECT k1,sum(k2) FROM t1 group by k1;

-- query 6
REFRESH MATERIALIZED VIEW mv1 with sync mode;

-- query 7
select * from mv1 order by k1;

-- query 8
INSERT INTO t1 VALUES (2,2);

-- query 9
REFRESH MATERIALIZED VIEW mv1 with sync mode;

-- query 10
select * from mv1 order by k1;
