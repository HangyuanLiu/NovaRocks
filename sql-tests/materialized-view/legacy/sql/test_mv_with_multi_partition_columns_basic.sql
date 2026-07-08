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
-- 1. Validate multi-partition-column MV behavior under DDL, refresh, and rewrite changes.
-- 2. Cover activation, rename, swap, and optimize flows for multi-column partition MVs.
-- Source: dev/test/sql/test_materialized_view/T/test_mv_with_multi_partition_columns_basic

-- query 1
CREATE TABLE t1 (
    k1 int,
    k2 date,
    k3 string
)
TBLPROPERTIES ("format-version" = "3");

-- query 2
INSERT INTO t1 VALUES (1,'2020-06-02','BJ'),(3,'2020-06-02','SZ'),(2,'2020-07-02','SH');

-- query 3
-- Create Fail: mv's partition columns are not the same as table's partition columns
CREATE MATERIALIZED VIEW test_mv1
partition by (date_trunc("day", k2))
REFRESH MANUAL
AS select sum(k1), k2, k3 from t1 group by k2, k3;

-- query 4
REFRESH MATERIALIZED VIEW test_mv1 WITH SYNC MODE;

-- query 5
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN select sum(k1), k2, k3 from t1 group by k2, k3;

-- query 6
select sum(k1), k2, k3 from t1 group by k2, k3;
