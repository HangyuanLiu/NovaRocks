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
-- 1. Validate count-rollup rewrite stays correct when the base table is empty and after late inserts.
-- 2. Cover empty-result aggregation semantics, async partitioned MV refresh, and PK-table rollup behavior.
-- Source: dev/test/sql/test_materialized_view/T/test_materialized_view_rewrite

-- query 1
create table empty_tbl(time date, user_id int not null, user_name varchar(20), tag_id int)
TBLPROPERTIES ("format-version" = "3");

-- query 2
create materialized view empty_tbl_with_mv distributed by hash(user_id)
as select user_id, time, count(tag_id) from empty_tbl group by user_id, time;

-- query 3
select count() from empty_tbl;

-- query 4
select user_id, count(tag_id) from empty_tbl group by user_id, time;

-- query 5
select user_id, count(tag_id) from empty_tbl group by user_id;

-- query 6
insert into empty_tbl values('2023-04-13', 1, 'a', 1);

-- query 7
refresh materialized view empty_tbl_with_mv with sync mode;

-- query 8
select count() from empty_tbl where user_id > 2;

-- query 9
select user_id, count(tag_id) from empty_tbl where user_id = 2 group by user_id, time;

-- query 10
select user_id, count(tag_id) from empty_tbl where user_id > 2 group by user_id;

-- query 11
select user_id, count(tag_id) from empty_tbl group by user_id;

-- query 12
select count(user_id) from empty_tbl where user_id > 2 group by user_id;

-- query 13
select count(user_id) from empty_tbl where user_id > 2;

-- query 14
drop table empty_tbl;

-- query 15
drop materialized view empty_tbl_with_mv;

-- query 16
CREATE TABLE orders (
    dt date NOT NULL,
    order_id bigint NOT NULL,
    user_id int NOT NULL,
    merchant_id int NOT NULL,
    good_id int NOT NULL,
    good_name string NOT NULL,
    price int NOT NULL,
    cnt int NOT NULL,
    revenue int NOT NULL,
    state tinyint NOT NULL
)
TBLPROPERTIES ("format-version" = "3");

-- query 17
CREATE MATERIALIZED VIEW order_mv2
PARTITION BY date_trunc('MONTH', dt)
DISTRIBUTED BY HASH(order_id) BUCKETS 10
REFRESH ASYNC START('2023-07-01 10:00:00') EVERY (interval 1 day)
AS
select
    dt,
    order_id,
    user_id,
    sum(cnt) as total_cnt,
    sum(revenue) as total_revenue,
    count(state) as state_count
from orders group by dt, order_id, user_id;

-- query 18
select count() from orders;

-- query 19
drop materialized view order_mv2;
