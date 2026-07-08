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
-- 1. Validate MV metadata functions expose expected identifiers and properties.
-- 2. Cover metadata lookup functions on active materialized views.
-- Source: dev/test/sql/test_materialized_view/T/test_mv_meta_functions

-- query 1
create table user_tags (time date, user_id int, user_name varchar(20), tag_id int)
TBLPROPERTIES ("format-version" = "3");

-- query 2
insert into user_tags values('2023-04-13', 1, 'a', 1);

-- query 3
insert into user_tags values('2023-04-13', 1, 'b', 2);

-- query 4
insert into user_tags values('2023-04-13', 1, 'c', 3);

-- query 5
insert into user_tags values('2023-04-13', 1, 'd', 4);

-- query 6
insert into user_tags values('2023-04-13', 1, 'e', 5);

-- query 7
insert into user_tags values('2023-04-13', 2, 'e', 5);

-- query 8
insert into user_tags values('2023-04-13', 3, 'e', 6);

-- query 9
create materialized view user_tags_mv1  distributed by hash(user_id) as select user_id, bitmap_union(to_bitmap(tag_id)) from user_tags group by user_id;

-- query 10
-- @skip_result_check=true
select inspect_mv_refresh_info('user_tags_mv1');

-- query 11
-- @skip_result_check=true
select inspect_table_partition_info('user_tags');

-- query 12
refresh materialized view user_tags_mv1 with sync mode;

-- query 13
-- @skip_result_check=true
select inspect_mv_plan('user_tags_mv1');

-- query 14
-- @skip_result_check=true
select inspect_mv_plan('user_tags_mv1', true);

-- query 15
-- @skip_result_check=true
select inspect_mv_plan('user_tags_mv1', false);

-- query 16
insert into user_tags values('2023-04-13', 3, 'e', 6);

-- query 17
-- @skip_result_check=true
select inspect_mv_refresh_info('user_tags_mv1');

-- query 18
-- @skip_result_check=true
select inspect_table_partition_info('user_tags');
