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
-- 1. Validate rewrite status/session metadata reflects MV rewrite outcomes.
-- 2. Cover optimizer status inspection for hit and miss scenarios.
-- Source: dev/test/sql/test_materialized_view/T/test_query_rewrite_status

-- query 1
create database db_${uuid0};

-- query 2
use db_${uuid0};

-- query 3
create table t1(c1 int, c2 int, c3 int)
TBLPROPERTIES ("format-version" = "3");

-- query 4
create materialized view mv1 refresh manual as select * from t1;

-- query 5
create materialized view mv2 refresh manual properties('enable_query_rewrite'='false') as select * from t1;

-- query 6
create materialized view mv3 refresh manual as select c1, count(*) from t1 group by c1;

-- query 7
alter materialized view mv3 inactive;

-- query 8
create materialized view mv4 refresh manual as select c1, count(*) from t1 group by c1 limit 5;

-- query 9
select table_name, query_rewrite_status from information_schema.materialized_views where TABLE_SCHEMA = 'db_${uuid0}' order by table_name;

-- query 10
drop database db_${uuid0} force;
