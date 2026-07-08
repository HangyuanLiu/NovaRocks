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
-- 1. Validate UNION ALL rewrite behavior when branch outputs are not perfectly aligned.
-- 2. Cover rewrite correctness across unaligned union branches.
-- Source: dev/test/sql/test_materialized_view/T/test_materialized_view_union

-- query 1
-- unaligned partitions: https://github.com/StarRocks/starrocks/issues/42949
CREATE TABLE IF NOT EXISTS t1 (
    leg_id VARCHAR(100) NOT NULL,
    cabin_class VARCHAR(1) NOT NULL,
    observation_date DATE NOT NULL
)
TBLPROPERTIES ("format-version" = "3");

-- query 2
CREATE TABLE IF NOT EXISTS t2 (
    leg_id VARCHAR(100) NOT NULL,
    cabin_class VARCHAR(1) NOT NULL,
    observation_date DATE NOT NULL
)
TBLPROPERTIES ("format-version" = "3");

-- query 3
CREATE TABLE IF NOT EXISTS t3 (
    leg_id VARCHAR(100) NOT NULL,
    cabin_class VARCHAR(1) NOT NULL,
    observation_date DATE NOT NULL
)
TBLPROPERTIES ("format-version" = "3");

-- query 4
CREATE TABLE IF NOT EXISTS t4 (
    leg_id VARCHAR(100) NOT NULL,
    cabin_class VARCHAR(1) NOT NULL,
    observation_date DATE NOT NULL
)
TBLPROPERTIES ("format-version" = "3");

-- query 5
insert into t1 (leg_id, cabin_class, observation_date) values
('FL_123', 'Y', '2024-03-21'),
('FL_124', 'Y', '2024-03-21'),
('FL_125', 'Y', '2024-03-21'),
('FL_126', 'Y', '2024-03-21');

-- query 6
insert into t2 (leg_id, cabin_class, observation_date) values
('FL_123', 'Y', '2024-03-22'),
('FL_124', 'Y', '2024-03-22'),
('FL_125', 'Y', '2024-03-22'),
('FL_126', 'Y', '2024-03-22');

-- query 7
insert into t3 (leg_id, cabin_class, observation_date) values
('FL_123', 'Y', '2024-03-22'),
('FL_124', 'Y', '2024-03-22'),
('FL_125', 'Y', '2024-03-22'),
('FL_126', 'Y', '2024-03-22');

-- query 8
CREATE MATERIALIZED VIEW v1
PARTITION BY date_trunc('day', observation_date)
DISTRIBUTED BY HASH(leg_id)
REFRESH DEFERRED ASYNC
AS
SELECT * FROM t1
UNION ALL
SELECT * FROM t2;

-- query 9
REFRESH MATERIALIZED VIEW v1 WITH SYNC MODE;

-- query 10
select count(*) from v1;

-- query 11
-- day and month
CREATE MATERIALIZED VIEW v2
PARTITION BY date_trunc('day', observation_date)
DISTRIBUTED BY HASH(leg_id)
REFRESH DEFERRED ASYNC
AS
SELECT * FROM t1
UNION ALL
SELECT * FROM t3;

-- query 12
-- unaligned range
CREATE MATERIALIZED VIEW v3
PARTITION BY date_trunc('day', observation_date)
DISTRIBUTED BY HASH(leg_id)
REFRESH DEFERRED ASYNC
AS
SELECT * FROM t1
UNION ALL
SELECT * FROM t4;
