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
-- 1. Validate UNION-style rewrite and compensation plan generation.
-- 2. Cover union rewrite eligibility across multiple branches.
-- Source: dev/test/sql/test_materialized_view/T/test_mv_union_rewrite

-- query 1
CREATE TABLE `event1` (
    `event_id` int(11) NOT NULL,
    `event_type` varchar(26) NOT NULL,
    `event_time` datetime NOT NULL
)
TBLPROPERTIES ("format-version" = "3");

-- query 2
insert into event1 values(129, "click", "2023-01-06 12:12:23"), (128, "click", "2023-01-06 18:12:23"), (127, "click2", "2023-01-05 12:12:23");

-- query 3
CREATE MATERIALIZED VIEW `olap_mv1`
COMMENT "MATERIALIZED_VIEW"
PARTITION BY (`event_time`)
DISTRIBUTED BY HASH(`event_id`) BUCKETS 1
REFRESH MANUAL
PROPERTIES (
"replication_num" = "1"
)
AS SELECT `event_id`, `event_type`, `event_time`
FROM `event1`
WHERE `event_type` = 'click';

-- query 4
refresh materialized view olap_mv1 with sync mode;

-- query 5
explain logical select count(event_id) from event1;

-- query 6
select count(event_id) from event1;
