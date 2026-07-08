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

-- @order_sensitive=true
-- @tags=analytic,range_frame,unbounded_following
-- Test Objective:
-- 1. Validate RANGE UNBOUNDED PRECEDING TO UNBOUNDED FOLLOWING semantics.
-- 2. Prevent regressions where RANGE full-partition frame is truncated by peer groups.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert deterministic rows with duplicate order keys and NULL value.
-- 3. Compute COUNT/SUM over full RANGE frame and assert stable output.
CREATE TABLE ${case_db}.t_analytic_range_unbounded_following_full_partition (
    grp VARCHAR(10),
    ord_key INT,
    v INT
)
TBLPROPERTIES ("format-version" = "3");

INSERT INTO ${case_db}.t_analytic_range_unbounded_following_full_partition VALUES
    ('A', 1, 10),
    ('A', 2, 20),
    ('A', 2, 30),
    ('A', 3, NULL);

SELECT
    grp,
    ord_key,
    v,
    COUNT(v) OVER (
        PARTITION BY grp ORDER BY ord_key
        RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS cnt_all_non_null,
    SUM(v) OVER (
        PARTITION BY grp ORDER BY ord_key
        RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS sum_all
FROM ${case_db}.t_analytic_range_unbounded_following_full_partition
ORDER BY grp, ord_key, v;
