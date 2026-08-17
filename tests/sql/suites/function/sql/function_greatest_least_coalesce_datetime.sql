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

-- Migrated from dev/test/sql/test_function/T/test_greatest_least_coalesce_datetime
-- Test Objective:
-- 1. Validate greatest(DATE, DATETIME) returns DATETIME, not BIGINT, and date() wrapping gives the correct date.
-- 2. Validate least(DATE, DATETIME) returns DATETIME with correct value.
-- 3. Validate coalesce(NULL, DATE, DATETIME) returns DATETIME.
-- 4. Validate greatest/least with pure DATE inputs returns DATETIME (since only DATETIME signatures exist).
-- 5. Validate typeof() confirms correct return types for all cases.
-- 6. Verify no regression: numeric types still work correctly with greatest().

-- query 1
-- The actual user-visible bug: wrapping result in date() returns wrong date
select date(greatest(date('2026-01-14'), timestamp('2026-01-11 00:00:00')));

-- query 2
-- greatest(DATE, DATETIME) should return DATETIME, not BIGINT
select greatest(date('2026-01-11'), timestamp('2026-01-14 00:00:00'));

-- query 3
select typeof(greatest(date('2026-01-11'), timestamp('2026-01-14 00:00:00')));

-- query 4
select least(date('2026-01-14'), timestamp('2026-01-11 00:00:00'));

-- query 5
select typeof(least(date('2026-01-14'), timestamp('2026-01-11 00:00:00')));

-- query 6
select coalesce(null, date('2026-01-11'), timestamp('2026-01-14 00:00:00'));

-- query 7
select typeof(coalesce(null, date('2026-01-11'), timestamp('2026-01-14 00:00:00')));

-- query 8
-- greatest/least only have DATETIME signatures, so pure DATE inputs return DATETIME (not INT)
select greatest(date('2026-01-11'), date('2026-01-14'), date('2026-01-09'));

-- query 9
select typeof(greatest(date('2026-01-11'), date('2026-01-14'), date('2026-01-09')));

-- query 10
-- Verify no regression: numeric types should still work correctly
select typeof(greatest(cast(100 as int), cast(200 as bigint)));

-- query 11
select typeof(greatest(20260114, 20260115));
