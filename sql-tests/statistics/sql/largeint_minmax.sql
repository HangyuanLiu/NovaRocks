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

-- @tags=statistics,largeint,minmax
-- Test Objective:
-- Preserve 128-bit LARGEINT min-max statistics at both boundaries
-- without coupling statistics collection to low-cardinality regressions.
CREATE TABLE ${case_db}.largeint_minmax (
    k LARGEINT NOT NULL
);

INSERT INTO ${case_db}.largeint_minmax VALUES
    (-170141183460469231731687303715884105728),
    (0),
    (170141183460469231731687303715884105727);

ANALYZE TABLE ${case_db}.largeint_minmax;

-- @result_contains=min-max stats
-- @skip_result_check=true
EXPLAIN VERBOSE
SELECT DISTINCT k
FROM ${case_db}.largeint_minmax;
