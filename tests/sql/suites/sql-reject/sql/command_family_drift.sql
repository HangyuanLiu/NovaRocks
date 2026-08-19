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

-- SQLP-0 drift corpus for the existing command-family probes.  These cases
-- intentionally lock current parser/admission behavior, including routes that
-- reject a malformed command before its family parser is reached.

-- @expect_error_tier=drift
-- @expect_error=ERROR 1105 (HY000): CREATE MATERIALIZED VIEW requires a DISTRIBUTED BY HASH(...) BUCKETS n clause
CREATE MATERIALIZED VIEW reject_mv AS SELECT 1;

-- @expect_error_tier=drift
-- @expect_error=ERROR 1105 (HY000): DROP MATERIALIZED VIEW ... FORCE is not supported
DROP MATERIALIZED VIEW reject_mv FORCE;

-- @expect_error_tier=drift
-- @expect_error=ERROR 1064 (42000): expected REFRESH or TBLPROPERTIES after ALTER MATERIALIZED VIEW ... SET
ALTER MATERIALIZED VIEW reject_mv SET FOO;

-- @expect_error_tier=drift
-- @expect_error=ERROR 1064 (42000): expected SYNC or ASYNC after REFRESH MATERIALIZED VIEW ... WITH
REFRESH MATERIALIZED VIEW reject_mv WITH BROKEN;

-- @expect_error_tier=drift
-- @expect_error=ERROR 1105 (HY000): SHOW MATERIALIZED VIEWS LIKE '...' is not supported yet
SHOW MATERIALIZED VIEWS LIKE 'reject%';

-- @expect_error_tier=drift
-- @expect_error=ERROR 1235 (42000): unsupported SQL command for the frontend capability router
ADD BACKEND 127.0.0.1:1234;

-- @expect_error_tier=drift
-- @expect_error=ERROR 1235 (42000): unsupported SQL command for the frontend capability router
DROP BACKEND 127.0.0.1:1234;

-- @expect_error_tier=drift
-- @expect_error=ERROR 1235 (42000): unsupported SQL command for the frontend capability router
ANALYZE TABLE reject_stats UPDATE HISTOGRAM ON c;

-- @expect_error_tier=drift
-- @expect_error=ERROR 1105 (HY000): Executor: TRUNCATE TABLE PARTITION (...) is not supported
TRUNCATE TABLE reject_table PARTITION (p1);

-- @expect_error_tier=drift
-- @expect_error=ERROR 1235 (42000): unsupported SQL command for the frontend capability router
ALTER TABLE reject_table CREATE BRANCH;

-- @expect_error_tier=drift
-- @expect_error=ERROR 1105 (HY000): Executor: ADD FILES: requires FROM <location>
ALTER TABLE reject_table ADD FILES;

-- @expect_error_tier=drift
-- @expect_error=ERROR 1105 (HY000): CALL procedure cannot mix positional and named arguments
CALL ice.system.rewrite_manifests('db.t', table => 'db.u');

-- @expect_error_tier=drift
-- @expect_error=ERROR 1064 (42000): sql parser error: Expected: =, found: )
CREATE CATALOG reject_catalog PROPERTIES ('type');

-- @expect_error_tier=drift
-- @expect_error=ERROR 1064 (42000): sql parser error: Expected: identifier, found: EOF
DROP CATALOG;

-- P8 classification-drift: this parser-domain validation currently reaches
-- the wire as errno 1105 / Internal rather than a typed SQL validation error.
-- @expect_error_tier=drift
-- @expect_error=ERROR 1105 (HY000): PRIMARY KEY clause requires at least one column
CREATE MATERIALIZED VIEW reject_mv
DISTRIBUTED BY HASH(k1) BUCKETS 1
PRIMARY KEY ()
AS SELECT k1 FROM source_table;
