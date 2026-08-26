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

-- @sequential=true
-- The proxy recognizes only ordinary Iceberg REST stage-create and table
-- commit requests. It never persists operation state or offers a recovery
-- endpoint. Each assertion below reads the table through the real Catalog.

-- query 1
-- @skip_result_check=true
DROP DATABASE IF EXISTS lake_publication_${suite_uuid0}.ns_${uuid0} FORCE;
CREATE DATABASE lake_publication_${suite_uuid0}.ns_${uuid0};
CREATE TABLE lake_publication_${suite_uuid0}.ns_${uuid0}.source_rows (
  id INT,
  value VARCHAR(16)
)
TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");
INSERT INTO lake_publication_${suite_uuid0}.ns_${uuid0}.source_rows VALUES
  (1, 'alpha'), (2, 'beta'), (3, 'gamma');

-- query 2
-- Frontier dispatch never occurred: the target must remain absent and the
-- statement must report a definite non-commit outcome.
-- @publication_catalog_fault=stage-create,before-dispatch
-- @expect_error_code=CommitKnownUncommitted
CREATE TABLE lake_publication_${suite_uuid0}.ns_${uuid0}.before_dispatch AS
  SELECT id, value FROM lake_publication_${suite_uuid0}.ns_${uuid0}.source_rows;

-- query 3
-- The single standard NotExist table commit succeeded at the Catalog but its
-- response was lost. The current statement may use its one same-session,
-- read-only adjudication to observe the exact marker and report committed;
-- the following restart must not replay or mutate the completed attempt.
-- @publication_catalog_fault=table-commit,after-commit-before-response
-- @skip_result_check=true
-- @restart_fe_after_step=true
CREATE TABLE lake_publication_${suite_uuid0}.ns_${uuid0}.response_lost AS
  SELECT id, value FROM lake_publication_${suite_uuid0}.ns_${uuid0}.source_rows;

-- query 4
-- The external-state observation is the authority, not the prior error text.
-- @retry_count=30
-- @retry_interval_ms=1000
SELECT id, value FROM lake_publication_${suite_uuid0}.ns_${uuid0}.response_lost ORDER BY id;

-- query 5
-- Success followed by an FE restart exercises the finalization boundary. The
-- restart must not resume a completed CTAS as a second Catalog mutation.
-- @skip_result_check=true
-- @restart_fe_after_step=true
CREATE TABLE lake_publication_${suite_uuid0}.ns_${uuid0}.restart_after_success AS
  SELECT id, value FROM lake_publication_${suite_uuid0}.ns_${uuid0}.source_rows;

-- query 6
SELECT COUNT(*) AS n
  FROM lake_publication_${suite_uuid0}.ns_${uuid0}.restart_after_success;

-- query 7
-- @skip_result_check=true
DROP DATABASE lake_publication_${suite_uuid0}.ns_${uuid0} FORCE;
