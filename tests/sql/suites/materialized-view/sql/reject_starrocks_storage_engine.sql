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

CREATE DATABASE IF NOT EXISTS ${case_db};
CREATE TABLE ${case_db}.base (id int)
TBLPROPERTIES ("format-version"="3", "write.row-lineage"="true");
-- @expect_error=storage_engine='starrocks'
CREATE MATERIALIZED VIEW ${case_db}.mv_bad
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ('storage_engine' = 'starrocks')
AS SELECT id FROM ${case_db}.base;
DROP TABLE ${case_db}.base;
DROP DATABASE ${case_db};
