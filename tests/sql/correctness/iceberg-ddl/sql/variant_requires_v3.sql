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

-- @expect_error_tier=drift
-- query 1
-- @expect_error=Iceberg Variant columns require format-version 3
CREATE TABLE ${case_db}.variant_default_v2 (id INT,v VARIANT);

-- query 2
-- @expect_error=Iceberg Variant columns require format-version 3
CREATE TABLE ${case_db}.variant_explicit_v2 (id INT,v VARIANT) TBLPROPERTIES ("format-version"="2");

-- query 3
-- @skip_result_check=true
CREATE TABLE ${case_db}.variant_source_v3 (id INT,v VARIANT) TBLPROPERTIES ("format-version"="3");
CREATE TABLE ${case_db}.variant_alter_v2 (id INT) TBLPROPERTIES ("format-version"="2");

-- query 4
-- @expect_error=Iceberg Variant columns require format-version 3
CREATE TABLE ${case_db}.variant_like_v2 LIKE ${case_db}.variant_source_v3;

-- query 5
-- @expect_error=Iceberg Variant columns require format-version 3
ALTER TABLE ${case_db}.variant_alter_v2 ADD COLUMN v VARIANT;

-- query 6
-- @skip_result_check=true
ALTER TABLE ${case_db}.variant_source_v3 ADD COLUMN extra VARIANT;
DROP TABLE ${case_db}.variant_source_v3;
DROP TABLE ${case_db}.variant_alter_v2;
