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
-- Denylist coverage: each reserved category errors clearly.

-- query 1
CREATE DATABASE iceberg_ddl_cat_${suite_uuid0}.tblprops_reject_${uuid0};
USE iceberg_ddl_cat_${suite_uuid0}.tblprops_reject_${uuid0};
DROP TABLE IF EXISTS p;
CREATE TABLE p (id INT) TBLPROPERTIES ("format-version" = "2");

-- query 2
-- @expect_error=Iceberg table property `format-version` is reserved
ALTER TABLE p SET TBLPROPERTIES ('format-version' = '3');

-- query 3
-- @expect_error=Iceberg internal metadata key
ALTER TABLE p SET TBLPROPERTIES ('identifier-field-ids' = '[1]');

-- query 4
-- @expect_error=Iceberg internal metadata key
ALTER TABLE p SET TBLPROPERTIES ('current-schema-id' = '5');

-- query 5
-- ADR-0182: this ordinary property does not author any type domain.
ALTER TABLE p SET TBLPROPERTIES ('novarocks.logical_type.foo' = 'TINYINT');
ALTER TABLE p UNSET TBLPROPERTIES ('novarocks.logical_type.foo');

-- query 6
-- @expect_error=novarocks.* namespace is reserved
ALTER TABLE p SET TBLPROPERTIES ('novarocks.future' = 'whatever');

-- query 7
-- UNSET path covered too.
-- @expect_error=Iceberg internal metadata key
ALTER TABLE p UNSET TBLPROPERTIES ('last-column-id');

-- query 8
-- @expect_error_tier=drift
-- @expect_error=write.parquet.variant-shredding.
ALTER TABLE p SET TBLPROPERTIES ('write.parquet.variant-shredding.payload' = 'a:int');

-- query 9
-- @expect_error_tier=drift
-- @expect_error=write.parquet.variant-shredding.
CREATE TABLE shredding_create (id INT) TBLPROPERTIES ('write.parquet.variant-shredding.payload' = 'a:int');

-- query 10
-- Only the current nominal document namespace authors these type facts.
-- @expect_error=Iceberg nominal declarations are owned by schema mutations
ALTER TABLE p SET TBLPROPERTIES ('novarocks.nominal-types.v1' = '{"version":1,"fields":[]}');

-- query 11
DROP TABLE p;
DROP DATABASE iceberg_ddl_cat_${suite_uuid0}.tblprops_reject_${uuid0};
