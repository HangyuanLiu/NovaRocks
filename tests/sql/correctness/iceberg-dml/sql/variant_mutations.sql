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
-- @expect_error_tier=drift
-- Data-producing writes reject Variant even when the table has no rows.
-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_v3_variant_mut FORCE;
DROP TABLE IF EXISTS ${case_db}.s_v3_variant_mut FORCE;
CREATE TABLE ${case_db}.t_v3_variant_mut (id BIGINT, v VARIANT)
TBLPROPERTIES ("format-version"="3", "write.row-lineage"="true");
CREATE TABLE ${case_db}.s_v3_variant_mut (id BIGINT, new_id BIGINT)
TBLPROPERTIES ("format-version"="3");
INSERT INTO ${case_db}.s_v3_variant_mut VALUES (3,30);

-- query 2
-- @expect_error=Iceberg Variant writes are not supported
UPDATE ${case_db}.t_v3_variant_mut SET id = 10 WHERE id = 1;

-- query 3
-- @expect_error=Iceberg Variant writes are not supported
MERGE INTO ${case_db}.t_v3_variant_mut t USING ${case_db}.s_v3_variant_mut s
ON t.id = s.id WHEN MATCHED THEN UPDATE SET id = s.new_id;

-- query 4
-- @expect_error=Iceberg Variant writes are not supported
MERGE INTO ${case_db}.t_v3_variant_mut t USING ${case_db}.s_v3_variant_mut s
ON t.id = s.id WHEN NOT MATCHED THEN INSERT (id,v) VALUES (s.id,NULL);

-- query 5
-- @expect_error=Iceberg Variant writes are not supported
INSERT OVERWRITE ${case_db}.t_v3_variant_mut VALUES (20,parse_json('{"a":20}'));

-- query 6
-- @expect_error=Iceberg Variant writes are not supported
CREATE TABLE ${case_db}.t_v3_variant_ctas TBLPROPERTIES ("format-version"="3")
AS SELECT CAST(1 AS BIGINT) AS id, parse_json('{"a":1}') AS v;

-- query 7
-- @skip_result_check=true
DELETE FROM ${case_db}.t_v3_variant_mut WHERE id = 2;
TRUNCATE TABLE ${case_db}.t_v3_variant_mut;

-- query 8
SELECT COUNT(*) AS remaining_rows FROM ${case_db}.t_v3_variant_mut;

-- query 9
-- @expect_error=equality-delete keys
ALTER TABLE ${case_db}.t_v3_variant_mut ADD EQUALITY DELETE (v) VALUES (NULL);

-- query 10
-- @expect_error=variant columns cannot appear in the partition spec
CREATE TABLE ${case_db}.t_v3_variant_partition_bad (id BIGINT,v VARIANT)
PARTITION BY identity(v) TBLPROPERTIES ("format-version"="3");

-- query 11
-- @expect_error=variant columns cannot appear in the partition spec
ALTER TABLE ${case_db}.t_v3_variant_mut ADD PARTITION COLUMN v;

-- query 12
-- @skip_result_check=true
CREATE TABLE ${case_db}.t_v3_variant_part_mut (id BIGINT,region VARCHAR,v VARIANT)
PARTITION BY identity(region) TBLPROPERTIES ("format-version"="3");

-- query 13
-- @expect_error=Iceberg Variant writes are not supported
INSERT OVERWRITE PARTITIONS ${case_db}.t_v3_variant_part_mut
VALUES (4,'us',parse_json('{"a":4}'));

-- query 14
-- @skip_result_check=true
DROP TABLE ${case_db}.t_v3_variant_mut FORCE;
DROP TABLE ${case_db}.s_v3_variant_mut FORCE;
DROP TABLE ${case_db}.t_v3_variant_part_mut FORCE;
