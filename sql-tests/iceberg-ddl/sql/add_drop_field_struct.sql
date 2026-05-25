-- @tags=iceberg_ddl,struct
-- Test Objective:
-- 1. Validate nested STRUCT field add/drop via dotted-path syntax on Iceberg.
-- 2. Verify add-field on a non-struct path is rejected.
-- 3. Verify add-field for an already-existing field is rejected.
-- 4. Verify drop-field for a non-existent field is rejected.
-- Note: NovaRocks's standalone INSERT path does not currently support STRUCT
-- column writes (see sql-tests/iceberg/sql/iceberg_schema_evolution_nested.sql),
-- so this test is DDL-only.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.tab1;
CREATE TABLE ${case_db}.tab1 (
  c0 INT,
  c1 STRUCT<v1 INT, v2 STRUCT<v3 INT, v4 INT>>
);

-- query 2
-- Negative: cannot ADD COLUMN under a non-struct path (v1 is an INT, not a STRUCT).
-- @expect_error=parent path must point to a STRUCT
ALTER TABLE ${case_db}.tab1 ADD COLUMN c1.v1.v5 INT;

-- query 3
-- Negative: cannot ADD COLUMN with an existing top-level field name (v2).
-- @expect_error=column `v2` already exists
ALTER TABLE ${case_db}.tab1 ADD COLUMN c1.v2 INT;

-- query 4
-- Negative: cannot ADD COLUMN with an existing nested field name (v2.v3).
-- @expect_error=column `v3` already exists
ALTER TABLE ${case_db}.tab1 ADD COLUMN c1.v2.v3 INT;

-- query 5
-- Positive: add a new top-level field.
-- @skip_result_check=true
ALTER TABLE ${case_db}.tab1 ADD COLUMN c1.val1 INT;

-- query 6
-- Negative: cannot DROP COLUMN a non-existent nested field.
-- @expect_error=column path 'c1.v2.v5' not found
ALTER TABLE ${case_db}.tab1 DROP COLUMN c1.v2.v5;

-- query 7
-- Positive: drop a top-level field.
-- @skip_result_check=true
ALTER TABLE ${case_db}.tab1 DROP COLUMN c1.v1;

-- query 8
-- Positive: re-add a previously-dropped field name with a new type.
-- @skip_result_check=true
ALTER TABLE ${case_db}.tab1 ADD COLUMN c1.v1 INT;
