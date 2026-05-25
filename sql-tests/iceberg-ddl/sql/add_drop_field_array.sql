-- @tags=iceberg_ddl,struct,array
-- Test Objective:
-- 1. Validate ARRAY<STRUCT> element field add/drop via Iceberg `element` notation.
-- 2. Verify negative paths (drop the element itself, drop non-existent field,
--    add/drop on a non-struct list element).
-- Note: NovaRocks's standalone INSERT path does not currently support ARRAY<STRUCT>
-- column writes, so this test is DDL-only.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.tab1;
CREATE TABLE ${case_db}.tab1 (
  c0 INT,
  c1 ARRAY<STRUCT<v1 INT, v2 INT>>
);

-- query 2
-- Negative: cannot DROP c1.element itself (path is empty after the 'element' segment).
-- @expect_error=drop path is empty
ALTER TABLE ${case_db}.tab1 DROP COLUMN c1.element;

-- query 3
-- Negative: cannot DROP a non-existent element field.
-- @expect_error=not found
ALTER TABLE ${case_db}.tab1 DROP COLUMN c1.element.v3;

-- query 4
-- Positive: add a new field to the array's element struct.
-- @skip_result_check=true
ALTER TABLE ${case_db}.tab1 ADD COLUMN c1.element.val1 INT;

-- query 5
-- Positive: drop a previously-existing element field.
-- @skip_result_check=true
ALTER TABLE ${case_db}.tab1 DROP COLUMN c1.element.v1;

-- query 6
-- Positive: re-add a previously-dropped element field name.
-- @skip_result_check=true
ALTER TABLE ${case_db}.tab1 ADD COLUMN c1.element.v1 INT;
