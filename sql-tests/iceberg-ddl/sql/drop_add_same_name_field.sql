-- @tags=iceberg_ddl,struct
-- Test Objective:
-- 1. Validate dropping then re-adding a STRUCT field with the same name but a different
--    type is accepted on Iceberg.
-- 2. Verify SELECT on the non-STRUCT column still works after the evolution sequence.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t;
CREATE TABLE ${case_db}.t (
  c1 INT,
  c2 STRUCT<v2_1 INT>
);

-- query 2
-- @skip_result_check=true
ALTER TABLE ${case_db}.t ADD COLUMN c2.v2_2 STRING;

-- query 3
-- @skip_result_check=true
ALTER TABLE ${case_db}.t DROP COLUMN c2.v2_2;

-- query 4
-- @skip_result_check=true
ALTER TABLE ${case_db}.t ADD COLUMN c2.v2_2 DATE;

-- query 5
-- @order_sensitive=true
SELECT c1 FROM ${case_db}.t ORDER BY c1;
