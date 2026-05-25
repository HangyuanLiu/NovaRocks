-- @tags=iceberg_ddl,struct
-- Test Objective:
-- 1. Dropping the final remaining field of a STRUCT is rejected on Iceberg.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.tab1;
CREATE TABLE ${case_db}.tab1 (
  c0 INT,
  c1 STRUCT<v1 INT>
);

-- query 2
-- @expect_error=cannot drop last field of STRUCT
ALTER TABLE ${case_db}.tab1 DROP COLUMN c1.v1;
