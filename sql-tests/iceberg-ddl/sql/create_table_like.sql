-- @tags=iceberg_ddl
-- Test Objective:
-- 1. Validate CREATE TABLE ... LIKE copies the source schema on an Iceberg table.
-- 2. Verify INSERT into the LIKE'd table works and the row count is correct.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.src;
DROP TABLE IF EXISTS ${case_db}.dst;
CREATE TABLE ${case_db}.src (
  id INT,
  name STRING,
  v BIGINT
) COMMENT 'source-table';
CREATE TABLE ${case_db}.dst LIKE ${case_db}.src;

-- query 2
-- @result_contains=`id`
-- @result_contains=`name`
-- @result_contains=`v`
SHOW CREATE TABLE ${case_db}.dst;

-- query 3
-- @skip_result_check=true
INSERT INTO ${case_db}.dst VALUES (1, 'alice', 100), (2, 'bob', 200);

-- query 4
SELECT count(1) AS n FROM ${case_db}.dst;
