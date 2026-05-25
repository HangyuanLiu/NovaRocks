-- @tags=iceberg_ddl
-- Test Objective:
-- 1. Validate ALTER TABLE ... ALTER COLUMN ... COMMENT 'x' on an Iceberg table.
-- 2. Verify SHOW CREATE TABLE reflects the updated comments.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t;
CREATE TABLE ${case_db}.t (k INT, v INT);

-- query 2
-- @skip_result_check=true
ALTER TABLE ${case_db}.t ALTER COLUMN k COMMENT 'key column';
ALTER TABLE ${case_db}.t ALTER COLUMN v COMMENT 'value column';

-- query 3
-- @result_contains=COMMENT 'key column'
-- @result_contains=COMMENT 'value column'
SHOW CREATE TABLE ${case_db}.t;
