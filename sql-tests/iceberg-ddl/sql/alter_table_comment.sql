-- @tags=iceberg_ddl
-- Test Objective:
-- 1. Validate ALTER TABLE t COMMENT 'x' updates the table-level comment on an Iceberg table.
-- 2. Verify SHOW CREATE TABLE reflects both the initial CREATE TABLE COMMENT and the ALTER-applied comment.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t;
CREATE TABLE ${case_db}.t (id INT, v INT) COMMENT 'c1';

-- query 2
-- @result_contains=COMMENT 'c1'
SHOW CREATE TABLE ${case_db}.t;

-- query 3
-- @skip_result_check=true
ALTER TABLE ${case_db}.t COMMENT 'c2';

-- query 4
-- @result_contains=COMMENT 'c2'
SHOW CREATE TABLE ${case_db}.t;
