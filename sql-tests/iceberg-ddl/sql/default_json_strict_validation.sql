-- @tags=iceberg_ddl,default,json,validation
-- Test Objective:
-- 1. Validate JSON DEFAULT with an invalid JSON literal is rejected at ALTER time.
-- 2. Complement to default_json.sql (positive case for JSON DEFAULT).

DROP TABLE IF EXISTS ${case_db}.t;
CREATE TABLE ${case_db}.t (id INT, name STRING) TBLPROPERTIES ("format-version" = "3");

-- @expect_error=invalid JSON DEFAULT literal
ALTER TABLE ${case_db}.t ADD COLUMN meta JSON DEFAULT 'not-a-json';
