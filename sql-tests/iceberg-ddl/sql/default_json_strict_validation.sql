-- @tags=iceberg_ddl,default,json,validation
-- Test Objective:
-- 1. Invalid JSON in DEFAULT is rejected at ALTER time.

DROP TABLE IF EXISTS ${case_db}.t;
CREATE TABLE ${case_db}.t (id INT, name STRING) TBLPROPERTIES ("format-version" = "3");

-- @expect_error=invalid JSON DEFAULT literal
ALTER TABLE ${case_db}.t ADD COLUMN meta JSON DEFAULT 'not-a-json';
