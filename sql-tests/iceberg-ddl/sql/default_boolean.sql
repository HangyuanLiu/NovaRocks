-- @order_sensitive=true
-- @tags=iceberg_ddl,default
-- Test Objective:
-- 1. Validate BOOLEAN initial-default + write-default in isolation (one column, two
--    explicit INSERT patterns: full-list and subset-list).
-- 2. Complementary to v3_default_primitive_types.sql which exercises BOOLEAN as one
--    of 11 types in a single combined case.

DROP TABLE IF EXISTS ${case_db}.t;
CREATE TABLE ${case_db}.t (id INT, name STRING) TBLPROPERTIES ("format-version" = "3");
INSERT INTO ${case_db}.t VALUES (1, 'alice'), (2, 'bob');
ALTER TABLE ${case_db}.t ADD COLUMN flag BOOLEAN DEFAULT true;
SELECT id, name, flag FROM ${case_db}.t ORDER BY id;
INSERT INTO ${case_db}.t (id, name) VALUES (3, 'charlie');
SELECT id, name, flag FROM ${case_db}.t ORDER BY id;
