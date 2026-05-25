-- @order_sensitive=true
-- @tags=iceberg_ddl,default
-- Test Objective:
-- 1. ALTER TABLE ADD COLUMN BOOLEAN DEFAULT v applies the default to existing
--    rows (Iceberg initial-default) and to subsequent INSERT-with-subset-columns
--    (Iceberg write-default).

DROP TABLE IF EXISTS ${case_db}.t;
CREATE TABLE ${case_db}.t (id INT, name STRING) TBLPROPERTIES ("format-version" = "3");
INSERT INTO ${case_db}.t VALUES (1, 'alice'), (2, 'bob');
ALTER TABLE ${case_db}.t ADD COLUMN flag BOOLEAN DEFAULT true;
SELECT id, name, flag FROM ${case_db}.t ORDER BY id;
INSERT INTO ${case_db}.t (id, name) VALUES (3, 'charlie');
SELECT id, name, flag FROM ${case_db}.t ORDER BY id;
