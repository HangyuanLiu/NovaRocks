-- @order_sensitive=true
-- @tags=iceberg_ddl,default,json
-- Test Objective:
-- 1. ALTER ADD COLUMN JSON DEFAULT v applies correctly with a valid JSON literal.

DROP TABLE IF EXISTS ${case_db}.t;
CREATE TABLE ${case_db}.t (id INT, name STRING) TBLPROPERTIES ("format-version" = "3");
INSERT INTO ${case_db}.t VALUES (1, 'alice'), (2, 'bob');

ALTER TABLE ${case_db}.t ADD COLUMN meta JSON DEFAULT '{"k":"v"}';

SELECT id, name, meta FROM ${case_db}.t ORDER BY id;

INSERT INTO ${case_db}.t (id, name) VALUES (3, 'charlie');
SELECT id, name, meta FROM ${case_db}.t ORDER BY id;
