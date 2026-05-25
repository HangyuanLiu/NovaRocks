-- @order_sensitive=true
-- @tags=iceberg_ddl,default,varbinary
-- Test Objective:
-- 1. ALTER ADD COLUMN VARBINARY DEFAULT 'literal' applies correctly.

DROP TABLE IF EXISTS ${case_db}.t;
CREATE TABLE ${case_db}.t (id INT, name STRING) TBLPROPERTIES ("format-version" = "3");
INSERT INTO ${case_db}.t VALUES (1, 'alice'), (2, 'bob');

ALTER TABLE ${case_db}.t ADD COLUMN payload VARBINARY DEFAULT 'abc';

SELECT id, name, payload FROM ${case_db}.t ORDER BY id;

INSERT INTO ${case_db}.t (id, name) VALUES (3, 'charlie');
SELECT id, name, payload FROM ${case_db}.t ORDER BY id;
