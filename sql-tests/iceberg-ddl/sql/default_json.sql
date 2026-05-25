-- @order_sensitive=true
-- @tags=iceberg_ddl,default,json
-- Test Objective:
-- 1. Validate JSON DEFAULT — a type NOT covered by v3_default_primitive_types.sql.
-- 2. Positive companion to v3_default_complex_type_rejected.sql (negative).

DROP TABLE IF EXISTS ${case_db}.t;
CREATE TABLE ${case_db}.t (id INT, name STRING) TBLPROPERTIES ("format-version" = "3");
INSERT INTO ${case_db}.t VALUES (1, 'alice'), (2, 'bob');

ALTER TABLE ${case_db}.t ADD COLUMN meta JSON DEFAULT '{"k":"v"}';

SELECT id, name, meta FROM ${case_db}.t ORDER BY id;

INSERT INTO ${case_db}.t (id, name) VALUES (3, 'charlie');
SELECT id, name, meta FROM ${case_db}.t ORDER BY id;
