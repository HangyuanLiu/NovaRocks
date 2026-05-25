-- @order_sensitive=true
-- @tags=iceberg_ddl,default,date
-- Test Objective:
-- 1. Validate DATE and DATETIME DEFAULT with mid-2024 calendar dates.
--    v3_default_primitive_types.sql uses the epoch + epoch-plus-1, which is
--    a different reference point.

DROP TABLE IF EXISTS ${case_db}.t;
CREATE TABLE ${case_db}.t (id INT, name STRING) TBLPROPERTIES ("format-version" = "3");
INSERT INTO ${case_db}.t VALUES (1, 'alice'), (2, 'bob');

ALTER TABLE ${case_db}.t ADD COLUMN d DATE DEFAULT '2024-01-01';
ALTER TABLE ${case_db}.t ADD COLUMN dt DATETIME DEFAULT '2024-01-01 12:00:00';

SELECT id, name, d, dt FROM ${case_db}.t ORDER BY id;

INSERT INTO ${case_db}.t (id, name) VALUES (3, 'charlie');
SELECT id, name, d, dt FROM ${case_db}.t ORDER BY id;
