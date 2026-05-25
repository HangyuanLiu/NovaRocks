-- @order_sensitive=true
-- @tags=iceberg_ddl,default,decimal
-- Test Objective:
-- 1. Validate ALTER ADD COLUMN ... DEFAULT for DECIMAL across multiple
--    precision/scale combos including DECIMAL(20, 6) (exceeds the single
--    DECIMAL(10, 2) example in v3_default_primitive_types.sql).

DROP TABLE IF EXISTS ${case_db}.t;
CREATE TABLE ${case_db}.t (id INT, name STRING) TBLPROPERTIES ("format-version" = "3");
INSERT INTO ${case_db}.t VALUES (1, 'alice'), (2, 'bob');

ALTER TABLE ${case_db}.t ADD COLUMN price DECIMAL(10, 2) DEFAULT 9.99;
ALTER TABLE ${case_db}.t ADD COLUMN rate DECIMAL(5, 4) DEFAULT 0.1234;
ALTER TABLE ${case_db}.t ADD COLUMN big DECIMAL(20, 6) DEFAULT 123456789.000001;

SELECT id, name, price, rate, big FROM ${case_db}.t ORDER BY id;

INSERT INTO ${case_db}.t (id, name) VALUES (3, 'charlie');
SELECT id, name, price, rate, big FROM ${case_db}.t ORDER BY id;
