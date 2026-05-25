-- @order_sensitive=true
-- @tags=iceberg_ddl,default,numeric
-- Test Objective:
-- 1. Validate ALTER ADD COLUMN ... DEFAULT for numeric types at boundary values
--    (negative, zero, very large), which v3_default_primitive_types.sql does not.
-- 2. Cover SMALLINT/INT/BIGINT/FLOAT/DOUBLE with explicit large/negative/zero
--    defaults distinct from the small literals in v3_default_primitive_types.

DROP TABLE IF EXISTS ${case_db}.t;
CREATE TABLE ${case_db}.t (id INT, name STRING) TBLPROPERTIES ("format-version" = "3");
INSERT INTO ${case_db}.t VALUES (1, 'alice'), (2, 'bob');

ALTER TABLE ${case_db}.t ADD COLUMN score SMALLINT DEFAULT 100;
ALTER TABLE ${case_db}.t ADD COLUMN salary INT DEFAULT 50000;
ALTER TABLE ${case_db}.t ADD COLUMN revenue BIGINT DEFAULT 1000000;
ALTER TABLE ${case_db}.t ADD COLUMN rating FLOAT DEFAULT 4.5;
ALTER TABLE ${case_db}.t ADD COLUMN percentage DOUBLE DEFAULT 95.5;
ALTER TABLE ${case_db}.t ADD COLUMN zero_v INT DEFAULT 0;
ALTER TABLE ${case_db}.t ADD COLUMN neg_v INT DEFAULT -100;

SELECT id, name, score, salary, revenue, rating, percentage, zero_v, neg_v
FROM ${case_db}.t ORDER BY id;

INSERT INTO ${case_db}.t (id, name) VALUES (3, 'charlie');

SELECT id, name, score, salary, revenue, rating, percentage, zero_v, neg_v
FROM ${case_db}.t ORDER BY id;
