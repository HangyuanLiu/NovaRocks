-- @order_sensitive=true
-- @tags=iceberg_ddl,default,complex
-- Test Objective:
-- 1. ALTER ADD COLUMN with ARRAY / MAP DEFAULT applies for empty literals.

DROP TABLE IF EXISTS ${case_db}.t;
CREATE TABLE ${case_db}.t (id INT, name STRING) TBLPROPERTIES ("format-version" = "3");
INSERT INTO ${case_db}.t VALUES (1, 'alice'), (2, 'bob');

ALTER TABLE ${case_db}.t ADD COLUMN tags ARRAY<INT> DEFAULT '[]';
ALTER TABLE ${case_db}.t ADD COLUMN counts MAP<STRING, INT> DEFAULT '{}';

SELECT id, name, tags, counts FROM ${case_db}.t ORDER BY id;

INSERT INTO ${case_db}.t (id, name) VALUES (3, 'charlie');
SELECT id, name, tags, counts FROM ${case_db}.t ORDER BY id;
