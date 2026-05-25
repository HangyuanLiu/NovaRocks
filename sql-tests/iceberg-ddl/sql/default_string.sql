-- @order_sensitive=true
-- @tags=iceberg_ddl,default,string
-- Test Objective:
-- 1. Validate STRING DEFAULT with empty-string, unicode, comma, and newline-escape
--    literals. v3_default_primitive_types.sql covers only the trivial 'hi' literal.

DROP TABLE IF EXISTS ${case_db}.t;
CREATE TABLE ${case_db}.t (id INT, name STRING) TBLPROPERTIES ("format-version" = "3");
INSERT INTO ${case_db}.t VALUES (1, 'alice'), (2, 'bob');

ALTER TABLE ${case_db}.t ADD COLUMN tag STRING DEFAULT 'default';
ALTER TABLE ${case_db}.t ADD COLUMN empty_v STRING DEFAULT '';
ALTER TABLE ${case_db}.t ADD COLUMN unicode_v STRING DEFAULT '日本語';
ALTER TABLE ${case_db}.t ADD COLUMN special_v STRING DEFAULT 'a,b\nc';

SELECT id, name, tag, empty_v, unicode_v, special_v FROM ${case_db}.t ORDER BY id;

INSERT INTO ${case_db}.t (id, name) VALUES (3, 'charlie');
SELECT id, name, tag, empty_v, unicode_v, special_v FROM ${case_db}.t ORDER BY id;
