-- @order_sensitive=true
-- @tags=iceberg_ddl,default,string
-- Test Objective:
-- 1. ALTER ADD COLUMN STRING / VARCHAR DEFAULT v applies correctly.
-- 2. Empty-string and special-character defaults survive round-trip.
-- Note: CHAR(N) was in the source; Iceberg has no fixed-CHAR so we widen to STRING.

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
