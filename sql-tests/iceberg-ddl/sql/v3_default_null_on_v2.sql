-- @order_sensitive=true
-- Test Point: DEFAULT NULL is permitted on v2 Iceberg tables and produces NULL on omitted-column INSERTs (preserves pre-existing v2 behavior).
-- Method: CREATE v2 (default) table, ALTER ADD COLUMN b INT DEFAULT NULL, INSERT (a) VALUES (1), SELECT to confirm NULL.
-- Scope: D5 — DEFAULT NULL semantics never write metadata, work on v2.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_v3_default_null_v2 FORCE;
CREATE TABLE ${case_db}.t_v3_default_null_v2 (
  a INT
);
ALTER TABLE ${case_db}.t_v3_default_null_v2 ADD COLUMN b INT DEFAULT NULL;
INSERT INTO ${case_db}.t_v3_default_null_v2 (a) VALUES (1);

-- query 2
SELECT a, b FROM ${case_db}.t_v3_default_null_v2 ORDER BY a;

-- query 3
-- @skip_result_check=true
DROP TABLE ${case_db}.t_v3_default_null_v2 FORCE;
