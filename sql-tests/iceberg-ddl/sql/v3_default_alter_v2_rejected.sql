-- @order_sensitive=true
-- Test Point: ALTER ADD COLUMN with non-NULL DEFAULT on an existing v2 table is hard-rejected.
-- Method: CREATE v2 (default) table, ALTER ADD COLUMN b INT DEFAULT 5; expect format-version 3 error.
-- Scope: D5 — v2 ALTER ADD COLUMN gate.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_v3_default_alter_v2 FORCE;
CREATE TABLE ${case_db}.t_v3_default_alter_v2 (
  a INT
);

-- query 2
-- @expect_error=format-version 3
ALTER TABLE ${case_db}.t_v3_default_alter_v2 ADD COLUMN b INT DEFAULT 5;

-- query 3
-- @skip_result_check=true
DROP TABLE ${case_db}.t_v3_default_alter_v2 FORCE;
