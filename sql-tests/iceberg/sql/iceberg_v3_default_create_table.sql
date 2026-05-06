-- @order_sensitive=true
-- Test Point: Iceberg v3 CREATE TABLE with non-NULL DEFAULT persists initial/write-default and INSERT (col list) materializes write-default.
-- Method: CREATE v3 table with `b INT DEFAULT 5`, INSERT with explicit column list omitting b, INSERT including b, then SELECT both rows.
-- Scope: CREATE TABLE DEFAULT capture, write-default materialization on VALUES INSERT, read-back of physically materialized values.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_v3_default_ct FORCE;
CREATE TABLE ${case_db}.t_v3_default_ct (
  a INT,
  b INT DEFAULT 5
)
TBLPROPERTIES (
  "format-version" = "3"
);
INSERT INTO ${case_db}.t_v3_default_ct (a) VALUES (1);
INSERT INTO ${case_db}.t_v3_default_ct (a, b) VALUES (2, 7);

-- query 2
SELECT a, b FROM ${case_db}.t_v3_default_ct ORDER BY a;

-- query 3
-- @skip_result_check=true
DROP TABLE ${case_db}.t_v3_default_ct FORCE;
