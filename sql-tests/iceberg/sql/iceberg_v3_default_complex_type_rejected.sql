-- @order_sensitive=true
-- Test Point: DEFAULT for complex/unsupported types (Array/Struct/etc.) is rejected at DDL time.
-- Method: ALTER ADD COLUMN with an ARRAY type and a DEFAULT expression; expect a "DEFAULT not supported" error.
-- Scope: D2 — complex-type defaults are out of scope; reject at parse time.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_v3_default_complex FORCE;
CREATE TABLE ${case_db}.t_v3_default_complex (
  id INT
)
TBLPROPERTIES (
  "format-version" = "3"
);

-- query 2
-- @expect_error=DEFAULT
ALTER TABLE ${case_db}.t_v3_default_complex ADD COLUMN c ARRAY<INT> DEFAULT [1,2];

-- query 3
-- @skip_result_check=true
DROP TABLE ${case_db}.t_v3_default_complex FORCE;
