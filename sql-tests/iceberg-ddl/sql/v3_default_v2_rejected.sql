-- @order_sensitive=true
-- Test Point: CREATE TABLE with non-NULL DEFAULT on a v2 (default) Iceberg table is hard-rejected.
-- Method: CREATE TABLE without explicit format-version (defaults to v2), include DEFAULT 5; expect a clear error mentioning format-version 3.
-- Scope: v2 table policy (D5 in spec) — fail-fast at DDL.

-- query 1
-- @expect_error=format-version 3
CREATE TABLE ${case_db}.t_v3_default_v2_rej (
  a INT,
  b INT DEFAULT 5
);
