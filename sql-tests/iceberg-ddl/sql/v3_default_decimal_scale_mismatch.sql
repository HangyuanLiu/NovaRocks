-- @order_sensitive=true
-- Test Point: DEFAULT for a DECIMAL column whose scale does not match the column's scale is rejected at parse/DDL time.
-- Method: CREATE v3 table with `c DECIMAL(10,2) DEFAULT 1.234`; expect a scale-mismatch error.
-- Scope: D2 type validation in parse_default_literal / default_literal_to_iceberg.

-- query 1
-- @expect_error=scale
CREATE TABLE ${case_db}.t_v3_default_dec (
  c DECIMAL(10,2) DEFAULT 1.234
)
TBLPROPERTIES (
  "format-version" = "3"
);
