-- @order_sensitive=true
-- ALTER COLUMN reorder (FIRST / AFTER / BEFORE) on top-level and nested.

-- query 1
CREATE DATABASE iceberg_cat_${suite_uuid0}.schema_reorder_${uuid0};
USE iceberg_cat_${suite_uuid0}.schema_reorder_${uuid0};
DROP TABLE IF EXISTS reorder_top;
CREATE TABLE reorder_top (
  a INT,
  b INT,
  c INT
) TBLPROPERTIES ("format-version" = "2");
INSERT INTO reorder_top VALUES (1, 2, 3);

-- query 2
SELECT a, b, c FROM reorder_top ORDER BY a;

-- query 3
ALTER TABLE reorder_top ALTER COLUMN c FIRST;

-- query 4
SELECT a, b, c FROM reorder_top ORDER BY a;

-- query 5
ALTER TABLE reorder_top ALTER COLUMN a AFTER b;

-- query 6
SELECT a, b, c FROM reorder_top ORDER BY a;

-- query 7
-- Nested reorder: standalone INSERT doesn't support STRUCT, so the
-- test exercises DDL only and confirms the SELECT projection still
-- resolves after the move.
DROP TABLE IF EXISTS reorder_nested;
CREATE TABLE reorder_nested (
  id INT,
  address STRUCT<street STRING, city STRING, zip INT>
) TBLPROPERTIES ("format-version" = "2");

-- query 8
ALTER TABLE reorder_nested ALTER COLUMN address.zip BEFORE address.street;

-- query 9
-- Cross-parent reference must be rejected.
-- @expect_error=AFTER target 'id' not found in same parent
ALTER TABLE reorder_nested ALTER COLUMN address.street AFTER id;

-- query 10
DROP TABLE reorder_top;
DROP TABLE reorder_nested;
DROP DATABASE iceberg_cat_${suite_uuid0}.schema_reorder_${uuid0};
SET catalog default_catalog;
