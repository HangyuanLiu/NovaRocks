-- @order_sensitive=true
-- Negative widening matrix.

-- query 1
CREATE DATABASE iceberg_cat_${suite_uuid0}.schema_reject_${uuid0};
USE iceberg_cat_${suite_uuid0}.schema_reject_${uuid0};
DROP TABLE IF EXISTS bad;
CREATE TABLE bad (
  i BIGINT,
  d DOUBLE,
  s STRING,
  ts DATETIME
) TBLPROPERTIES ("format-version" = "2");

-- query 2
-- @expect_error=unsupported Iceberg type evolution
ALTER TABLE bad MODIFY COLUMN i INT;

-- query 3
-- @expect_error=unsupported Iceberg type evolution
ALTER TABLE bad MODIFY COLUMN d FLOAT;

-- query 4
-- @expect_error=unsupported Iceberg type evolution
ALTER TABLE bad MODIFY COLUMN s VARBINARY;

-- query 5
-- @expect_error=unsupported Iceberg type evolution
ALTER TABLE bad MODIFY COLUMN ts DATE;

-- query 6
DROP TABLE bad;
DROP DATABASE iceberg_cat_${suite_uuid0}.schema_reject_${uuid0};
SET catalog default_catalog;
