-- @order_sensitive=true
-- ARRAY element + MAP value widen DDL end-to-end.
-- Note: standalone INSERT does not yet support ARRAY/MAP column writes,
-- so this test exercises the DDL pipeline only. Data-level coverage of
-- element/value widen is provided by unit tests in
-- `connector::iceberg::catalog::schema_update`.

-- query 1
CREATE DATABASE iceberg_cat_${suite_uuid0}.schema_arrmap_${uuid0};
USE iceberg_cat_${suite_uuid0}.schema_arrmap_${uuid0};
DROP TABLE IF EXISTS samples;
CREATE TABLE samples (
  id INT,
  scores ARRAY<INT>,
  attrs MAP<STRING, INT>
) TBLPROPERTIES (
  "format-version" = "2"
);

-- query 2
ALTER TABLE samples MODIFY COLUMN scores.element BIGINT;

-- query 3
ALTER TABLE samples MODIFY COLUMN attrs.value BIGINT;

-- query 4
-- @expect_error=unsupported Iceberg type evolution
ALTER TABLE samples MODIFY COLUMN scores.element INT;

-- query 5
DROP TABLE samples;
DROP DATABASE iceberg_cat_${suite_uuid0}.schema_arrmap_${uuid0};
SET catalog default_catalog;
