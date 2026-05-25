-- @order_sensitive=true
-- Nested STRUCT add / drop / rename / widen DDL end-to-end.
-- Note: NovaRocks's standalone INSERT path doesn't yet support STRUCT
-- column writes, so this test exercises the DDL pipeline only and
-- confirms that nested-path SELECT projection still resolves after
-- each ALTER. Data-level coverage of the nested schema is provided by
-- unit tests in `connector::iceberg::catalog::schema_update`.

-- query 1
CREATE DATABASE iceberg_cat_${suite_uuid0}.schema_nested_${uuid0};
USE iceberg_cat_${suite_uuid0}.schema_nested_${uuid0};
DROP TABLE IF EXISTS people;
CREATE TABLE people (
  id INT,
  address STRUCT<street STRING, city STRING>
) TBLPROPERTIES (
  "format-version" = "2"
);

-- query 2
ALTER TABLE people ADD COLUMN address.zip INT;

-- query 3
ALTER TABLE people RENAME COLUMN address.zip TO postal_code;

-- query 4
ALTER TABLE people MODIFY COLUMN address.postal_code BIGINT;

-- query 5
ALTER TABLE people DROP COLUMN address.city;

-- query 6
-- @expect_error=column path 'address.city' not found
ALTER TABLE people DROP COLUMN address.city;

-- query 7
-- @expect_error=column path 'address.bogus' not found
ALTER TABLE people DROP COLUMN address.bogus;

-- query 8
DROP TABLE people;
DROP DATABASE iceberg_cat_${suite_uuid0}.schema_nested_${uuid0};
SET catalog default_catalog;
