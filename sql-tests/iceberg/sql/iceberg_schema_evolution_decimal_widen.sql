-- @order_sensitive=true
-- Decimal precision widen happy path + scale-change reject.

-- query 1
CREATE DATABASE iceberg_cat_${suite_uuid0}.schema_decimal_${uuid0};
USE iceberg_cat_${suite_uuid0}.schema_decimal_${uuid0};
DROP TABLE IF EXISTS sales;
CREATE TABLE sales (
  id INT,
  price DECIMAL(10, 2)
) TBLPROPERTIES (
  "format-version" = "2"
);
INSERT INTO sales VALUES (1, 12345.67);

-- query 2
SELECT id, price FROM sales ORDER BY id;

-- query 3
ALTER TABLE sales MODIFY COLUMN price DECIMAL(20, 2);
INSERT INTO sales VALUES (2, 999999999999999999.99);

-- query 4
SELECT id, price FROM sales ORDER BY id;

-- query 5
-- @expect_error=decimal scale change is not allowed
ALTER TABLE sales MODIFY COLUMN price DECIMAL(20, 4);

-- query 6
-- @expect_error=decimal precision must strictly increase
ALTER TABLE sales MODIFY COLUMN price DECIMAL(15, 2);

-- query 7
DROP TABLE sales;
DROP DATABASE iceberg_cat_${suite_uuid0}.schema_decimal_${uuid0};
SET catalog default_catalog;
