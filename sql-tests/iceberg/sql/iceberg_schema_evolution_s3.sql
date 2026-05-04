-- @order_sensitive=true
-- Validate top-level Iceberg schema evolution against an S3-backed catalog.

-- query 1
CREATE DATABASE iceberg_cat_${suite_uuid0}.schema_evolution_s3_${uuid0};
USE iceberg_cat_${suite_uuid0}.schema_evolution_s3_${uuid0};
DROP TABLE IF EXISTS orders_s3;
CREATE TABLE orders_s3 (
  id INT,
  amount FLOAT
) TBLPROPERTIES (
  "format-version" = "2"
);
INSERT INTO orders_s3 VALUES (1, 10.5), (2, 20.25);
ALTER TABLE orders_s3 ADD COLUMN note_text STRING DEFAULT NULL;

-- query 2
SELECT id, amount, note_text FROM orders_s3 ORDER BY id;

-- query 3
INSERT INTO orders_s3 (id, amount, note_text) VALUES (3, 30.75, 's3-new');
ALTER TABLE orders_s3 RENAME COLUMN amount TO total_amount;

-- query 4
SELECT id, total_amount, note_text FROM orders_s3 ORDER BY id;

-- query 5
ALTER TABLE orders_s3 MODIFY COLUMN id BIGINT;
ALTER TABLE orders_s3 DROP COLUMN note_text;

-- query 6
SELECT id + 10000000000 AS widened_id, total_amount FROM orders_s3 ORDER BY id;

-- query 7
ALTER TABLE orders_s3 ADD COLUMN note_text STRING DEFAULT NULL;

-- query 8
SELECT id, total_amount, note_text FROM orders_s3 ORDER BY id;

-- query 9
INSERT INTO orders_s3 (id, total_amount, note_text) VALUES (4, 40.5, 's3-fresh');

-- query 10
SELECT id, total_amount, note_text FROM orders_s3 ORDER BY id;

-- query 11
SET catalog default_catalog;
DROP TABLE iceberg_cat_${suite_uuid0}.schema_evolution_s3_${uuid0}.orders_s3 FORCE;
DROP DATABASE iceberg_cat_${suite_uuid0}.schema_evolution_s3_${uuid0};
