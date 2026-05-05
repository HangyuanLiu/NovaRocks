-- @order_sensitive=true
-- Validate equality-delete read semantics across Iceberg schema evolution.

-- query 1
CREATE DATABASE iceberg_cat_${suite_uuid0}.eq_delete_schema_evolution_${uuid0};
USE iceberg_cat_${suite_uuid0}.eq_delete_schema_evolution_${uuid0};
DROP TABLE IF EXISTS orders_eq_evo;
CREATE TABLE orders_eq_evo (
  id INT,
  amount FLOAT
) TBLPROPERTIES (
  "format-version" = "2"
);
INSERT INTO orders_eq_evo VALUES
  (1, 10.5),
  (2, 20.25),
  (3, 30.75);
ALTER TABLE orders_eq_evo ADD EQUALITY DELETE (amount) VALUES (20.25);
ALTER TABLE orders_eq_evo RENAME COLUMN amount TO total_amount;
ALTER TABLE orders_eq_evo MODIFY COLUMN total_amount DOUBLE;

-- query 2
SELECT id, total_amount FROM orders_eq_evo ORDER BY id;

-- query 3
SET catalog default_catalog;
DROP TABLE iceberg_cat_${suite_uuid0}.eq_delete_schema_evolution_${uuid0}.orders_eq_evo FORCE;
DROP DATABASE iceberg_cat_${suite_uuid0}.eq_delete_schema_evolution_${uuid0};
