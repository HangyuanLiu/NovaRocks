-- @order_sensitive=true
-- DATE -> TIMESTAMP widen.

-- query 1
CREATE DATABASE iceberg_cat_${suite_uuid0}.schema_date_${uuid0};
USE iceberg_cat_${suite_uuid0}.schema_date_${uuid0};
DROP TABLE IF EXISTS events;
CREATE TABLE events (
  id INT,
  occurred_on DATE
) TBLPROPERTIES (
  "format-version" = "2"
);
INSERT INTO events VALUES (1, '2026-01-15');

-- query 2
SELECT id, occurred_on FROM events ORDER BY id;

-- query 3
ALTER TABLE events MODIFY COLUMN occurred_on DATETIME;
INSERT INTO events VALUES (2, '2026-02-20 11:22:33');

-- query 4
SELECT id, occurred_on FROM events ORDER BY id;

-- query 5
DROP TABLE events;
DROP DATABASE iceberg_cat_${suite_uuid0}.schema_date_${uuid0};
SET catalog default_catalog;
