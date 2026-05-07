-- @order_sensitive=true
-- ALTER TABLE SET / UNSET TBLPROPERTIES happy path.
-- Note: SHOW CREATE TABLE does not surface TBLPROPERTIES in NovaRocks today.
-- Each SELECT confirms the table is still queryable after each property change.

-- query 1
CREATE DATABASE iceberg_cat_${suite_uuid0}.tblprops_${uuid0};
USE iceberg_cat_${suite_uuid0}.tblprops_${uuid0};
DROP TABLE IF EXISTS p;
CREATE TABLE p (id INT) TBLPROPERTIES ("format-version" = "2");
INSERT INTO p VALUES (1);

-- query 2
SELECT id FROM p ORDER BY id;

-- query 3
ALTER TABLE p SET TBLPROPERTIES ('write.parquet.compression-codec' = 'zstd');

-- query 4
SELECT id FROM p ORDER BY id;

-- query 5
ALTER TABLE p SET TBLPROPERTIES ('comment' = 'hello', 'gc.enabled' = 'true');

-- query 6
SELECT id FROM p ORDER BY id;

-- query 7
-- Overwrite an existing key.
ALTER TABLE p SET TBLPROPERTIES ('comment' = 'world');

-- query 8
SELECT id FROM p ORDER BY id;

-- query 9
ALTER TABLE p UNSET TBLPROPERTIES ('comment');

-- query 10
SELECT id FROM p ORDER BY id;

-- query 11
DROP TABLE p;
DROP DATABASE iceberg_cat_${suite_uuid0}.tblprops_${uuid0};
