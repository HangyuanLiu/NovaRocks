-- @order_sensitive=true
-- Validate SELECT * FROM <tbl>$refs surfaces branches and tags.
-- Tests Phase A (metadata table SQL routing) — refs flavour.

-- query 1
-- @skip_result_check=true
CREATE DATABASE iceberg_cat_${suite_uuid0}.iceberg_meta_db_${uuid0};

-- query 2
-- @skip_result_check=true
CREATE TABLE iceberg_cat_${suite_uuid0}.iceberg_meta_db_${uuid0}.metaref_${uuid0} (id INT)
TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");

-- query 3
-- @skip_result_check=true
INSERT INTO iceberg_cat_${suite_uuid0}.iceberg_meta_db_${uuid0}.metaref_${uuid0} VALUES (1);

-- query 4
-- @skip_result_check=true
ALTER TABLE iceberg_cat_${suite_uuid0}.iceberg_meta_db_${uuid0}.metaref_${uuid0} CREATE BRANCH dev;

-- query 5
-- @skip_result_check=true
ALTER TABLE iceberg_cat_${suite_uuid0}.iceberg_meta_db_${uuid0}.metaref_${uuid0} CREATE TAG v1;

-- query 6
-- 3 refs total: dev (BRANCH), main (BRANCH), v1 (TAG). Names are tab-stable.
SELECT name, type
  FROM iceberg_cat_${suite_uuid0}.iceberg_meta_db_${uuid0}.metaref_${uuid0}$refs
  ORDER BY name;

-- query 7
-- @skip_result_check=true
ALTER TABLE iceberg_cat_${suite_uuid0}.iceberg_meta_db_${uuid0}.metaref_${uuid0} DROP TAG v1;

-- query 8
-- @skip_result_check=true
ALTER TABLE iceberg_cat_${suite_uuid0}.iceberg_meta_db_${uuid0}.metaref_${uuid0} DROP BRANCH dev;

-- query 9
-- @skip_result_check=true
DROP TABLE iceberg_cat_${suite_uuid0}.iceberg_meta_db_${uuid0}.metaref_${uuid0};

-- query 10
-- @skip_result_check=true
DROP DATABASE iceberg_cat_${suite_uuid0}.iceberg_meta_db_${uuid0};
