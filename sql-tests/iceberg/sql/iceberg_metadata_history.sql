-- @order_sensitive=true
-- Validate SELECT * FROM <tbl>$history surfaces snapshot history rows.
-- Tests Phase A (metadata table SQL routing) — history flavour.

-- query 1
-- @skip_result_check=true
CREATE DATABASE iceberg_cat_${suite_uuid0}.iceberg_meta_db_${uuid0};

-- query 2
-- @skip_result_check=true
CREATE TABLE iceberg_cat_${suite_uuid0}.iceberg_meta_db_${uuid0}.metahist_${uuid0} (id INT)
TBLPROPERTIES ("format-version" = "3");

-- query 3
-- @skip_result_check=true
INSERT INTO iceberg_cat_${suite_uuid0}.iceberg_meta_db_${uuid0}.metahist_${uuid0} VALUES (1);

-- query 4
-- @skip_result_check=true
INSERT INTO iceberg_cat_${suite_uuid0}.iceberg_meta_db_${uuid0}.metahist_${uuid0} VALUES (2);

-- query 5
-- 2 history rows, one per snapshot.
SELECT count(*) AS n_history
  FROM iceberg_cat_${suite_uuid0}.iceberg_meta_db_${uuid0}.metahist_${uuid0}$history;

-- query 6
-- All history rows are ancestors of current after 2 sequential appends on main.
SELECT count(*) AS n_ancestors
  FROM iceberg_cat_${suite_uuid0}.iceberg_meta_db_${uuid0}.metahist_${uuid0}$history
  WHERE is_current_ancestor = TRUE;

-- query 7
-- @skip_result_check=true
DROP TABLE iceberg_cat_${suite_uuid0}.iceberg_meta_db_${uuid0}.metahist_${uuid0};

-- query 8
-- @skip_result_check=true
DROP DATABASE iceberg_cat_${suite_uuid0}.iceberg_meta_db_${uuid0};
