-- @order_sensitive=true
-- Validate branch-qualified INSERT / UPDATE / DELETE on iceberg branches.
-- Verifies the target ref is advanced and main is untouched, then drops the branch.
-- Table must be Iceberg v3 (row-lineage) because branch writes require v3.

-- query 1
-- @skip_result_check=true
-- Create test database.
CREATE DATABASE iceberg_cat_${suite_uuid0}.iceberg_bw_db_${uuid0};

-- query 2
-- @skip_result_check=true
-- Create v3 row-lineage table (branch writes require v3).
CREATE TABLE iceberg_cat_${suite_uuid0}.iceberg_bw_db_${uuid0}.t_bw_${uuid0} (id INT, v INT)
TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");

-- query 3
-- @skip_result_check=true
-- Initial INSERT to main (creates first snapshot).
INSERT INTO iceberg_cat_${suite_uuid0}.iceberg_bw_db_${uuid0}.t_bw_${uuid0} VALUES (1, 10), (2, 20);

-- query 4
-- @skip_result_check=true
-- Branch dev off main.
ALTER TABLE iceberg_cat_${suite_uuid0}.iceberg_bw_db_${uuid0}.t_bw_${uuid0} CREATE BRANCH dev;

-- query 5
-- @skip_result_check=true
-- INSERT to branch dev.
INSERT INTO iceberg_cat_${suite_uuid0}.iceberg_bw_db_${uuid0}.t_bw_${uuid0}.branch_dev VALUES (3, 30);

-- query 6
-- main should still have only 2 rows.
SELECT id, v FROM iceberg_cat_${suite_uuid0}.iceberg_bw_db_${uuid0}.t_bw_${uuid0}
  FOR VERSION AS OF 'main' ORDER BY id;

-- query 7
-- dev should now have 3 rows.
SELECT id, v FROM iceberg_cat_${suite_uuid0}.iceberg_bw_db_${uuid0}.t_bw_${uuid0}
  FOR VERSION AS OF 'dev' ORDER BY id;

-- query 8
-- @skip_result_check=true
-- UPDATE on branch dev: set v=99 for row id=1.
UPDATE iceberg_cat_${suite_uuid0}.iceberg_bw_db_${uuid0}.t_bw_${uuid0}.branch_dev
  SET v = 99 WHERE id = 1;

-- query 9
-- main: row 1 v=10 unchanged.
SELECT id, v FROM iceberg_cat_${suite_uuid0}.iceberg_bw_db_${uuid0}.t_bw_${uuid0}
  FOR VERSION AS OF 'main' ORDER BY id;

-- query 10
-- dev: row 1 v=99.
SELECT id, v FROM iceberg_cat_${suite_uuid0}.iceberg_bw_db_${uuid0}.t_bw_${uuid0}
  FOR VERSION AS OF 'dev' ORDER BY id;

-- query 11
-- @skip_result_check=true
-- DELETE on branch dev: remove row id=2.
DELETE FROM iceberg_cat_${suite_uuid0}.iceberg_bw_db_${uuid0}.t_bw_${uuid0}.branch_dev
  WHERE id = 2;

-- query 12
-- main: row 2 still there.
SELECT id, v FROM iceberg_cat_${suite_uuid0}.iceberg_bw_db_${uuid0}.t_bw_${uuid0}
  FOR VERSION AS OF 'main' ORDER BY id;

-- query 13
-- dev: row 2 gone.
SELECT id, v FROM iceberg_cat_${suite_uuid0}.iceberg_bw_db_${uuid0}.t_bw_${uuid0}
  FOR VERSION AS OF 'dev' ORDER BY id;

-- query 14
-- @skip_result_check=true
-- Cleanup: drop branch, table, database.
ALTER TABLE iceberg_cat_${suite_uuid0}.iceberg_bw_db_${uuid0}.t_bw_${uuid0} DROP BRANCH dev;

-- query 15
-- @skip_result_check=true
DROP TABLE iceberg_cat_${suite_uuid0}.iceberg_bw_db_${uuid0}.t_bw_${uuid0};

-- query 16
-- @skip_result_check=true
DROP DATABASE iceberg_cat_${suite_uuid0}.iceberg_bw_db_${uuid0};
