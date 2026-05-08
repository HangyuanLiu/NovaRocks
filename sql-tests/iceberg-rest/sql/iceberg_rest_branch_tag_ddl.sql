-- @order_sensitive=true
-- Validate REST updateRefs commit: CREATE BRANCH/TAG, branch-qualified write,
-- DROP BRANCH/TAG. v3 (row-lineage) is required for branch writes.

-- query 1
-- @skip_result_check=true
CREATE DATABASE iceberg_rest_${suite_uuid0}.iceberg_rest_ref_db_${uuid0};

-- query 2
-- @skip_result_check=true
CREATE TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_ref_db_${uuid0}.t_ref_${uuid0} (id INT, v INT)
TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");

-- query 3
-- @skip_result_check=true
INSERT INTO iceberg_rest_${suite_uuid0}.iceberg_rest_ref_db_${uuid0}.t_ref_${uuid0} VALUES (1, 10), (2, 20);

-- query 4
-- @skip_result_check=true
ALTER TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_ref_db_${uuid0}.t_ref_${uuid0} CREATE BRANCH dev;

-- query 5
-- @skip_result_check=true
ALTER TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_ref_db_${uuid0}.t_ref_${uuid0} CREATE TAG release_v1;

-- query 6
-- @skip_result_check=true
INSERT INTO iceberg_rest_${suite_uuid0}.iceberg_rest_ref_db_${uuid0}.t_ref_${uuid0}.branch_dev VALUES (3, 30);

-- query 7
-- main is unchanged: 2 rows.
SELECT id, v
  FROM iceberg_rest_${suite_uuid0}.iceberg_rest_ref_db_${uuid0}.t_ref_${uuid0}
  FOR VERSION AS OF 'main' ORDER BY id;

-- query 8
-- dev now has 3 rows.
SELECT id, v
  FROM iceberg_rest_${suite_uuid0}.iceberg_rest_ref_db_${uuid0}.t_ref_${uuid0}
  FOR VERSION AS OF 'dev' ORDER BY id;

-- query 9
-- @skip_result_check=true
ALTER TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_ref_db_${uuid0}.t_ref_${uuid0} DROP BRANCH dev;

-- query 10
-- @skip_result_check=true
ALTER TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_ref_db_${uuid0}.t_ref_${uuid0} DROP TAG release_v1;

-- query 11
-- @skip_result_check=true
-- Idempotent IF EXISTS after DROP.
ALTER TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_ref_db_${uuid0}.t_ref_${uuid0} DROP BRANCH IF EXISTS dev;

-- query 12
-- @skip_result_check=true
DROP TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_ref_db_${uuid0}.t_ref_${uuid0};

-- query 13
-- @skip_result_check=true
DROP DATABASE iceberg_rest_${suite_uuid0}.iceberg_rest_ref_db_${uuid0};
