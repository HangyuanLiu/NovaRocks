-- @order_sensitive=true
-- Validate ALTER TABLE ... CREATE/DROP BRANCH|TAG happy path on iceberg.

-- query 1
-- @skip_result_check=true
CREATE DATABASE iceberg_cat_${suite_uuid0}.iceberg_ref_db_${uuid0};

-- query 2
-- @skip_result_check=true
CREATE TABLE iceberg_cat_${suite_uuid0}.iceberg_ref_db_${uuid0}.t_ref_${uuid0} (id INT, v INT);

-- query 3
-- @skip_result_check=true
INSERT INTO iceberg_cat_${suite_uuid0}.iceberg_ref_db_${uuid0}.t_ref_${uuid0} VALUES (1, 10), (2, 20);

-- query 4
-- @skip_result_check=true
ALTER TABLE iceberg_cat_${suite_uuid0}.iceberg_ref_db_${uuid0}.t_ref_${uuid0} CREATE BRANCH dev;

-- query 5
-- @skip_result_check=true
ALTER TABLE iceberg_cat_${suite_uuid0}.iceberg_ref_db_${uuid0}.t_ref_${uuid0} CREATE BRANCH IF NOT EXISTS dev;

-- query 6
-- @skip_result_check=true
ALTER TABLE iceberg_cat_${suite_uuid0}.iceberg_ref_db_${uuid0}.t_ref_${uuid0} CREATE OR REPLACE BRANCH dev;

-- query 7
-- @skip_result_check=true
ALTER TABLE iceberg_cat_${suite_uuid0}.iceberg_ref_db_${uuid0}.t_ref_${uuid0} CREATE TAG release_v1;

-- query 8
-- @skip_result_check=true
ALTER TABLE iceberg_cat_${suite_uuid0}.iceberg_ref_db_${uuid0}.t_ref_${uuid0} DROP TAG release_v1;

-- query 9
-- @skip_result_check=true
ALTER TABLE iceberg_cat_${suite_uuid0}.iceberg_ref_db_${uuid0}.t_ref_${uuid0} DROP BRANCH IF EXISTS dev;

-- query 10
-- @skip_result_check=true
ALTER TABLE iceberg_cat_${suite_uuid0}.iceberg_ref_db_${uuid0}.t_ref_${uuid0} DROP BRANCH IF EXISTS dev;

-- query 11
-- @skip_result_check=true
DROP TABLE iceberg_cat_${suite_uuid0}.iceberg_ref_db_${uuid0}.t_ref_${uuid0};

-- query 12
-- @skip_result_check=true
DROP DATABASE iceberg_cat_${suite_uuid0}.iceberg_ref_db_${uuid0};
