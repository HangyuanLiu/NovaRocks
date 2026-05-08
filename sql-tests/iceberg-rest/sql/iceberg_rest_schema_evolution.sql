-- @order_sensitive=true
-- Validate REST updateSchema commit: ADD COLUMN, RENAME COLUMN, type widen
-- INT→BIGINT, DROP COLUMN. After each ALTER, SELECT verifies the new schema
-- and existing data are read back correctly.

-- query 1
-- @skip_result_check=true
CREATE DATABASE iceberg_rest_${suite_uuid0}.iceberg_rest_se_db_${uuid0};

-- query 2
-- @skip_result_check=true
CREATE TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_se_db_${uuid0}.t_se_${uuid0} (id INT, v INT);

-- query 3
-- @skip_result_check=true
INSERT INTO iceberg_rest_${suite_uuid0}.iceberg_rest_se_db_${uuid0}.t_se_${uuid0} VALUES (1, 10), (2, 20);

-- query 4
-- @skip_result_check=true
ALTER TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_se_db_${uuid0}.t_se_${uuid0} ADD COLUMN c STRING;

-- query 5
-- @skip_result_check=true
INSERT INTO iceberg_rest_${suite_uuid0}.iceberg_rest_se_db_${uuid0}.t_se_${uuid0} VALUES (3, 30, 'three');

-- query 6
-- Old rows must read c as NULL; new row must read c='three'.
SELECT id, v, c
  FROM iceberg_rest_${suite_uuid0}.iceberg_rest_se_db_${uuid0}.t_se_${uuid0}
  ORDER BY id;

-- query 7
-- @skip_result_check=true
ALTER TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_se_db_${uuid0}.t_se_${uuid0} RENAME COLUMN v TO val;

-- query 8
-- After rename, val must read the original v values.
SELECT id, val, c
  FROM iceberg_rest_${suite_uuid0}.iceberg_rest_se_db_${uuid0}.t_se_${uuid0}
  ORDER BY id;

-- query 9
-- @skip_result_check=true
ALTER TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_se_db_${uuid0}.t_se_${uuid0} MODIFY COLUMN id BIGINT;

-- query 10
-- After widen INT→BIGINT, existing values must be readable.
SELECT id, val, c
  FROM iceberg_rest_${suite_uuid0}.iceberg_rest_se_db_${uuid0}.t_se_${uuid0}
  ORDER BY id;

-- query 11
-- @skip_result_check=true
ALTER TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_se_db_${uuid0}.t_se_${uuid0} DROP COLUMN c;

-- query 12
-- After DROP COLUMN c, only id and val remain.
SELECT id, val
  FROM iceberg_rest_${suite_uuid0}.iceberg_rest_se_db_${uuid0}.t_se_${uuid0}
  ORDER BY id;

-- query 13
-- @skip_result_check=true
DROP TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_se_db_${uuid0}.t_se_${uuid0};

-- query 14
-- @skip_result_check=true
DROP DATABASE iceberg_rest_${suite_uuid0}.iceberg_rest_se_db_${uuid0};
