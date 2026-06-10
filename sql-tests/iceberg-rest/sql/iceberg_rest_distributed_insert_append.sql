-- @order_sensitive=true
-- Validate IW-7 distributed Iceberg INSERT append through REST:
-- - column-list VALUES reorders target columns and materializes write-defaults
-- - INSERT SELECT writes through the Iceberg table sink into partitioned output
-- - column-list VALUES width mismatch fails instead of dropping extra values

-- query 1
-- @skip_result_check=true
CREATE DATABASE iceberg_rest_${suite_uuid0}.iceberg_rest_iw7_db_${uuid0};

-- query 2
-- @skip_result_check=true
CREATE TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_iw7_db_${uuid0}.t_iw7_${uuid0} (
  id INT,
  region STRING
)
PARTITION BY (region)
TBLPROPERTIES ("format-version" = "3");

-- query 3
-- @skip_result_check=true
ALTER TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_iw7_db_${uuid0}.t_iw7_${uuid0}
  ADD COLUMN amount INT DEFAULT 7;

-- query 4
-- @skip_result_check=true
INSERT INTO iceberg_rest_${suite_uuid0}.iceberg_rest_iw7_db_${uuid0}.t_iw7_${uuid0} (region, id)
VALUES ('us', 1), ('eu', 2);

-- query 5
-- Column-list VALUES append must reorder columns and materialize amount=7.
SELECT id, region, amount
  FROM iceberg_rest_${suite_uuid0}.iceberg_rest_iw7_db_${uuid0}.t_iw7_${uuid0}
  ORDER BY id;

-- query 6
-- @skip_result_check=true
INSERT INTO iceberg_rest_${suite_uuid0}.iceberg_rest_iw7_db_${uuid0}.t_iw7_${uuid0} (region, id)
SELECT region, id + 10
  FROM iceberg_rest_${suite_uuid0}.iceberg_rest_iw7_db_${uuid0}.t_iw7_${uuid0}
  WHERE id <= 2;

-- query 7
-- INSERT SELECT append must use the same target column order and default fill.
SELECT id, region, amount
  FROM iceberg_rest_${suite_uuid0}.iceberg_rest_iw7_db_${uuid0}.t_iw7_${uuid0}
  ORDER BY id;

-- query 8
-- @expect_error=expected 1 values for column list, got 2
INSERT INTO iceberg_rest_${suite_uuid0}.iceberg_rest_iw7_db_${uuid0}.t_iw7_${uuid0} (id)
VALUES (100, 'dropped');

-- query 9
-- The failed INSERT must not add a row.
SELECT COUNT(*) AS n
  FROM iceberg_rest_${suite_uuid0}.iceberg_rest_iw7_db_${uuid0}.t_iw7_${uuid0};

-- query 10
-- @skip_result_check=true
DROP TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_iw7_db_${uuid0}.t_iw7_${uuid0};

-- query 11
-- @skip_result_check=true
DROP DATABASE iceberg_rest_${suite_uuid0}.iceberg_rest_iw7_db_${uuid0};
