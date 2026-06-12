-- @order_sensitive=true
-- Validate distributed Iceberg DELETE through ICEBERG_DELETE_SINK:
-- - v2/legacy tables write position-delete files from a distributed SELECT
-- - partitioned delete files preserve partition values from sink source cols
-- - v3 row-lineage/DV DELETE fails fast until DV writer output is available

-- query 1
-- @skip_result_check=true
CREATE DATABASE iceberg_rest_${suite_uuid0}.iceberg_rest_del_db_${uuid0};

-- query 2
-- @skip_result_check=true
CREATE TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_del_db_${uuid0}.t_del_${uuid0} (
  id INT,
  region STRING,
  amount INT
)
PARTITION BY (region);

-- query 3
-- @skip_result_check=true
INSERT INTO iceberg_rest_${suite_uuid0}.iceberg_rest_del_db_${uuid0}.t_del_${uuid0}
VALUES (1, 'east', 10), (2, 'east', 20), (3, 'west', 30);

-- query 4
-- @skip_result_check=true
DELETE FROM iceberg_rest_${suite_uuid0}.iceberg_rest_del_db_${uuid0}.t_del_${uuid0}
WHERE region = 'east' AND amount = 10;

-- query 5
SELECT id, region, amount
  FROM iceberg_rest_${suite_uuid0}.iceberg_rest_del_db_${uuid0}.t_del_${uuid0}
  ORDER BY id;

-- query 6
-- @skip_result_check=true
CREATE TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_del_db_${uuid0}.t_del_v3_${uuid0} (
  id INT,
  region STRING
)
TBLPROPERTIES ("format-version" = "3");

-- query 7
-- @skip_result_check=true
INSERT INTO iceberg_rest_${suite_uuid0}.iceberg_rest_del_db_${uuid0}.t_del_v3_${uuid0}
VALUES (1, 'east');

-- query 8
-- @expect_error=UnsupportedDistributedDmlShape: deletion-vector DELETE
DELETE FROM iceberg_rest_${suite_uuid0}.iceberg_rest_del_db_${uuid0}.t_del_v3_${uuid0}
WHERE id = 1;

-- query 9
-- @skip_result_check=true
DROP TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_del_db_${uuid0}.t_del_v3_${uuid0};

-- query 10
-- @skip_result_check=true
DROP TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_del_db_${uuid0}.t_del_${uuid0};

-- query 11
-- @skip_result_check=true
DROP DATABASE iceberg_rest_${suite_uuid0}.iceberg_rest_del_db_${uuid0};
