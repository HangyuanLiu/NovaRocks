-- @sequential=true
-- @order_sensitive=true
-- @tags=pir8,iceberg_rest,write_sink
-- PIR-8 M3 guard: SQL EXPLAIN does not expose the root Iceberg write sink yet,
-- so this case locks the executable INSERT SELECT path through a partitioned
-- Iceberg REST table. Rust IR tests assert the root ICEBERG_TABLE_SINK payload.

-- query 1
-- @skip_result_check=true
CREATE DATABASE iceberg_rest_${suite_uuid0}.pir8_sink_db_${uuid0};

-- query 2
-- @skip_result_check=true
CREATE TABLE iceberg_rest_${suite_uuid0}.pir8_sink_db_${uuid0}.t_sink_${uuid0} (
  id INT,
  region STRING,
  amount INT
)
PARTITION BY (region)
TBLPROPERTIES ("format-version" = "3");

-- query 3
-- @skip_result_check=true
INSERT INTO iceberg_rest_${suite_uuid0}.pir8_sink_db_${uuid0}.t_sink_${uuid0}
VALUES
  (1, 'east', 10),
  (2, 'west', 20);

-- query 4
-- @skip_result_check=true
INSERT INTO iceberg_rest_${suite_uuid0}.pir8_sink_db_${uuid0}.t_sink_${uuid0}
SELECT id + 10, region, amount
FROM iceberg_rest_${suite_uuid0}.pir8_sink_db_${uuid0}.t_sink_${uuid0}
WHERE id <= 2;

-- query 5
SELECT id, region, amount
FROM iceberg_rest_${suite_uuid0}.pir8_sink_db_${uuid0}.t_sink_${uuid0}
ORDER BY id;

-- query 6
-- @skip_result_check=true
DROP TABLE iceberg_rest_${suite_uuid0}.pir8_sink_db_${uuid0}.t_sink_${uuid0};

-- query 7
-- @skip_result_check=true
DROP DATABASE iceberg_rest_${suite_uuid0}.pir8_sink_db_${uuid0};
